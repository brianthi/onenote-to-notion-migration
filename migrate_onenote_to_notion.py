#!/usr/bin/env python3
"""
migrate_onenote_to_notion.py – Single-script OneNote → Notion migration.

Walks your OneNote notebooks via Microsoft Graph API and imports each page
directly into Notion, preserving the full hierarchy:
  Notebook → Section Group → Section → Page

Each page is an atomic unit: export HTML → download images → convert to Notion
blocks → create Notion page → mark complete. If the script is interrupted at
any point, re-running it picks up exactly where it left off.

Features:
  • Single manifest tracks every notebook, section, section group, and page
  • Per-page state machine: pending → exported → notion_created → imported
  • Block-level resume: tracks next block index so mid-append crashes continue exactly
  • Automatic token refresh (MSAL persistent cache)
  • Retry with exponential backoff + jitter on both Graph and Notion APIs
  • Notion File Upload API for images (no external hosting needed)
  • Upload deduplication (same image across pages uploads once)
  • Graceful Ctrl+C with progress saved
  • Detailed summary report with error log

Prerequisites:
  pip install msal requests beautifulsoup4 lxml

Environment variables (required):
  MS_CLIENT_ID       – Azure AD app Client ID (for OneNote access)
  NOTION_TOKEN       – Notion integration token (starts with ntn_)
  NOTION_PARENT_ID   – Notion page ID to import into (32-char hex from URL)

Environment variables (optional):
  OUT_DIR              – Local cache directory (default: ./onenote_migration)
  IMAGE_BASE_URL       – External URL prefix for images (skips Notion uploads)
  UPLOAD_LOCAL_FILES   – Upload images to Notion directly (default: 1)
  NOTION_MAX_UPLOAD_MB – Max file size for direct uploads in MB (default: 20)
  NOTION_VERSION       – Notion API version (default: 2025-09-03)

Usage:
  export MS_CLIENT_ID="your-azure-client-id"
  export NOTION_TOKEN="ntn_..."
  export NOTION_PARENT_ID="abc123def456..."
  python migrate_onenote_to_notion.py

  # Resume after interruption — just run the same command again.
"""

import os
import re
import json
import base64
import time
import hashlib
import logging
import pathlib
import mimetypes
import random
from urllib.parse import urlparse
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional, Generator

import requests
import msal
from bs4 import BeautifulSoup, NavigableString, Tag, FeatureNotFound

# ══════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════
GRAPH = "https://graph.microsoft.com/v1.0"
AUTHORITY = "https://login.microsoftonline.com/consumers"
SCOPES = ["Notes.Read"]

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = os.environ.get("NOTION_VERSION", "2025-09-03")

# Retry / rate-limit
MAX_RETRIES = 5
INITIAL_BACKOFF_SEC = 1.0
BACKOFF_MULTIPLIER = 2.0
MAX_BACKOFF_SEC = 120.0
GRAPH_DELAY_SEC = 0.15
NOTION_DELAY_SEC = 0.35

# Notion limits
BLOCKS_PER_REQUEST = 100
RICH_TEXT_MAX_CHARS = 2000

SESSION = requests.Session()

# ══════════════════════════════════════════════
# Logging
# ══════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("migrate")


# ══════════════════════════════════════════════
# Stats
# ══════════════════════════════════════════════
@dataclass
class Stats:
    notebooks: int = 0
    section_groups: int = 0
    sections: int = 0
    pages_exported: int = 0
    pages_imported: int = 0
    pages_skipped: int = 0
    pages_failed: int = 0
    blocks_created: int = 0
    images_downloaded: int = 0
    images_uploaded: int = 0
    images_linked: int = 0
    images_skipped: int = 0
    uploads_failed: int = 0
    errors: list = field(default_factory=list)

    def summary(self) -> str:
        lines = [
            "\n" + "=" * 60,
            "MIGRATION SUMMARY",
            "=" * 60,
            f"  Notebooks:          {self.notebooks}",
            f"  Section Groups:     {self.section_groups}",
            f"  Sections:           {self.sections}",
            f"  Pages exported:     {self.pages_exported}",
            f"  Pages imported:     {self.pages_imported}",
            f"  Pages skipped:      {self.pages_skipped} (already done)",
            f"  Pages failed:       {self.pages_failed}",
            f"  Blocks created:     {self.blocks_created}",
            f"  Images downloaded:  {self.images_downloaded}",
            f"  Images uploaded:    {self.images_uploaded}",
            f"  Images linked:      {self.images_linked} (external URL)",
            f"  Images skipped:     {self.images_skipped}",
            f"  Uploads failed:     {self.uploads_failed}",
        ]
        if self.errors:
            lines.append(f"\n  ERRORS ({len(self.errors)}):")
            for err in self.errors[:50]:
                lines.append(f"    • {err}")
            if len(self.errors) > 50:
                lines.append(f"    ... and {len(self.errors) - 50} more")
        lines.append("=" * 60)
        return "\n".join(lines)


# ══════════════════════════════════════════════
# Unified Manifest
# ══════════════════════════════════════════════
class Manifest:
    """
    Tracks the state of every entity in the migration.

    Structure:
      {
        "structure": {
          "<onenote_id>": {
            "type": "notebook"|"section_group"|"section",
            "name": "...",
            "notion_id": "...",
          }
        },
        "pages": {
          "<onenote_page_id>": {
            "title": "...",
            "status": "exported"|"notion_created"|"imported",
            "local_path": "...",
            "notion_id": "...",
            "import_next_block": 0,
            "exported_at": "...",
            "notion_created_at": "...",
            "imported_at": "..."
          }
        },
        "uploaded_files": {
          "<local_file_path>": "<notion_file_upload_id>"
        }
      }
    """

    def __init__(self, path: pathlib.Path):
        self.path = path
        self.structure: dict[str, dict] = {}
        self.pages: dict[str, dict] = {}
        self.uploaded_files: dict[str, str] = {}
        self._dirty_count = 0
        self._load()

    def _load(self):
        if self.path.exists():
            try:
                data = json.loads(self.path.read_text(encoding="utf-8"))
                self.structure = data.get("structure", {})
                self.pages = data.get("pages", {})
                self.uploaded_files = data.get("uploaded_files", {})
                total = len(self.pages)
                imported = sum(1 for p in self.pages.values() if p.get("status") == "imported")
                log.info(f"Loaded manifest: {imported}/{total} pages imported, "
                         f"{len(self.structure)} structural nodes, "
                         f"{len(self.uploaded_files)} files uploaded")
            except (json.JSONDecodeError, KeyError):
                log.warning("Manifest corrupted, starting fresh")

    def save(self):
        data = {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "structure": self.structure,
            "pages": self.pages,
            "uploaded_files": self.uploaded_files,
        }
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
        tmp.replace(self.path)
        self._dirty_count = 0

    def _auto_save(self):
        """Save periodically to avoid losing too much progress."""
        self._dirty_count += 1
        if self._dirty_count >= 10:
            self.save()

    # ── Structure (notebooks, sections, section groups) ──

    def get_structure_notion_id(self, onenote_id: str) -> Optional[str]:
        info = self.structure.get(onenote_id)
        return info.get("notion_id") if info else None

    def set_structure(self, onenote_id: str, name: str, entity_type: str, notion_id: str):
        self.structure[onenote_id] = {
            "type": entity_type,
            "name": name,
            "notion_id": notion_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._auto_save()

    # ── Pages ──

    def page_status(self, page_id: str) -> Optional[str]:
        info = self.pages.get(page_id)
        if not info:
            return None
        status = info.get("status")
        # Verify local files still exist for pre-imported states
        if status in ("exported", "notion_created"):
            local = info.get("local_path")
            if local and not pathlib.Path(local).exists():
                # Files gone — need to re-export, but if a Notion page already
                # exists we must NOT re-create it (that would duplicate).
                if info.get("notion_id"):
                    return "notion_created"  # keep Notion page, re-export HTML
                return None  # no Notion page yet, start fresh
        return status

    def mark_exported(self, page_id: str, title: str, local_path: str):
        existing = self.pages.get(page_id, {})
        existing.update({
            "title": title,
            "status": "exported",
            "local_path": local_path,
            "exported_at": datetime.now(timezone.utc).isoformat(),
        })
        self.pages[page_id] = existing
        self._auto_save()


    def mark_notion_created(self, page_id: str, notion_id: str):
        """Record that the Notion page exists (so we can resume block appends without creating duplicates)."""
        existing = self.pages.get(page_id, {})
        existing.update({
            "status": "notion_created",
            "notion_id": notion_id,
            "import_next_block": int(existing.get("import_next_block", 0) or 0),
            "notion_created_at": datetime.now(timezone.utc).isoformat(),
        })
        self.pages[page_id] = existing
        self._auto_save()

    def get_import_next_block(self, page_id: str) -> int:
        info = self.pages.get(page_id) or {}
        try:
            return int(info.get("import_next_block", 0) or 0)
        except (TypeError, ValueError):
            return 0

    def set_import_next_block(self, page_id: str, next_block_index: int):
        if page_id not in self.pages:
            self.pages[page_id] = {}
        self.pages[page_id]["import_next_block"] = int(next_block_index)
        self._auto_save()

    def mark_imported(self, page_id: str, notion_id: str):
        if page_id in self.pages:
            self.pages[page_id]["status"] = "imported"
            self.pages[page_id]["notion_id"] = notion_id
            self.pages[page_id]["imported_at"] = datetime.now(timezone.utc).isoformat()
        self._auto_save()

    def get_page_local_path(self, page_id: str) -> Optional[str]:
        info = self.pages.get(page_id)
        return info.get("local_path") if info else None

    # ── File uploads ──

    def get_uploaded_file_id(self, local_path: str) -> Optional[str]:
        return self.uploaded_files.get(local_path)

    def remember_uploaded_file(self, local_path: str, file_upload_id: str):
        self.uploaded_files[local_path] = file_upload_id
        self._auto_save()


# ══════════════════════════════════════════════
# Microsoft Graph Authentication
# ══════════════════════════════════════════════
class TokenManager:
    """MSAL authentication with persistent cache and silent refresh."""

    def __init__(self, client_id: str, cache_path: pathlib.Path):
        self.client_id = client_id
        self.cache_path = cache_path
        self.cache = msal.SerializableTokenCache()

        if self.cache_path.exists():
            try:
                self.cache.deserialize(self.cache_path.read_text(encoding="utf-8"))
                log.info("Loaded MSAL token cache")
            except Exception as e:
                log.warning(f"Failed to load MSAL cache: {e}")

        self.app = msal.PublicClientApplication(
            client_id=client_id, authority=AUTHORITY, token_cache=self.cache,
        )
        self._token_result: Optional[dict] = None
        self._token_expiry: Optional[float] = None

    def _persist_cache(self):
        try:
            if getattr(self.cache, "has_state_changed", False):
                tmp = self.cache_path.with_suffix(self.cache_path.suffix + ".tmp")
                tmp.write_text(self.cache.serialize(), encoding="utf-8")
                tmp.replace(self.cache_path)
        except Exception as e:
            log.warning(f"Failed to persist MSAL cache: {e}")

    def authenticate(self) -> str:
        accounts = self.app.get_accounts()
        if accounts:
            result = self.app.acquire_token_silent(SCOPES, account=accounts[0])
            if result and "access_token" in result:
                self._store_token(result)
                log.info("Token acquired silently")
                return result["access_token"]

        flow = self.app.initiate_device_flow(scopes=SCOPES)
        if "message" not in flow:
            raise SystemExit(f"Device flow failed: {flow}")

        print("\n" + "=" * 60)
        print("MICROSOFT AUTHENTICATION")
        print("=" * 60)
        print(flow["message"])
        print("=" * 60 + "\n")

        result = self.app.acquire_token_by_device_flow(flow)
        if "access_token" not in result:
            raise SystemExit(f"Auth failed: {result.get('error_description', result)}")

        self._store_token(result)
        log.info("Microsoft authentication successful")
        return result["access_token"]

    def get_token(self) -> str:
        if self._token_result is None:
            return self.authenticate()
        if self._token_expiry and time.time() > (self._token_expiry - 300):
            log.info("Token nearing expiry, refreshing...")
            accounts = self.app.get_accounts()
            if accounts:
                result = self.app.acquire_token_silent(SCOPES, account=accounts[0])
                if result and "access_token" in result:
                    self._store_token(result)
                    return result["access_token"]
            return self.authenticate()
        return self._token_result["access_token"]

    def _store_token(self, result: dict):
        self._token_result = result
        self._token_expiry = time.time() + int(result.get("expires_in", 3600))
        self._persist_cache()


# ══════════════════════════════════════════════
# Graph API helpers
# ══════════════════════════════════════════════
def graph_request(url: str, token_mgr: TokenManager, accept: str = "application/json") -> requests.Response:
    backoff = INITIAL_BACKOFF_SEC
    for attempt in range(1, MAX_RETRIES + 1):
        token = token_mgr.get_token()
        headers = {"Authorization": f"Bearer {token}", "Accept": accept}
        try:
            r = SESSION.get(url, headers=headers, timeout=60)
            if r.status_code == 200:
                return r
            if r.status_code == 401:
                token_mgr._token_expiry = 0
                continue
            if r.status_code in (408, 429) or r.status_code >= 500:
                retry_after = r.headers.get("Retry-After")
                try:
                    wait = float(retry_after) if retry_after else backoff
                except ValueError:
                    wait = backoff
                wait = min(wait, MAX_BACKOFF_SEC) * (0.5 + random.random())
                log.warning(f"Graph {r.status_code} attempt {attempt}/{MAX_RETRIES}, retry in {wait:.1f}s")
                time.sleep(wait)
                backoff = min(backoff * BACKOFF_MULTIPLIER, MAX_BACKOFF_SEC)
                continue
            r.raise_for_status()
        except (requests.ConnectionError, requests.Timeout) as e:
            log.warning(f"Graph network error attempt {attempt}/{MAX_RETRIES}: {e}")
            time.sleep(min(backoff, MAX_BACKOFF_SEC))
            backoff *= BACKOFF_MULTIPLIER
    raise requests.exceptions.RetryError(f"Graph failed after {MAX_RETRIES} attempts: {url[:200]}")


def paged_json(url: str, token_mgr: TokenManager) -> Generator[dict, None, None]:
    while url:
        data = graph_request(url, token_mgr).json()
        for item in data.get("value", []):
            yield item
        url = data.get("@odata.nextLink")
        if url:
            time.sleep(GRAPH_DELAY_SEC)


# ══════════════════════════════════════════════
# Notion API client
# ══════════════════════════════════════════════
class NotionClient:
    def __init__(self, token: str):
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json",
        }

    def _request(self, method: str, url: str, payload: Optional[dict] = None) -> dict:
        backoff = INITIAL_BACKOFF_SEC
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = SESSION.request(method, url, headers=self.headers, json=payload, timeout=60)
                if r.status_code in (200, 201):
                    time.sleep(NOTION_DELAY_SEC)
                    return r.json()
                if r.status_code == 429 or r.status_code >= 500:
                    retry_after = r.headers.get("Retry-After")
                    try:
                        wait = float(retry_after) if retry_after else backoff
                    except ValueError:
                        wait = backoff
                    wait = min(wait, MAX_BACKOFF_SEC) * (0.5 + random.random())
                    log.warning(f"Notion {r.status_code} attempt {attempt}/{MAX_RETRIES}, retry in {wait:.1f}s")
                    time.sleep(wait)
                    backoff = min(backoff * BACKOFF_MULTIPLIER, MAX_BACKOFF_SEC)
                    continue
                error_body = ""
                try:
                    error_body = r.json().get("message", r.text[:500])
                except Exception:
                    error_body = r.text[:500]
                raise requests.HTTPError(f"HTTP {r.status_code}: {error_body}", response=r)
            except (requests.ConnectionError, requests.Timeout) as e:
                log.warning(f"Notion network error attempt {attempt}/{MAX_RETRIES}: {e}")
                time.sleep(min(backoff, MAX_BACKOFF_SEC))
                backoff *= BACKOFF_MULTIPLIER
        raise requests.exceptions.RetryError(f"Notion failed after {MAX_RETRIES} attempts: {url}")

    def create_page(self, parent_id: str, title: str) -> dict:
        return self._request("POST", f"{NOTION_API}/pages", {
            "parent": {"page_id": parent_id},
            "properties": {"title": [{"type": "text", "text": {"content": title[:2000]}}]},
        })

    def append_children(self, block_id: str, children: list[dict]) -> dict:
        """Append blocks in chunks of BLOCKS_PER_REQUEST. For resumable imports, use append_children_batch."""
        result = {}
        for i in range(0, len(children), BLOCKS_PER_REQUEST):
            result = self.append_children_batch(block_id, children[i:i + BLOCKS_PER_REQUEST])
        return result

    def append_children_batch(self, block_id: str, children_batch: list[dict]) -> dict:
        """Append a single batch of children (<=100 blocks). Useful for resumable imports."""
        return self._request(
            "PATCH", f"{NOTION_API}/blocks/{block_id}/children", {"children": children_batch}
        )

    def create_file_upload(self, filename: str, content_type: str) -> dict:
        return self._request("POST", f"{NOTION_API}/file_uploads", {
            "mode": "single_part", "filename": filename, "content_type": content_type,
        })

    def send_file_upload(self, file_upload_id: str, file_path: pathlib.Path) -> dict:
        backoff = INITIAL_BACKOFF_SEC
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                headers = {
                    "Authorization": f"Bearer {self.token}",
                    "Notion-Version": NOTION_VERSION,
                    "Accept": "application/json",
                }
                with file_path.open("rb") as f:
                    r = SESSION.post(
                        f"{NOTION_API}/file_uploads/{file_upload_id}/send",
                        headers=headers, files={"file": (file_path.name, f)}, timeout=120,
                    )
                if r.status_code in (200, 201):
                    time.sleep(NOTION_DELAY_SEC)
                    return r.json()
                if r.status_code == 429 or r.status_code >= 500:
                    retry_after = r.headers.get("Retry-After")
                    try:
                        wait = float(retry_after) if retry_after else backoff
                    except ValueError:
                        wait = backoff
                    wait = min(wait, MAX_BACKOFF_SEC) * (0.5 + random.random())
                    log.warning(f"Upload {r.status_code} attempt {attempt}/{MAX_RETRIES}, retry in {wait:.1f}s")
                    time.sleep(wait)
                    backoff = min(backoff * BACKOFF_MULTIPLIER, MAX_BACKOFF_SEC)
                    continue
                error_body = ""
                try:
                    error_body = r.json().get("message", r.text[:500])
                except Exception:
                    error_body = r.text[:500]
                raise requests.HTTPError(f"Upload HTTP {r.status_code}: {error_body}", response=r)
            except (requests.ConnectionError, requests.Timeout) as e:
                log.warning(f"Upload network error attempt {attempt}/{MAX_RETRIES}: {e}")
                time.sleep(min(backoff, MAX_BACKOFF_SEC))
                backoff = min(backoff * BACKOFF_MULTIPLIER, MAX_BACKOFF_SEC)
        raise requests.exceptions.RetryError(f"Upload failed after {MAX_RETRIES} attempts: {file_path}")


# ══════════════════════════════════════════════
# File utilities
# ══════════════════════════════════════════════
def sanitize(name: str) -> str:
    name = re.sub(r"[<>:\"/\\|?*\x00-\x1F]", "_", name).strip()
    name = re.sub(r"_+", "_", name).strip(". _")
    return (name or "untitled")[:120]


def unique_path(path: pathlib.Path) -> pathlib.Path:
    if not path.exists():
        return path
    stem, suffix, parent = path.stem, path.suffix, path.parent
    counter = 1
    while True:
        new = parent / f"{stem}_{counter}{suffix}"
        if not new.exists():
            return new
        counter += 1


def guess_ext(content_type: str, url: str) -> str:
    ext = mimetypes.guess_extension(content_type.split(";")[0].strip()) if content_type else None
    if ext:
        return ext
    url_path = url.split("?")[0]
    if "." in url_path.split("/")[-1]:
        return "." + url_path.split("/")[-1].rsplit(".", 1)[-1][:5]
    return ".png"


def looks_like_graph_url(u: str) -> bool:
    if not u or u.startswith("data:"):
        return False
    if "graph.microsoft.com" in u:
        return True
    return u.startswith(("/v1.0/", "v1.0/", "/beta/", "beta/"))


def normalize_graph_url(u: str) -> str:
    u = (u or "").strip()
    if u.startswith(("v1.0/", "beta/")):
        return "https://graph.microsoft.com/" + u
    if u.startswith(("/v1.0/", "/beta/")):
        return "https://graph.microsoft.com" + u
    return u


def is_graph_host(u: str) -> bool:
    try:
        return urlparse((u or "").strip()).netloc.lower().endswith("graph.microsoft.com")
    except Exception:
        return False


def is_onenote_resource_url(u: str) -> bool:
    return "onenote/resources" in (u or "").lower()


# ══════════════════════════════════════════════
# Page Exporter (OneNote → local HTML)
# ══════════════════════════════════════════════
class PageExporter:
    """Exports a single OneNote page to local HTML with images."""

    def __init__(self, token_mgr: TokenManager, out_dir: pathlib.Path, stats: Stats):
        self.token_mgr = token_mgr
        self.out_dir = out_dir
        self.stats = stats

    def export_page(self, page: dict, section_path: pathlib.Path) -> pathlib.Path:
        """Export page HTML + images, return the page directory."""
        page_id = page["id"]
        title = sanitize(page.get("title") or page_id)

        page_dir = unique_path(section_path / title)
        page_dir.mkdir(parents=True, exist_ok=True)

        # Fetch HTML
        content_url = f"{GRAPH}/me/onenote/pages/{page_id}/content"
        r = graph_request(content_url, self.token_mgr, accept="text/html")
        html_content = r.content

        # Download and localize images
        html_content = self._localize_resources(html_content, page_dir)

        # Save
        (page_dir / "page.html").write_bytes(html_content)
        meta = {
            "id": page_id,
            "title": page.get("title"),
            "createdDateTime": page.get("createdDateTime"),
            "lastModifiedDateTime": page.get("lastModifiedDateTime"),
            "exported_at": datetime.now(timezone.utc).isoformat(),
        }
        (page_dir / "metadata.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

        self.stats.pages_exported += 1
        return page_dir

    def _localize_resources(self, html_content: bytes, page_dir: pathlib.Path) -> bytes:
        """Download images/objects/attachments and rewrite URLs to local paths."""
        try:

            soup = BeautifulSoup(html_content, "lxml")

        except FeatureNotFound:

            soup = BeautifulSoup(html_content, "html.parser")
        images_dir = page_dir / "_images"
        attachments_dir = page_dir / "_attachments"
        images_created = attachments_created = False
        url_to_rel: dict[str, str] = {}

        def ensure_dir(d: pathlib.Path, flag_name: str) -> bool:
            nonlocal images_created, attachments_created
            if flag_name == "images" and not images_created:
                d.mkdir(parents=True, exist_ok=True)
                images_created = True
            elif flag_name == "attachments" and not attachments_created:
                d.mkdir(parents=True, exist_ok=True)
                attachments_created = True
            return True

        def download(url: str, out_dir: pathlib.Path, hint: str, ct_hint: str = "") -> str:
            url = normalize_graph_url(url)
            if url in url_to_rel:
                return url_to_rel[url]

            if urlparse(url).scheme in ("http", "https") and not is_graph_host(url):
                resp = SESSION.get(url, headers={"Accept": "*/*"}, timeout=60)
                resp.raise_for_status()
            else:
                resp = graph_request(url, self.token_mgr, accept="*/*")

            ct = resp.headers.get("Content-Type") or ct_hint or "application/octet-stream"
            ext = guess_ext(ct, url)
            url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
            fname = f"{sanitize(hint)}_{url_hash}{ext}"
            out_path = unique_path(out_dir / fname)
            out_path.write_bytes(resp.content)
            rel = f"{out_dir.name}/{out_path.name}"
            url_to_rel[url] = rel
            self.stats.images_downloaded += 1
            time.sleep(GRAPH_DELAY_SEC)
            return rel

        # Images
        for idx, img in enumerate(soup.find_all("img")):
            src = img.get("data-fullres-src") or img.get("src") or ""
            if not src:
                continue

            if src.startswith("data:"):
                try:
                    header, b64data = src.split(",", 1)
                    mime = header.split(":")[1].split(";")[0]
                    ext = guess_ext(mime, "")
                    img_bytes = base64.b64decode(b64data)
                    ensure_dir(images_dir, "images")
                    img_path = unique_path(images_dir / f"img_{idx:04d}{ext}")
                    img_path.write_bytes(img_bytes)
                    img["src"] = f"_images/{img_path.name}"
                    self.stats.images_downloaded += 1
                except Exception as e:
                    log.debug(f"Failed to decode data URI: {e}")
                continue

            if looks_like_graph_url(src) or is_onenote_resource_url(src):
                try:
                    ensure_dir(images_dir, "images")
                    ct_hint = img.get("data-fullres-src-type") or img.get("data-src-type") or "image/png"
                    hint = f"img_{idx:04d}"
                    data_id = img.get("data-id")
                    if data_id:
                        hint = f"img_{sanitize(data_id)}"
                    rel = download(src, images_dir, hint, ct_hint)
                    img["src"] = rel
                    img["data-original-src"] = src
                except Exception as e:
                    log.warning(f"Image download failed: {e}")
                    img["data-original-src"] = src
                    img["alt"] = (img.get("alt", "") + " [DOWNLOAD FAILED]").strip()

        # Objects / attachments
        for obj in soup.find_all("object"):
            data_src = obj.get("data") or ""
            if not (looks_like_graph_url(data_src) or is_onenote_resource_url(data_src)):
                continue
            try:
                ensure_dir(attachments_dir, "attachments")
                name = obj.get("data-attachment") or f"att_{hashlib.md5(data_src.encode()).hexdigest()[:8]}"
                ct_hint = obj.get("type") or "application/octet-stream"
                rel = download(data_src, attachments_dir, name, ct_hint)
                obj["data"] = rel
                obj["data-original-src"] = data_src
            except Exception as e:
                log.warning(f"Object download failed: {e}")

        # Links to onenote resources
        for a in soup.find_all("a"):
            href = a.get("href") or ""
            if not is_onenote_resource_url(href):
                continue
            try:
                ensure_dir(attachments_dir, "attachments")
                hint = (a.get_text() or "linked_resource").strip()[:80] or "linked_resource"
                rel = download(href, attachments_dir, hint)
                a["href"] = rel
                a["data-original-href"] = href
            except Exception as e:
                log.warning(f"Link resource download failed: {e}")

        return soup.encode(formatter="html5")


# ══════════════════════════════════════════════
# HTML → Notion block converter
# ══════════════════════════════════════════════
class HtmlToNotionConverter:
    """Converts OneNote HTML to Notion block structures."""

    def __init__(
        self,
        page_dir: pathlib.Path,
        image_base_url: Optional[str],
        upload_local: bool,
        notion: NotionClient,
        manifest: Manifest,
        stats: Stats,
    ):
        self.page_dir = page_dir
        self.image_base_url = image_base_url.rstrip("/") if image_base_url else None
        self.upload_local = upload_local
        self.notion = notion
        self.manifest = manifest
        self.stats = stats

    def convert(self, html_content: bytes) -> list[dict]:
        try:

            soup = BeautifulSoup(html_content, "lxml")

        except FeatureNotFound:

            soup = BeautifulSoup(html_content, "html.parser")
        body = soup.find("body") or soup
        blocks: list[dict] = []
        self._process_children(body, blocks)
        return blocks

    def _process_children(self, element: Tag, blocks: list[dict]):
        for child in element.children:
            if isinstance(child, NavigableString):
                text = child.strip()
                if text:
                    blocks.append(self._para(self._chunk_text(text)))
                continue
            if not isinstance(child, Tag):
                continue

            tag = child.name.lower()

            if tag in ("head", "style", "script", "meta", "link", "title"):
                continue
            elif tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
                blocks.append(self._heading(child))
            elif tag in ("p", "div", "span"):
                b = self._convert_p_or_div(child)
                if b:
                    blocks.append(b)
            elif tag in ("ul", "ol"):
                blocks.extend(self._convert_list(child, ordered=(tag == "ol")))
            elif tag == "pre":
                blocks.append(self._convert_pre(child))
            elif tag == "blockquote":
                blocks.append({"type": "quote", "quote": {
                    "rich_text": self._inline_rt(child),
                }})
            elif tag == "table":
                blocks.extend(self._convert_table(child))
            elif tag == "img":
                b = self._convert_image(child)
                if b:
                    blocks.append(b)
            elif tag == "hr":
                blocks.append({"type": "divider", "divider": {}})
            elif tag == "br":
                blocks.append(self._para([]))
            elif tag == "figure":
                img = child.find("img")
                if img:
                    b = self._convert_image(img)
                    if b:
                        blocks.append(b)
                cap = child.find("figcaption")
                if cap:
                    blocks.append(self._para(self._inline_rt(cap)))
            elif tag == "object":
                b = self._convert_object(child)
                if b:
                    blocks.append(b)
            else:
                self._process_children(child, blocks)

    # ── Block builders ───────────────────────────

    def _para(self, rt: list[dict]) -> dict:
        return {"type": "paragraph", "paragraph": {"rich_text": rt}}

    def _heading(self, tag: Tag) -> dict:
        level = min(int(tag.name[1]), 3)
        ht = f"heading_{level}"
        return {"type": ht, ht: {"rich_text": self._inline_rt(tag)}}

    def _convert_p_or_div(self, tag: Tag) -> Optional[dict]:
        children = [c for c in tag.children if not (isinstance(c, NavigableString) and not c.strip())]
        if len(children) == 1 and isinstance(children[0], Tag) and children[0].name == "img":
            return self._convert_image(children[0])
        rt = self._inline_rt(tag)
        return self._para(rt) if rt else None

    def _convert_list(self, list_tag: Tag, ordered: bool) -> list[dict]:
        blocks = []
        for li in list_tag.find_all("li", recursive=False):
            checkbox = li.find("input", {"type": "checkbox"})
            if checkbox:
                checked = checkbox.get("checked") is not None
                checkbox.decompose()
                block = {"type": "to_do", "to_do": {
                    "rich_text": self._inline_rt(li, skip_lists=True), "checked": checked,
                }}
            elif ordered:
                block = {"type": "numbered_list_item", "numbered_list_item": {
                    "rich_text": self._inline_rt(li, skip_lists=True),
                }}
            else:
                block = {"type": "bulleted_list_item", "bulleted_list_item": {
                    "rich_text": self._inline_rt(li, skip_lists=True),
                }}
            # Nested lists
            nested = []
            for nl in li.find_all(["ul", "ol"], recursive=False):
                nested.extend(self._convert_list(nl, ordered=(nl.name == "ol")))
            if nested:
                block[block["type"]]["children"] = nested
            blocks.append(block)
        return blocks

    def _convert_pre(self, pre: Tag) -> dict:
        code = pre.find("code")
        text = code.get_text() if code else pre.get_text()
        lang = "plain text"
        if code:
            for cls in code.get("class", []):
                if cls.startswith("language-"):
                    lang = cls[9:]
                    break
        return {"type": "code", "code": {"rich_text": self._chunk_text(text), "language": lang}}

    def _convert_table(self, table: Tag) -> list[dict]:
        rows = []
        has_header = False
        thead = table.find("thead")
        if thead:
            for tr in thead.find_all("tr"):
                rows.append([self._inline_rt(td) for td in tr.find_all(["th", "td"])])
            has_header = True
        tbody = table.find("tbody") or table
        for tr in tbody.find_all("tr", recursive=False):
            if tr.parent and tr.parent.name == "thead":
                continue
            cells = [self._inline_rt(td) for td in tr.find_all(["th", "td"])]
            if cells:
                rows.append(cells)
        if not rows:
            return []
        max_cols = max(len(r) for r in rows)
        for r in rows:
            while len(r) < max_cols:
                r.append([])
        return [{"type": "table", "table": {
            "table_width": max_cols, "has_column_header": has_header,
            "has_row_header": False,
            "children": [{"type": "table_row", "table_row": {"cells": r}} for r in rows],
        }}]

    # ── Image / file handling ────────────────────

    def _convert_image(self, img: Tag) -> Optional[dict]:
        src = img.get("src", "")
        if not src:
            return None

        caption_text = img.get("alt", "")
        caption = [{"type": "text", "text": {"content": caption_text}}] if caption_text else []

        # External URL?
        url = self._resolve_external_url(src)
        if url:
            self.stats.images_linked += 1
            return {"type": "image", "image": {
                "type": "external", "external": {"url": url}, "caption": caption,
            }}

        # Upload to Notion?
        fid = self._upload_local_file(self.page_dir / src)
        if fid:
            return {"type": "image", "image": {
                "type": "file_upload", "file_upload": {"id": fid}, "caption": caption,
            }}

        # Placeholder
        self.stats.images_skipped += 1
        alt = img.get("alt", "[image]")
        return self._para([{"type": "text", "text": {"content": f"📎 {alt}"}}])

    def _convert_object(self, obj: Tag) -> Optional[dict]:
        data = obj.get("data", "")
        name = obj.get("data-attachment", data.split("/")[-1] if data else "attachment")
        if not data:
            return None

        url = self._resolve_external_url(data)
        if url:
            return self._para([{"type": "text", "text": {"content": f"📎 {name}", "link": {"url": url}}}])

        fid = self._upload_local_file(self.page_dir / data)
        if fid:
            return {"type": "file", "file": {
                "type": "file_upload", "file_upload": {"id": fid}, "caption": [],
            }}

        return self._para([{"type": "text", "text": {
            "content": f"📎 {name} (attachment — enable uploads to include)",
        }}])

    def _resolve_external_url(self, src: str) -> Optional[str]:
        if src.startswith(("http://", "https://")):
            return src
        if not self.image_base_url:
            return None
        full = (self.page_dir / src).resolve()
        try:
            # Walk up to find export root
            root = self.page_dir
            for _ in range(10):
                if (root / "_manifest.json").exists():
                    break
                if root.parent == root:
                    break
                root = root.parent
            rel = full.relative_to(root)
            return f"{self.image_base_url}/{rel.as_posix()}"
        except (ValueError, RuntimeError):
            return f"{self.image_base_url}/{src}"

    def _upload_local_file(self, local_path: pathlib.Path) -> Optional[str]:
        if not self.upload_local:
            return None
        try:
            local_path = local_path.resolve()
        except Exception:
            pass
        key = str(local_path)

        # Check cache
        cached = self.manifest.get_uploaded_file_id(key)
        if cached:
            return cached

        if not local_path.exists() or not local_path.is_file():
            return None

        # Notion direct uploads (single_part) support up to 20 MB per file by default.
        try:
            max_mb = int(os.environ.get("NOTION_MAX_UPLOAD_MB", "20"))
        except ValueError:
            max_mb = 20
        max_bytes = max_mb * 1024 * 1024
        try:
            if local_path.stat().st_size > max_bytes:
                log.warning(f"Skipping upload (> {max_mb}MB): {local_path.name}")
                return None
        except Exception:
            pass

        ct, _ = mimetypes.guess_type(str(local_path))
        ct = ct or "application/octet-stream"

        try:
            upload_obj = self.notion.create_file_upload(local_path.name, ct)
            fid = upload_obj["id"]
            sent = self.notion.send_file_upload(fid, local_path)
            if sent.get("status") != "uploaded":
                self.stats.uploads_failed += 1
                return None
            self.manifest.remember_uploaded_file(key, fid)
            self.stats.images_uploaded += 1
            return fid
        except Exception as e:
            self.stats.uploads_failed += 1
            log.warning(f"Upload failed for {local_path.name}: {e}")
            return None

    # ── Rich text / inline formatting ────────────

    def _inline_rt(self, element: Tag, skip_lists: bool = False) -> list[dict]:
        result: list[dict] = []
        self._walk_inline(element, result, {}, skip_lists)
        return self._clamp_rt(result)

    def _walk_inline(self, node, result: list, ann: dict, skip_lists: bool):
        for child in node.children:
            if isinstance(child, NavigableString):
                text = str(child)
                if text:
                    result.append(self._rt_item(text, ann))
                continue
            if not isinstance(child, Tag):
                continue

            tag = child.name.lower()

            if skip_lists and tag in ("ul", "ol"):
                continue
            if tag in ("table", "pre", "div", "p", "h1", "h2", "h3", "h4", "h5", "h6"):
                text = child.get_text()
                if text.strip():
                    result.append(self._rt_item(text, ann))
                continue
            if tag == "img":
                result.append(self._rt_item(f" {child.get('alt', '[image]')} ", ann))
                continue
            if tag == "br":
                result.append(self._rt_item("\n", ann))
                continue

            new_ann = dict(ann)
            if tag in ("b", "strong"):
                new_ann["bold"] = True
            elif tag in ("i", "em"):
                new_ann["italic"] = True
            elif tag == "u":
                new_ann["underline"] = True
            elif tag in ("s", "del", "strike"):
                new_ann["strikethrough"] = True
            elif tag == "code":
                new_ann["code"] = True
            elif tag == "a":
                href = child.get("href", "")
                if href and not href.startswith("#"):
                    link_text = child.get_text()
                    if link_text.strip():
                        item = self._rt_item(link_text, new_ann)
                        item["text"]["link"] = {"url": href[:2000]}
                        result.append(item)
                    continue
            elif tag in ("sup", "sub"):
                marker = "^" if tag == "sup" else "_"
                result.append(self._rt_item(marker, ann))
                self._walk_inline(child, result, ann, skip_lists)
                continue

            self._walk_inline(child, result, new_ann, skip_lists)

    def _rt_item(self, text: str, ann: dict) -> dict:
        item: dict = {"type": "text", "text": {"content": text}}
        filtered = {k: True for k in ("bold", "italic", "underline", "strikethrough", "code") if ann.get(k)}
        if filtered:
            item["annotations"] = filtered
        return item

    def _chunk_text(self, text: str) -> list[dict]:
        chunks = []
        while text:
            chunks.append({"type": "text", "text": {"content": text[:RICH_TEXT_MAX_CHARS]}})
            text = text[RICH_TEXT_MAX_CHARS:]
        return chunks

    def _clamp_rt(self, items: list[dict]) -> list[dict]:
        # Split oversized
        clamped = []
        for item in items:
            content = item.get("text", {}).get("content", "")
            if len(content) <= RICH_TEXT_MAX_CHARS:
                clamped.append(item)
            else:
                ann = item.get("annotations", {})
                link = item.get("text", {}).get("link")
                while content:
                    new = {"type": "text", "text": {"content": content[:RICH_TEXT_MAX_CHARS]}}
                    if ann:
                        new["annotations"] = dict(ann)
                    if link:
                        new["text"]["link"] = link
                    clamped.append(new)
                    content = content[RICH_TEXT_MAX_CHARS:]

        # Consolidate adjacent identical items
        if len(clamped) <= 1:
            return clamped
        out = [clamped[0]]
        for item in clamped[1:]:
            prev = out[-1]
            if (prev.get("annotations", {}) == item.get("annotations", {})
                    and prev.get("text", {}).get("link") == item.get("text", {}).get("link")
                    and len(prev["text"]["content"]) + len(item["text"]["content"]) <= RICH_TEXT_MAX_CHARS):
                prev["text"]["content"] += item["text"]["content"]
            else:
                out.append(item)
        return out


# ══════════════════════════════════════════════
# Migration orchestrator
# ══════════════════════════════════════════════
def ensure_notion_node(
    onenote_id: str,
    name: str,
    entity_type: str,
    parent_notion_id: str,
    notion: NotionClient,
    manifest: Manifest,
    stats: Stats,
) -> Optional[str]:
    """Create or reuse a Notion page for a structural node (notebook/section/group)."""
    existing = manifest.get_structure_notion_id(onenote_id)
    if existing:
        return existing

    try:
        result = notion.create_page(parent_notion_id, name)
        notion_id = result["id"]
        manifest.set_structure(onenote_id, name, entity_type, notion_id)

        if entity_type == "notebook":
            stats.notebooks += 1
        elif entity_type == "section_group":
            stats.section_groups += 1
        elif entity_type == "section":
            stats.sections += 1

        return notion_id
    except Exception as e:
        log.error(f"Failed to create {entity_type} '{name}': {e}")
        stats.errors.append(f"{entity_type} '{name}': {e}")
        return None


def migrate_page(
    page: dict,
    section_path: pathlib.Path,
    section_notion_id: str,
    exporter: PageExporter,
    notion: NotionClient,
    manifest: Manifest,
    stats: Stats,
    image_base_url: Optional[str],
    upload_local: bool,
):
    """Atomic export+import of a single page with resumable progress.

    States:
      - None/pending: export + create Notion page + append blocks
      - exported: exported locally, Notion page not created yet
      - notion_created: Notion page exists, continue appending blocks from import_next_block
      - imported: fully done
    """
    page_id = page["id"]
    title = page.get("title") or page_id
    status = manifest.page_status(page_id)

    if status == "imported":
        stats.pages_skipped += 1
        return

    try:
        # ── Phase 1: Export ─────────────────────────────────────────────
        page_dir: Optional[pathlib.Path] = None
        if status in ("exported", "notion_created"):
            local = manifest.get_page_local_path(page_id)
            if local and pathlib.Path(local).exists():
                page_dir = pathlib.Path(local)
                log.info(f"      ↳ Resuming ({status}): {sanitize(title)}")

        if page_dir is None:
            # (Re-)export HTML + images. mark_exported preserves any existing
            # notion_id in the manifest so we don't create a duplicate Notion page.
            page_dir = exporter.export_page(page, section_path)
            manifest.mark_exported(page_id, title, str(page_dir))
            if status != "notion_created":
                status = "exported"
            log.info(f"      ✓ Exported: {sanitize(title)}")

        # ── Build blocks (deterministic) ────────────────────────────────
        html_content = (page_dir / "page.html").read_bytes()
        converter = HtmlToNotionConverter(
            page_dir, image_base_url, upload_local, notion, manifest, stats,
        )
        blocks = converter.convert(html_content)

        # ── Phase 2: Ensure Notion page exists ─────────────────────────
        # Always check manifest for an existing notion_id first —
        # catches re-export scenarios where status reverted to "exported"
        # but notion_id was preserved from a prior notion_created state.
        notion_page_id: Optional[str] = (manifest.pages.get(page_id) or {}).get("notion_id")

        if not notion_page_id:
            result = notion.create_page(section_notion_id, title[:2000])
            notion_page_id = result["id"]
            manifest.mark_notion_created(page_id, notion_page_id)
            status = "notion_created"
            log.info(f"      ✓ Created Notion page: {sanitize(title)}")

        # ── Phase 3: Append remaining blocks with progress tracking ─────
        next_idx = manifest.get_import_next_block(page_id)
        if next_idx < 0:
            next_idx = 0
        if next_idx > len(blocks):
            next_idx = len(blocks)

        remaining = blocks[next_idx:]
        appended = 0
        for i in range(0, len(remaining), BLOCKS_PER_REQUEST):
            batch = remaining[i:i + BLOCKS_PER_REQUEST]
            if not batch:
                continue
            notion.append_children_batch(notion_page_id, batch)
            appended += len(batch)
            manifest.set_import_next_block(page_id, next_idx + appended)

        if appended:
            stats.blocks_created += appended

        manifest.mark_imported(page_id, notion_page_id)
        stats.pages_imported += 1
        log.info(
            f"      ✓ Imported: {sanitize(title)} "
            f"({next_idx + appended}/{len(blocks)} blocks)"
        )

    except Exception as e:
        error_msg = f"Page '{sanitize(title)}' (id={page_id}): {e}"
        log.error(f"      FAILED: {error_msg}")
        stats.pages_failed += 1
        stats.errors.append(error_msg)

def migrate_sections(
    sections_url: str,
    parent_path: pathlib.Path,
    parent_notion_id: str,
    exporter: PageExporter,
    token_mgr: TokenManager,
    notion: NotionClient,
    manifest: Manifest,
    stats: Stats,
    image_base_url: Optional[str],
    upload_local: bool,
):
    """Process all sections and their pages."""
    for sec in paged_json(sections_url, token_mgr):
        sec_name = sanitize(sec.get("displayName") or sec["id"])
        sec_notion_id = ensure_notion_node(
            sec["id"], sec_name, "section", parent_notion_id, notion, manifest, stats,
        )
        if not sec_notion_id:
            continue

        sec_path = parent_path / sec_name
        sec_path.mkdir(parents=True, exist_ok=True)
        log.info(f"    Section: {sec_name}")

        pages_url = (
            f"{GRAPH}/me/onenote/sections/{sec['id']}/pages"
            f"?$select=id,title,createdDateTime,lastModifiedDateTime"
            f"&$orderby=createdDateTime"
        )
        for page in paged_json(pages_url, token_mgr):
            migrate_page(
                page, sec_path, sec_notion_id, exporter, notion, manifest,
                stats, image_base_url, upload_local,
            )
            time.sleep(GRAPH_DELAY_SEC)


def migrate_section_groups(
    sg_url: str,
    parent_path: pathlib.Path,
    parent_notion_id: str,
    exporter: PageExporter,
    token_mgr: TokenManager,
    notion: NotionClient,
    manifest: Manifest,
    stats: Stats,
    image_base_url: Optional[str],
    upload_local: bool,
):
    """Recursively process section groups."""
    for sg in paged_json(sg_url, token_mgr):
        sg_name = sanitize(sg.get("displayName") or sg["id"])
        sg_notion_id = ensure_notion_node(
            sg["id"], sg_name, "section_group", parent_notion_id, notion, manifest, stats,
        )
        if not sg_notion_id:
            continue

        sg_path = parent_path / sg_name
        sg_path.mkdir(parents=True, exist_ok=True)
        log.info(f"  Section Group: {sg_name}")

        # Sections in this group
        sections_url = f"{GRAPH}/me/onenote/sectionGroups/{sg['id']}/sections?$select=id,displayName"
        migrate_sections(
            sections_url, sg_path, sg_notion_id, exporter, token_mgr,
            notion, manifest, stats, image_base_url, upload_local,
        )

        # Nested section groups (recursive)
        nested_url = f"{GRAPH}/me/onenote/sectionGroups/{sg['id']}/sectionGroups?$select=id,displayName"
        migrate_section_groups(
            nested_url, sg_path, sg_notion_id, exporter, token_mgr,
            notion, manifest, stats, image_base_url, upload_local,
        )


# ══════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════
def main():
    client_id = os.environ.get("MS_CLIENT_ID")
    notion_token = os.environ.get("NOTION_TOKEN")
    parent_id = os.environ.get("NOTION_PARENT_ID")
    out_dir = pathlib.Path(os.environ.get("OUT_DIR", "onenote_migration")).resolve()
    image_base_url = os.environ.get("IMAGE_BASE_URL")
    upload_local = os.environ.get("UPLOAD_LOCAL_FILES", "1").strip() not in ("0", "false", "False")

    # ── Validate inputs ──
    missing = []
    if not client_id:
        missing.append("MS_CLIENT_ID")
    if not notion_token:
        missing.append("NOTION_TOKEN")
    if not parent_id:
        missing.append("NOTION_PARENT_ID")

    if missing:
        print("ERROR: Missing required environment variables:")
        for var in missing:
            print(f"  • {var}")
        print()
        print("Setup:")
        print("  MS_CLIENT_ID     → Azure AD app registration Client ID")
        print("    https://portal.azure.com → App registrations → New → Personal accounts")
        print("    Redirect URI: https://login.microsoftonline.com/common/oauth2/nativeclient")
        print("    API permissions: Microsoft Graph → Delegated → Notes.Read")
        print()
        print("  NOTION_TOKEN     → Notion integration secret (starts with ntn_)")
        print("    https://www.notion.so/my-integrations → Create integration")
        print()
        print("  NOTION_PARENT_ID → Target Notion page ID (32-char hex from URL)")
        print("    Share the page with your integration via ··· → Connections")
        raise SystemExit(1)

    parent_id = parent_id.replace("-", "")
    out_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Output cache: {out_dir}")
    log.info(f"Notion parent: {parent_id[:8]}...")
    if image_base_url:
        log.info(f"Image base URL: {image_base_url}")
    elif upload_local:
        log.info("Images will be uploaded directly to Notion via File Upload API")
    else:
        log.warning("No IMAGE_BASE_URL and uploads disabled — images will be placeholders")

    # ── Initialize ──
    token_mgr = TokenManager(client_id, out_dir / "_msal_cache.json")
    notion = NotionClient(notion_token)
    manifest = Manifest(out_dir / "_manifest.json")
    stats = Stats()
    exporter = PageExporter(token_mgr, out_dir, stats)

    # ── Verify connections ──
    token_mgr.authenticate()

    try:
        SESSION.get(
            f"{NOTION_API}/pages/{parent_id}",
            headers=notion.headers, timeout=30,
        ).raise_for_status()
        log.info("Notion connection verified ✓")
    except Exception as e:
        print(f"ERROR: Cannot access Notion parent page: {e}")
        print("Make sure you've shared the page with your integration.")
        raise SystemExit(1)

    # ── Run migration ──
    log.info("Starting migration...")
    try:
        notebooks_url = f"{GRAPH}/me/onenote/notebooks?$select=id,displayName&$orderby=displayName"

        for nb in paged_json(notebooks_url, token_mgr):
            nb_name = sanitize(nb.get("displayName") or nb["id"])
            nb_notion_id = ensure_notion_node(
                nb["id"], nb_name, "notebook", parent_id, notion, manifest, stats,
            )
            if not nb_notion_id:
                continue

            nb_path = out_dir / nb_name
            nb_path.mkdir(parents=True, exist_ok=True)
            log.info(f"Notebook: {nb_name}")

            # Top-level sections
            sections_url = f"{GRAPH}/me/onenote/notebooks/{nb['id']}/sections?$select=id,displayName"
            migrate_sections(
                sections_url, nb_path, nb_notion_id, exporter, token_mgr,
                notion, manifest, stats, image_base_url, upload_local,
            )

            # Section groups (recursive)
            sg_url = f"{GRAPH}/me/onenote/notebooks/{nb['id']}/sectionGroups?$select=id,displayName"
            migrate_section_groups(
                sg_url, nb_path, nb_notion_id, exporter, token_mgr,
                notion, manifest, stats, image_base_url, upload_local,
            )

    except KeyboardInterrupt:
        log.warning("\nMigration interrupted — progress saved. Run again to resume.")
    except Exception as e:
        log.error(f"Fatal error: {e}")
        stats.errors.append(f"Fatal: {e}")
    finally:
        manifest.save()
        print(stats.summary())

        if stats.errors:
            error_path = out_dir / "_errors.json"
            error_path.write_text(json.dumps(stats.errors, indent=2), encoding="utf-8")
            log.info(f"Error details: {error_path}")

        log.info(f"Manifest: {out_dir / '_manifest.json'}")
        log.info("Run the same command again to resume if incomplete.")


if __name__ == "__main__":
    main()
