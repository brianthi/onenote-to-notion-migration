# OneNote to Notion Migration

Single-file migration tool that walks your OneNote hierarchy through Microsoft Graph and imports pages into Notion while preserving structure:

`Notebook -> Section Group -> Section -> Page`

The script is resumable. If interrupted, run it again and it continues from saved state.

## What this does

- Authenticates to Microsoft Graph with device login (MSAL cache persisted locally).
- Reads all notebooks, sections, section groups, and pages.
- Exports each page HTML and downloads referenced images.
- Converts HTML content into Notion blocks.
- Creates Notion pages and appends blocks in batches.
- Tracks progress in a manifest so retries do not re-import completed pages.

## Features

- Resume-safe migration (`pending -> exported -> notion_created -> imported`)
- Block-level resume (continues mid-page append)
- Retry with exponential backoff for Graph and Notion API calls
- Notion file upload support for images
- Image upload deduplication (reuse previous uploaded file IDs)
- Graceful `Ctrl+C` handling with progress saved
- Final migration summary and error log export

## Prerequisites

- Python 3.10+ (recommended)
- A Microsoft account with OneNote data
- Azure app registration for Graph `Notes.Read` delegated permission
- A Notion integration with access to your target parent page

Install dependencies:

```bash
pip install msal requests beautifulsoup4 lxml
```

## Required environment variables

- `MS_CLIENT_ID`: Azure AD app client ID
- `NOTION_TOKEN`: Notion integration token
- `NOTION_PARENT_ID`: Target Notion parent page ID (32-char hex; dashes are accepted)

## Optional environment variables

- `OUT_DIR` (default: `onenote_migration`)
- `IMAGE_BASE_URL` (if set, image URLs are linked from this base instead of uploading files)
- `UPLOAD_LOCAL_FILES` (default: `1`; set `0`, `false`, or `False` to disable uploads)
- `NOTION_MAX_UPLOAD_MB` (default: `20`)
- `NOTION_VERSION` (default: `2025-09-03`)

## Setup

### 1) Azure app registration

- Create app: Azure Portal -> App registrations -> New registration
- Supported account type: Personal Microsoft accounts
- Redirect URI: `https://login.microsoftonline.com/common/oauth2/nativeclient`
- API permission: Microsoft Graph -> Delegated -> `Notes.Read`
- Copy the Application (client) ID into `MS_CLIENT_ID`

### 2) Notion integration

- Create integration at `https://www.notion.so/my-integrations`
- Copy the integration token into `NOTION_TOKEN`
- Open your destination page in Notion and share it with the integration
- Extract the page ID from URL into `NOTION_PARENT_ID`

## Usage

```bash
export MS_CLIENT_ID="your-azure-client-id"
export NOTION_TOKEN="your-notion-token"
export NOTION_PARENT_ID="your-notion-parent-page-id"

python migrate_onenote_to_notion.py
```

On first run, you will complete Microsoft device authentication in the browser using the code shown in terminal.

To resume after interruption, run the same command again.

## Output and state files

By default, all migration artifacts are written under `./onenote_migration`:

- `_manifest.json`: canonical resume state for structure/pages/uploads
- `_msal_cache.json`: cached auth tokens
- `_errors.json`: written when migration errors occur
- Per-page exported HTML and downloaded images in notebook/section folders

## Notes and caveats

- The script verifies your Notion parent page is accessible before migration starts.
- If `IMAGE_BASE_URL` is set, images are linked instead of uploaded to Notion.
- If uploads are disabled and no `IMAGE_BASE_URL` is set, image placeholders are used.
- Existing Notion pages created by this script are not auto-deleted on failures.

## Troubleshooting

- Missing env var errors: set `MS_CLIENT_ID`, `NOTION_TOKEN`, `NOTION_PARENT_ID`.
- Notion access error: ensure target page is shared with the integration.
- Upload size issues: increase `NOTION_MAX_UPLOAD_MB` or use `IMAGE_BASE_URL`.
- Interrupted migration: rerun the script; progress is read from `_manifest.json`.
