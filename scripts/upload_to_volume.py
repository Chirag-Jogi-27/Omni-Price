"""
OmniPrice Local-to-Databricks Volume Uploader.

Uploads local raw scraped JSONL files and manifests to Databricks Unity Catalog Volume:
/Volumes/omniprice/raw/raw_files/platform=amazon_in/
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timezone
import requests

# Load credentials strictly from .env
env_file = Path(__file__).resolve().parent.parent / ".env"
if env_file.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(env_file)
    except ImportError:
        with open(env_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN") or os.getenv("DBT_TOKEN")
VOLUME_BASE_PATH = os.getenv("VOLUME_BASE_PATH", "/Volumes/omniprice/raw/raw_files/platform=amazon_in")

if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
    print("❌ ERROR: DATABRICKS_HOST or DATABRICKS_TOKEN missing in .env file.")
    sys.exit(1)

def get_headers():
    return {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/octet-stream"
    }

def get_remote_file_size(remote_volume_path: str) -> int | None:
    """Returns remote file size in bytes if it exists, else None."""
    try:
        url = f"https://{DATABRICKS_HOST}/api/2.0/fs/files{remote_volume_path}"
        r = requests.head(url, headers=get_headers(), timeout=10)
        if r.status_code == 200 and "Content-Length" in r.headers:
            return int(r.headers["Content-Length"])
    except Exception:
        pass
    return None

def upload_file_to_volume(local_path: Path, remote_volume_path: str, overwrite: bool = True) -> bool:
    """Uploads a single file to Databricks Volume via Files API with smart deduplication."""
    local_size = local_path.stat().st_size
    file_size_mb = local_size / (1024 * 1024)

    # For immutable data files (.jsonl), skip if already synced on Volume with identical size
    if local_path.name != "manifest.json":
        remote_size = get_remote_file_size(remote_volume_path)
        if remote_size is not None and remote_size == local_size:
            print(f"  ⚡ Skipping: {local_path.name} ({file_size_mb:.2f} MB, already synced on Volume)")
            return True

    query = "?overwrite=true" if overwrite else ""
    url = f"https://{DATABRICKS_HOST}/api/2.0/fs/files{remote_volume_path}{query}"
    
    print(f"  -> Uploading: {local_path.name} ({file_size_mb:.2f} MB)...")
    
    with open(local_path, "rb") as f:
        resp = requests.put(url, headers=get_headers(), data=f)
        
    if resp.status_code in (200, 201, 204):
        print(f"     ✅ Uploaded successfully -> {remote_volume_path}")
        return True
    else:
        print(f"     ❌ Upload failed ({resp.status_code}): {resp.text}")
        return False

def sync_date_to_volume(target_date: str = None):
    """
    Scans local data lake for the given date (default: today) and uploads
    both search and pdp raw JSONL files and manifests.
    """
    if not target_date:
        target_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
    print(f"\n=======================================================")
    print(f"🚀 Starting OmniPrice Raw Volume Sync for Date: {target_date}")
    print(f"   Target Volume: {VOLUME_BASE_PATH}")
    print(f"=======================================================\n")
    
    local_base = Path("data_lake/raw/platform=amazon_in")
    sources = ["search", "pdp"]
    
    total_uploaded = 0
    total_failed = 0
    
    for src in sources:
        ingest_dir = local_base / f"source={src}" / f"ingest_date={target_date}"
        if not ingest_dir.exists():
            print(f"ℹ️ No local directory found for source='{src}' on date={target_date}. Skipping.")
            continue
            
        print(f"📂 Processing source='{src}' ({ingest_dir})...")
        files_to_upload = list(ingest_dir.glob("*.jsonl")) + list(ingest_dir.glob("manifest.json"))
        
        if not files_to_upload:
            print(f"   No files to upload in {ingest_dir}.")
            continue
            
        for file in sorted(files_to_upload):
            remote_path = f"{VOLUME_BASE_PATH}/source={src}/ingest_date={target_date}/{file.name}"
            success = upload_file_to_volume(file, remote_path)
            if success:
                total_uploaded += 1
            else:
                total_failed += 1
                
    print(f"\n🎉 Sync completed: {total_uploaded} files uploaded, {total_failed} failed.")
    return total_failed == 0

if __name__ == "__main__":
    args = sys.argv[1:]
    if "--all" in args:
        local_base = Path("data_lake/raw/platform=amazon_in")
        found_dates = set()
        for p in local_base.glob("source=*/ingest_date=*"):
            found_dates.add(p.name.replace("ingest_date=", ""))
        dates_to_sync = sorted(list(found_dates))
        print(f"📦 Found {len(dates_to_sync)} dates locally to sync: {dates_to_sync}")
        all_success = True
        for d in dates_to_sync:
            if not sync_date_to_volume(d):
                all_success = False
        sys.exit(0 if all_success else 1)
    elif len(args) > 1:
        all_success = True
        for d in args:
            if not sync_date_to_volume(d):
                all_success = False
        sys.exit(0 if all_success else 1)
    else:
        date_arg = args[0] if args else None
        success = sync_date_to_volume(date_arg)
        sys.exit(0 if success else 1)
