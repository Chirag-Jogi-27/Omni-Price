"""Immutable raw-landing run layout for OmniPrice ingestion streams.

Raw files are written locally first and can be copied unchanged to S3 and to a
Unity Catalog Volume.  A consumer validates each JSONL file against the
date-level manifest before processing it.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


RAW_ROOT = Path("data_lake/raw")
RAW_SCHEMA_VERSION = "1.0"


@dataclass(frozen=True)
class RawRun:
    platform: str
    source: str
    run_id: str
    started_at: datetime
    run_dir: Path

    @property
    def ingest_date(self) -> str:
        return self.started_at.date().isoformat()


def create_raw_run(platform: str, source: str, raw_root: Path = RAW_ROOT) -> RawRun:
    """Create a UTC-partitioned raw run directory contract."""
    if not platform or not source:
        raise ValueError("platform and source are required")

    started_at = datetime.now(timezone.utc)
    run_id = f"{started_at.strftime('%Y%m%dT%H%M%SZ')}_{uuid4().hex[:8]}"
    run_dir = (
        Path(raw_root)
        / f"platform={platform}"
        / f"source={source}"
        / f"ingest_date={started_at.date().isoformat()}"
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    return RawRun(platform, source, run_id, started_at, run_dir)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def finalize_raw_run(
    run: RawRun,
    data_file: Path,
    record_count: int,
    scraper_name: str,
    extra_metadata: dict | None = None,
) -> Path:
    """Append a completed JSONL entry to the single date-level manifest."""
    data_file = Path(data_file)
    if not data_file.is_file() or data_file.parent != run.run_dir:
        raise ValueError("data_file must exist directly inside this raw run directory")

    completed_at = datetime.now(timezone.utc)
    manifest_path = run.run_dir / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    else:
        manifest = {
            "raw_schema_version": RAW_SCHEMA_VERSION,
            "platform": run.platform,
            "source": run.source,
            "ingest_date": run.ingest_date,
            "files": [],
        }

    file_entry = {
        "name": data_file.name,
        "format": "jsonl",
        "run_id": run.run_id,
        "record_count": record_count,
        "size_bytes": data_file.stat().st_size,
        "sha256": sha256_file(data_file),
        "started_at": run.started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "scraper_name": scraper_name,
        "extra_metadata": extra_metadata or {},
    }
    manifest["files"] = [entry for entry in manifest.get("files", []) if entry.get("name") != data_file.name]
    manifest["files"].append(file_entry)
    manifest["updated_at"] = completed_at.isoformat()

    # Replace the manifest only after the complete JSON has been written.
    temp_manifest = manifest_path.with_suffix(".json.tmp")
    temp_manifest.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temp_manifest.replace(manifest_path)
    return manifest_path


def completed_raw_files(platform: str, source: str, raw_root: Path = RAW_ROOT) -> list[Path]:
    """Return JSONL files registered in their date-level manifest."""
    source_root = (
        Path(raw_root)
        / f"platform={platform}"
        / f"source={source}"
    )
    if not source_root.exists():
        return []
    files = []
    for path in source_root.rglob("*.jsonl"):
        manifest_path = path.parent / "manifest.json"
        if not manifest_path.is_file():
            continue
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        registered = {entry.get("name") for entry in manifest.get("files", [])}
        if path.name in registered:
            files.append(path)
    return sorted(files)
