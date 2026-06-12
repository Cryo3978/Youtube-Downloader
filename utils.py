import json
import os
from pathlib import Path
from typing import Any, List, Tuple


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


def append_unique_metas(existing: List[dict], new_entries: List[dict]) -> Tuple[List[dict], int]:
    seen = {m["id"] for m in existing if m.get("id")}
    added: List[dict] = []
    for entry in new_entries:
        vid = entry.get("id")
        if not vid or vid in seen:
            continue
        seen.add(vid)
        added.append(entry)
    return existing + added, len(added)


def video_file_exists(save_root: Path, vid: str, ext: str = "mp4") -> bool:
    if list(save_root.glob(f"{vid} - *.{ext}")):
        return True
    return any(
        p.suffix.lower() in (".mp4", ".mkv", ".webm")
        for p in save_root.glob(f"{vid}.*")
    )


def is_in_download_archive(archive_path: Path, vid: str) -> bool:
    if not archive_path.is_file():
        return False
    needle = f"youtube {vid}"
    with open(archive_path, encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if stripped == needle or stripped.endswith(f" {vid}"):
                return True
    return False


def download_succeeded(save_root: Path, archive_path: Path, vid: str, ext: str = "mp4") -> bool:
    if video_file_exists(save_root, vid, ext):
        return True
    return is_in_download_archive(archive_path, vid)
