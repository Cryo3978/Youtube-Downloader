import os
from pathlib import Path
from typing import Optional, Union

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parent
load_dotenv(_ROOT / ".env")


def _get(key: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(key)
    if value is None or not value.strip():
        return default
    return value.strip()


def _int(key: str, default: int) -> int:
    value = _get(key)
    return int(value) if value is not None else default


def _bool(key: str, default: bool) -> bool:
    value = _get(key)
    if value is None:
        return default
    return value.lower() in ("1", "true", "yes", "on")


def _path(key: str, default: Union[str, Path]) -> Path:
    raw = _get(key)
    path = Path(raw if raw is not None else default).expanduser()
    if not path.is_absolute():
        path = _ROOT / path
    return path.resolve()


# Paths & storage
SAVE_ROOT = _path("SAVE_ROOT", "./data")
COOKIE_PATH = _path("COOKIE_PATH", "./youtube_cookies.txt")
META_PATH = _path("META_PATH", SAVE_ROOT / "metadata.json")
GLOBAL_ARCHIVE = _path("GLOBAL_ARCHIVE", SAVE_ROOT / "_downloaded_global.txt")

# Metadata collection
MAX_RESULTS = _int("MAX_RESULTS", 3000)
COLLECT_SLEEP_SECONDS = _int("COLLECT_SLEEP_SECONDS", 5)
YTDLP_COLLECT_PLAYER_CLIENT = _get("YTDLP_COLLECT_PLAYER_CLIENT", "tv_embedded")

# Video download
RETRY_LIMIT = _int("RETRY_LIMIT", 2)
DOWNLOAD_SLEEP_INTERVAL = _int("DOWNLOAD_SLEEP_INTERVAL", 3)
DOWNLOAD_MAX_SLEEP_INTERVAL = _int("DOWNLOAD_MAX_SLEEP_INTERVAL", 6)
RATE_LIMIT_SLEEP_SECONDS = _int("RATE_LIMIT_SLEEP_SECONDS", 120)
YTDLP_DOWNLOAD_PLAYER_CLIENT = _get("YTDLP_DOWNLOAD_PLAYER_CLIENT", "tv_embedded,android")
DOWNLOAD_FORMAT = _get("DOWNLOAD_FORMAT", "bestvideo+bestaudio/best")
MERGE_OUTPUT_FORMAT = _get("MERGE_OUTPUT_FORMAT", "mp4")

# Remote sync
SSH_KEY = _get("SSH_KEY")
REMOTE_USER = _get("REMOTE_USER")
REMOTE_HOST = _get("REMOTE_HOST")
REMOTE_DIR = _get("REMOTE_DIR")
SYNC_INTERVAL = _int("SYNC_INTERVAL", 10)
SYNC_POST_SLEEP_SECONDS = _int("SYNC_POST_SLEEP_SECONDS", 30)
SSH_OPTIONS = _get("SSH_OPTIONS", "-o StrictHostKeyChecking=accept-new")
DELETE_LOCAL_AFTER_RSYNC = _bool("DELETE_LOCAL_AFTER_RSYNC", True)


def get_po_token() -> Optional[str]:
    token = _get("PO_TOKEN")
    if not token or token.upper().startswith("YOUR "):
        return None
    return token


def youtube_extractor_args(player_clients: str, po_token: Optional[str] = None) -> dict:
    args = [f"player-client={player_clients}"]
    if po_token:
        args.append(f"po_token=android.gvs+{po_token}")
    return {"youtube": args}


def rsync_enabled() -> bool:
    return all([SSH_KEY, REMOTE_USER, REMOTE_HOST, REMOTE_DIR])


def validate_config(*, require_cookies: bool = True) -> None:
    SAVE_ROOT.mkdir(parents=True, exist_ok=True)
    if require_cookies and not COOKIE_PATH.is_file():
        raise FileNotFoundError(
            f"Cookie file not found: {COOKIE_PATH}. "
            "Set COOKIE_PATH in .env or copy .env.example to .env."
        )
