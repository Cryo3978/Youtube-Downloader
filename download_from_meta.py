# download_from_meta.py
import json
import os
import subprocess
import time

import yt_dlp

import config
from utils import download_succeeded


def delete_local_files():
    for name in os.listdir(config.SAVE_ROOT):
        if not name.endswith(".mp4"):
            continue
        path = config.SAVE_ROOT / name
        try:
            path.unlink()
            print(f"🧹 Deleted local file: {name}")
        except OSError as e:
            print(f"⚠️ Failed to delete {name}: {e}")


def rsync_upload():
    if not config.rsync_enabled():
        return

    cmd = [
        "rsync", "-avz", "--progress",
        "-e", f"ssh -i {config.SSH_KEY} {config.SSH_OPTIONS}",
        f"{config.SAVE_ROOT}/",
        f"{config.REMOTE_USER}@{config.REMOTE_HOST}:{config.REMOTE_DIR}/",
    ]
    result = subprocess.run(cmd, check=False)
    if result.returncode == 0:
        print("✅ Rsync success")
        if config.DELETE_LOCAL_AFTER_RSYNC:
            delete_local_files()
    else:
        print("⚠️ Rsync failed, skipped cleanup.")


def download_one(url, vid):
    po_token = config.get_po_token()
    ydl_opts = {
        "outtmpl": f"{config.SAVE_ROOT}/%(id)s - %(title).200s.%(ext)s",
        "format": config.DOWNLOAD_FORMAT,
        "merge_output_format": config.MERGE_OUTPUT_FORMAT,
        "cookiefile": str(config.COOKIE_PATH),
        "download_archive": str(config.GLOBAL_ARCHIVE),
        "sleep_interval": config.DOWNLOAD_SLEEP_INTERVAL,
        "max_sleep_interval": config.DOWNLOAD_MAX_SLEEP_INTERVAL,
        "extractor_args": config.youtube_extractor_args(
            config.YTDLP_DOWNLOAD_PLAYER_CLIENT, po_token
        ),
    }
    for attempt in range(config.RETRY_LIMIT):
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
            if download_succeeded(
                config.SAVE_ROOT,
                config.GLOBAL_ARCHIVE,
                vid,
                config.MERGE_OUTPUT_FORMAT or "mp4",
            ):
                return True
            print(f"⚠️ No output file for {vid}, retry {attempt + 1}")
            time.sleep(10)
        except Exception as e:
            if "rate-limited" in str(e):
                print(f"⚠️ Rate limited, sleeping {config.RATE_LIMIT_SLEEP_SECONDS}s")
                time.sleep(config.RATE_LIMIT_SLEEP_SECONDS)
            else:
                print(f"⚠️ Retry {attempt + 1} for {vid}: {e}")
                time.sleep(10)
    return False


def main():
    config.validate_config()
    if not config.META_PATH.is_file():
        raise FileNotFoundError(
            f"Metadata not found: {config.META_PATH}. Run collect_meta.py first."
        )

    with open(config.META_PATH, encoding="utf-8") as f:
        metas = json.load(f)

    done = 0
    for m in metas:
        if not m.get("url"):
            continue
        if download_one(m["url"], m["id"]):
            done += 1
        if done and done % config.SYNC_INTERVAL == 0:
            rsync_upload()
            print(f"✅ Synced batch, sleeping {config.SYNC_POST_SLEEP_SECONDS}s")
            time.sleep(config.SYNC_POST_SLEEP_SECONDS)

    if done and config.rsync_enabled():
        rsync_upload()


if __name__ == "__main__":
    main()
