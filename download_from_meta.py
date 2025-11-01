# download_from_meta.py
import yt_dlp
import os, json, time, random, subprocess

SAVE_ROOT = 
META_PATH = os.path.join(SAVE_ROOT, "metadata.json")
COOKIE_PATH = 
GLOBAL_ARCHIVE = os.path.join(SAVE_ROOT, "_downloaded_global.txt")
SSH_KEY = 
REMOTE_USER = 
REMOTE_HOST = 
REMOTE_DIR = 
SYNC_INTERVAL = 10
RETRY_LIMIT = 2

def delete_local_files():
    for f in os.listdir(SAVE_ROOT):
        if f.endswith(".mp4"):
            full = os.path.join(SAVE_ROOT, f)
            try:
                os.remove(full)
                print(f"🧹 Deleted local file: {f}")
            except Exception as e:
                print(f"⚠️ Failed to delete {f}: {e}")


def rsync_upload():
    cmd = [
        "rsync", "-avz", "--progress",
        "-e", f"ssh -i {SSH_KEY} -o StrictHostKeyChecking=no",
        f"{SAVE_ROOT}/", f"{REMOTE_USER}@{REMOTE_HOST}:{REMOTE_DIR}/"
    ]
    result = subprocess.run(cmd, check=False)
    if result.returncode == 0:
        print("✅ Rsync success — cleaning up local files...")
        delete_local_files()
    else:
        print("⚠️ Rsync failed, skipped cleanup.")


def download_one(url, vid):
    ydl_opts = {
        'outtmpl': f'{SAVE_ROOT}/%(id)s - %(title).200s.%(ext)s',
        'format': 'bestvideo+bestaudio/best',
        'merge_output_format': 'mp4',
        'cookiefile': COOKIE_PATH,
        'download_archive': GLOBAL_ARCHIVE,
        'sleep_interval': 3,
        'max_sleep_interval': 6,
        'extractor_args': {'youtube': ['player-client=tv_embedded,android']},
        'ignoreerrors': True
    }
    for attempt in range(RETRY_LIMIT):
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
                return True
        except Exception as e:
            if "rate-limited" in str(e):
                print("⚠️ Rate limited, sleeping 2 min")
                time.sleep(120)
            else:
                print(f"⚠️ Retry {attempt+1} for {vid}")
                time.sleep(10)
    return False

def main():
    with open(META_PATH, "r", encoding="utf-8") as f:
        metas = json.load(f)
    done = 0
    for m in metas:
        if not m.get("url"): continue
        ok = download_one(m["url"], m["id"])
        if ok: done += 1
        if done % SYNC_INTERVAL == 0:
            rsync_upload()
            print("✅ Synced batch, sleeping 30 s")
            time.sleep(30)

if __name__ == "__main__":
    main()
