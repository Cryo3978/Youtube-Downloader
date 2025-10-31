import yt_dlp
import os
import json
import shutil
import datetime
import time
import random
from tqdm import tqdm
import subprocess
from CATEGORIES import CATEGORIES


# ----------- Config -----------
SAVE_ROOT = "/mnt/e/wild_gui_video"   # ✅ 改为WSL路径
GLOBAL_ARCHIVE = os.path.join(SAVE_ROOT, "_downloaded_global.txt")
GLOBAL_SEEN_IDS = os.path.join(SAVE_ROOT, "_seen_ids.txt")
GLOBAL_SYNCED = os.path.join(SAVE_ROOT, "_synced.txt")
PROGRESS_FILE = os.path.join(SAVE_ROOT, "_progress.json")
META_PATH = os.path.join(SAVE_ROOT, "metadata.json")
SUCCESS_PATH = os.path.join(SAVE_ROOT, "success.json")
FAIL_PATH = os.path.join(SAVE_ROOT, "fail.json")
PO_TOKEN_PATH = "/mnt/e/po_token.txt"
COOKIE_PATH = "/mnt/e/youtube_cookies.txt"

# SSH target
REMOTE_USER = "shuheng"
REMOTE_HOST = "38.80.122.165"
REMOTE_DIR = "/export/home/wild_gui_video"
SSH_KEY = "/mnt/e/shuhengc"   # ✅ 改为WSL路径

MAX_RESULTS = 1500
RETRY_LIMIT = 2
SYNC_INTERVAL = 10  # 每下载10个视频后执行一次全目录同步
# -------------------------------

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def load_json(path, default):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default

def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def load_seen_ids(path):
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return {ln.strip() for ln in f if ln.strip()}

def append_seen_id(path, vid):
    with open(path, "a", encoding="utf-8") as f:
        f.write(vid + "\n")

def load_po_token():
    if os.path.exists(PO_TOKEN_PATH):
        with open(PO_TOKEN_PATH, "r", encoding="utf-8") as f:
            token = f.read().strip()
            if token:
                print(f"✅ Loaded PO Token from {PO_TOKEN_PATH}")
                return token
    print("⚠️ No PO Token found! Please create /mnt/e/po_token.txt with your mweb.gvs token.")
    return None

def compute_ui_score(title, desc):
    text = (title + " " + desc).lower()
    positive = ["screen recording", "tutorial", "windows", "excel", "file explorer", "vscode", "操作", "教程"]
    negative = ["vlog", "camera", "reaction", "review", "trailer", "podcast"]
    score = sum(p in text for p in positive) - sum(n in text for n in negative)
    return round(max(0.0, min(1.0, 0.5 + 0.1 * score)), 2)

def extract_metadata(entry, keyword, category, status):
    return {
        "id": entry.get("id"),
        "url": entry.get("webpage_url"),
        "title": entry.get("title"),
        "description": entry.get("description"),
        "channel": entry.get("channel") or entry.get("uploader"),
        "upload_date": entry.get("upload_date"),
        "file_path": f"{SAVE_ROOT}/{entry.get('id')} - {entry.get('title', '').replace('/', '_')}.mp4",
        "keyword": keyword,
        "category": category,
        "status": status,
        "collected_at": datetime.datetime.utcnow().isoformat() + "Z"
    }

# ✅ 改用rsync
def rsync_upload(local_path):
    try:
        print(f"📤 Syncing {local_path} → {REMOTE_USER}@{REMOTE_HOST}:{REMOTE_DIR}")
        cmd = [
            "rsync", "-avz", "--progress",
            "-e", f"ssh -i {SSH_KEY} -o StrictHostKeyChecking=no",
            f"{local_path}/",  # ✅ 同步整个文件夹
            f"{REMOTE_USER}@{REMOTE_HOST}:{REMOTE_DIR}/"
        ]
        subprocess.run(cmd, check=True)
        print("✅ Rsync completed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Rsync failed (exit {e.returncode})")
        return False
    except Exception as e:
        print(f"❌ Rsync error: {e}")
        return False

def delete_local_file(file_path):
    """安全删除单个视频文件"""
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"🧹 Deleted local file: {file_path}")
        else:
            print(f"⚠️ File not found when deleting: {file_path}")
    except PermissionError:
        print(f"⚠️ Permission denied deleting {file_path}, retrying...")
        time.sleep(2)
        try:
            os.remove(file_path)
            print(f"🧹 Deleted (after retry): {file_path}")
        except Exception as e:
            print(f"❌ Failed again: {e}")
    except Exception as e:
        print(f"❌ Unexpected delete error: {e}")

def cleanup_leftover_files(max_age_hours=3):
    """
    清理遗留的本地视频文件：
    - 仅删除超过 max_age_hours 小时未同步的 mp4 文件
    - 避免误删正在下载或新生成的文件
    """
    now = time.time()
    removed = 0
    for f in os.listdir(SAVE_ROOT):
        if f.endswith(".mp4"):
            full_path = os.path.join(SAVE_ROOT, f)
            age_hours = (now - os.path.getmtime(full_path)) / 3600
            if age_hours > max_age_hours:
                try:
                    os.remove(full_path)
                    removed += 1
                    print(f"🧹 Cleaned leftover: {f} ({age_hours:.1f}h old)")
                except Exception as e:
                    print(f"⚠️ Could not delete leftover {f}: {e}")
    if removed:
        print(f"✅ Cleaned {removed} stale video(s).")


def download_video(url, vid, po_token):
    """下载单个视频并在成功上传后清理本地文件"""
    ydl_opts = {
        'outtmpl': f'{SAVE_ROOT}/%(id)s - %(title).200s.%(ext)s',
        'format': 'bestvideo+bestaudio/best',
        'merge_output_format': 'mp4',
        'postprocessors': [{'key': 'FFmpegVideoConvertor', 'preferedformat': 'mp4'}],
        'cookiefile': COOKIE_PATH,
        'download_archive': GLOBAL_ARCHIVE,
        'sleep_interval': 2,
        'max_sleep_interval': 5,
        'sleep_interval_requests': 1,
        'retries': 10,
        'ignoreerrors': True,
        'skip_unavailable_fragments': True,
        'extractor_args': {
            'youtube': [
                'player-client=tv_embedded,android'
            ]
        },
        'quiet': False
    }

    fail_count = 0
    for attempt in range(RETRY_LIMIT + 1):
        try:
            print(f"➡️  Attempt {attempt + 1}/{RETRY_LIMIT} for {vid}")
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                result = ydl.extract_info(url, download=True)

                if result:
                    local_file = f"{SAVE_ROOT}/{result.get('id')} - {result.get('title', '').replace('/', '_')}.mp4"
                    print(f"✅ Downloaded: {vid}")

                    if rsync_upload(SAVE_ROOT):  # ✅ 上传整个目录
                        delete_local_file(local_file)
                    return result, "success"

        except Exception as e:
            err = str(e)

            # 🔸 跳过 SABR / nsig extraction 报错
            if "nsig extraction failed" in err or "SABR" in err:
                print(f"⚠️ Skipping SABR/NSIG video: {vid}")
                return None, "failed"

            # 🔸 处理常见报错
            if "403" in err or "po_token" in err.lower():
                print("⚠️ PO Token expired.")
                return None, "failed"
            elif "rate-limited" in err or "HTTP Error 429" in err:
                wait = 90 + random.randint(30, 60)
                print(f"⚠️ Rate-limited. Waiting {wait}s.")
                time.sleep(wait)
            else:
                fail_count += 1
                print(f"❌ Error ({vid}): {err}")
                if fail_count >= 5:
                    print("⏸ Too many failures. Cooling down 120s...")
                    time.sleep(120)
                else:
                    time.sleep(10)
    return None, "failed"


def search_and_download(keyword, category, po_token):
    print(f"\n=== {keyword} ({category}) ===")
    seen_ids = load_seen_ids(GLOBAL_SEEN_IDS)
    synced_ids = load_seen_ids(GLOBAL_SYNCED)
    meta_all = load_json(META_PATH, [])
    success_list = load_json(SUCCESS_PATH, [])
    fail_list = load_json(FAIL_PATH, [])

    BATCH_SIZE = 20        # 每批最多多少条
    TOTAL_TARGET = 1500    # 目标总数
    batch_count = 0
    collected = set()

    while len(collected) < TOTAL_TARGET:
        batch_count += 1
        print(f"\n🔍 Batch {batch_count}: fetching next {BATCH_SIZE} videos for '{keyword}'")
        query = f"ytsearch{BATCH_SIZE}:{keyword}"

        search_opts = {
            'quiet': True,
            'cookiefile': COOKIE_PATH,
            'user_agent': 'Mozilla/5.0',
            'retries': 3,
            'extractor_args': {'youtube': ['player-client=tv_embedded']}
        }

        try:
            with yt_dlp.YoutubeDL(search_opts) as ydl:
                info = ydl.extract_info(query, download=False)
                entries = info.get('entries', [])
        except Exception as e:
            print(f"❌ Search failed in batch {batch_count}: {e}")
            break

        if not entries:
            print("⚠️ No more results from YouTube search. Possibly exhausted.")
            break

        counter = 0
        for e in tqdm(entries, desc=f"Downloading {keyword} (Batch {batch_count})", ncols=80):
            vid, url = e.get('id'), e.get('webpage_url')
            if not url or not vid or vid in seen_ids or vid in collected:
                continue
            if "/shorts/" in url or "playlist" in url or "live" in url:
                continue
            if e.get('duration') and e['duration'] < 30:
                continue

            result, status = download_video(url, vid, po_token)
            if status == "success":
                append_seen_id(GLOBAL_SEEN_IDS, vid)
                meta_all.append(extract_metadata(result, keyword, category, status))
                success_list.append(vid)
            else:
                fail_list.append({"id": vid, "url": url, "keyword": keyword})

            collected.add(vid)
            counter += 1

            if counter % SYNC_INTERVAL == 0:
                rsync_upload(SAVE_ROOT)
                cleanup_leftover_files(max_age_hours=1)

        # 保存进度
        save_json(META_PATH, meta_all)
        save_json(SUCCESS_PATH, success_list)
        save_json(FAIL_PATH, fail_list)
        rsync_upload(SAVE_ROOT)

        # 防止限流
        print("😴 Sleeping 45 seconds before next batch to avoid rate limit...")
        time.sleep(45)

    print(f"✅ Completed keyword '{keyword}' — Total unique collected: {len(collected)}")


def main():
    ensure_dir(SAVE_ROOT)
    po_token = load_po_token()
    if not po_token:
        print("❌ Cannot start without PO Token.")
        return

    progress = load_json(PROGRESS_FILE, {})
    for cat, kws in CATEGORIES.items():
        for kw in kws:
            if progress.get(kw) == "done":
                continue
            search_and_download(kw, cat, po_token)
            progress[kw] = "done"
            save_json(PROGRESS_FILE, progress)

    print("\n✅ All downloads complete (auto rsync + cleanup enabled).")

if __name__ == "__main__":
    print("🚀 Starting Computer-Use Downloader (Local→Remote via rsync)...")
    main()