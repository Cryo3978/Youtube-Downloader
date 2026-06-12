# collect_meta.py
import json
import datetime
import time

import yt_dlp
from tqdm import tqdm

from CATEGORIES import CATEGORIES
import config

def load_json(path, default):
    if not path.exists():
        return default
    with open(path, encoding="utf-8") as f:
        return json.load(f)

def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def collect(keyword, category):
    print(f"\n🔍 Collecting meta for {keyword}")
    query = f"ytsearch{config.MAX_RESULTS}:{keyword}"
    ydl_opts = {
        "quiet": True,
        "cookiefile": str(config.COOKIE_PATH),
        "extract_flat": True,
        "forcejson": False,
        "simulate": True,
        "extractor_args": config.youtube_extractor_args(config.YTDLP_COLLECT_PLAYER_CLIENT),
    }
    results = []
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(query, download=False)
        for e in tqdm(info.get("entries", []), desc=keyword, ncols=80):
            if not e or "/shorts/" in e["url"] or "live" in e["url"]:
                continue
            desc = e.get("description", "") or ""
            results.append({
                "id": e.get("id"),
                "url": e.get("url"),
                "title": e.get("title"),
                "description": desc[:200],
                "duration": e.get("duration"),
                "view_count": e.get("view_count"),
                "like_count": e.get("like_count"),
                "channel": e.get("channel"),
                "channel_id": e.get("channel_id"),
                "upload_date": e.get("upload_date"),
                "uploader": e.get("uploader"),
                "keyword": keyword,
                "category": category,
                "collected_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            })
    meta_all = load_json(config.META_PATH, [])
    meta_all.extend(results)
    save_json(config.META_PATH, meta_all)
    print(f"✅ Collected {len(results)} entries for {keyword}")

def main():
    config.validate_config()
    for cat, kws in CATEGORIES.items():
        for kw in kws:
            collect(kw, cat)
            time.sleep(config.COLLECT_SLEEP_SECONDS)

if __name__ == "__main__":
    main()
