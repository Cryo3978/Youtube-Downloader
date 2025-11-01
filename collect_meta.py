# collect_meta.py
import yt_dlp
import os, json, datetime, time
from tqdm import tqdm
from CATEGORIES import CATEGORIES

SAVE_ROOT = 
META_PATH = os.path.join(SAVE_ROOT, "metadata.json")
COOKIE_PATH = 
MAX_RESULTS = 3000

def ensure_dir(p): os.makedirs(p, exist_ok=True)
def load_json(p, d): return json.load(open(p)) if os.path.exists(p) else d
def save_json(p, d): json.dump(d, open(p, "w", encoding="utf-8"), indent=2, ensure_ascii=False)

def collect(keyword, category):
    print(f"\n🔍 Collecting meta for {keyword}")
    query = f"ytsearch{MAX_RESULTS}:{keyword}"
    ydl_opts = {
        'quiet': True,
        'cookiefile': COOKIE_PATH,
        'extract_flat': True,
        'forcejson': False,
        'simulate': True,
        'extractor_args': {'youtube': ['player-client=tv_embedded']}
    }
    results = []
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(query, download=False)
        for e in tqdm(info.get('entries', []), desc=keyword, ncols=80):
            if not e or "/shorts/" in e['url'] or "live" in e['url']:
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
                "collected_at": datetime.datetime.utcnow().isoformat()+"Z"
            })
    meta_all = load_json(META_PATH, [])
    meta_all.extend(results)
    save_json(META_PATH, meta_all)
    print(f"✅ Collected {len(results)} entries for {keyword}")

def main():
    ensure_dir(SAVE_ROOT)
    for cat, kws in CATEGORIES.items():
        for kw in kws:
            collect(kw, cat)
            time.sleep(5)  # avoid Youtube Limit

if __name__ == "__main__":
    main()
