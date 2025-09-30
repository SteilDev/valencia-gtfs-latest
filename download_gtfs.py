#!/usr/bin/env python3
import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

OUTPUT_FILE = "es_mapping.json"
API_KEY = os.getenv('NAP_API_KEY')

if not API_KEY:
    raise SystemExit("Missing NAP_API_KEY environment variable. Set it and retry.")

def download_file(url, filename):
    print(f"Downloading {filename} from {url}")
    headers = {"ApiKey": f"{API_KEY}"}
    r = requests.get(url, stream=True, headers=headers, timeout=30)
    r.raise_for_status()
    with open(filename, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

def main():
    print("Loading local es.json ...")
    with open("es.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    sources = data.get("sources", [])

    http_feeds = [s for s in sources if s.get("type") == "http" and "url" in s]
    http_feeds_sorted = sorted(http_feeds, key=lambda s: f"es_{s['name']}.zip")

    mapping = {}

    for idx, src in enumerate(http_feeds_sorted):
        name = src["name"]
        filename = f"es_{name}.zip"
        prefix = idx
        url = src["url"]

        rt_feeds = [
            s["url"]
            for s in sources
            if s.get("type") == "url" and s.get("spec") == "gtfs-rt" and s.get("name") == name
        ]

        mapping[filename] = {
            "prefix": prefix,
            "download_url": url,
            "gtfs_rt": rt_feeds if rt_feeds else None
        }

        if not os.path.exists(filename):
            download_file(url, filename)
        else:
            print(f"Skipping {filename}, already exists")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=2, ensure_ascii=False)

    print(f"\nDownloaded {len(http_feeds_sorted)} GTFS feeds")
    print(f"Mapping saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
