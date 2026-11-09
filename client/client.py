"""Minimal client stub (Day 1): only a health check. Upload/download come later."""
import json
import sys
import urllib.request

BASE = "http://localhost:8000"


def health():
    with urllib.request.urlopen(BASE + "/health", timeout=3) as r:
        return json.load(r)


if __name__ == "__main__":
    print(json.dumps(health(), indent=2))
    sys.exit(0)
