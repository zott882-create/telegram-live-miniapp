#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import sys
import urllib.parse
import urllib.request


def main() -> int:
    token = os.environ.get("BOT_TOKEN", "").strip()
    webapp_url = os.environ.get("WEBAPP_URL", "").strip()

    if not token:
        print("BOT_TOKEN is empty")
        return 1
    if not webapp_url.startswith("https://"):
        print("WEBAPP_URL must start with https://")
        return 1

    api_url = f"https://api.telegram.org/bot{token}/setChatMenuButton"
    payload = {
        "menu_button": {
            "type": "web_app",
            "text": "Live матчи",
            "web_app": {"url": webapp_url},
        }
    }
    data = urllib.parse.urlencode({"menu_button": json.dumps(payload["menu_button"], ensure_ascii=False)}).encode("utf-8")

    req = urllib.request.Request(api_url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=20) as resp:
        text = resp.read().decode("utf-8", "replace")
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
