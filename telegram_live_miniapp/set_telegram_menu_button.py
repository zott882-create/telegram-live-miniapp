import os, requests
token = os.environ.get("BOT_TOKEN")
url = os.environ.get("WEBAPP_URL")
if not token or not url:
    raise SystemExit("Set BOT_TOKEN and WEBAPP_URL")
r = requests.post(f"https://api.telegram.org/bot{token}/setChatMenuButton", json={
    "menu_button": {"type": "web_app", "text": "Live матчи", "web_app": {"url": url}}
})
print(r.status_code, r.text)
