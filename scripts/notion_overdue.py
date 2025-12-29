import os
import json
import time
import requests
import smtplib
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional

NOTION_TOKEN = os.getenv("NOTION_TOKEN", "").strip()
DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "").strip()
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()

SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "0") or "0")
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASS = os.getenv("SMTP_PASS", "").strip()
EMAIL_TO = os.getenv("EMAIL_TO", "").strip()

NOTION_VERSION = "2022-06-28"
NOTION_API = "https://api.notion.com/v1"

# ✅ Notion 속성명(여기 DB에 맞게 수정)
PROP_TITLE = "책 제목"           # Title property name (예: "도서명" / "Name" / "이름")
PROP_BORROWER = "대여자"      # People property name
PROP_OVERDUE = "연체 (30일초과)"     # Formula(checkbox result)
PROP_NOTIFIED = "반납알림완료" # Checkbox


def notion_headers() -> Dict[str, str]:
    if not NOTION_TOKEN:
        raise RuntimeError("NOTION_TOKEN is missing.")
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def safe_get_title(page: Dict[str, Any]) -> str:
    props = page.get("properties", {})
    title_prop = props.get(PROP_TITLE, {})
    title_arr = title_prop.get("title", [])
    if not title_arr:
        # fallback: try any title property
        for v in props.values():
            if v.get("type") == "title" and v.get("title"):
                title_arr = v["title"]
                break
    if not title_arr:
        return "(제목 없음)"
    return "".join([t.get("plain_text", "") for t in title_arr]).strip() or "(제목 없음)"


def safe_get_borrowers(page: Dict[str, Any]) -> str:
    props = page.get("properties", {})
    p = props.get(PROP_BORROWER, {})
    people = p.get("people", []) if p.get("type") == "people" else []
    names = [x.get("name", "").strip() for x in people if x.get("name")]
    return ", ".join(names) if names else "(대여자 정보 없음)"


def query_overdue_pages() -> List[Dict[str, Any]]:
    """Filter: 연체 (30일초과) == true AND 반납알림완료 == false"""
    url = f"{NOTION_API}/databases/{DATABASE_ID}/query"
    payload = {
        "filter": {
            "and": [
                {
                    "property": PROP_OVERDUE,
                    "checkbox": {"equals": True}
                },
                {
                    "property": PROP_NOTIFIED,
                    "checkbox": {"equals": False}
                }
            ]
        },
        "page_size": 100
    }


    results: List[Dict[str, Any]] = []
    has_more = True
    start_cursor: Optional[str] = None

    while has_more:
        if start_cursor:
            payload["start_cursor"] = start_cursor
        resp = requests.post(url, headers=notion_headers(), data=json.dumps(payload), timeout=30)
        resp.raise_for_status()
        data = resp.json()
        results.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")
        # rate limit 완화
        time.sleep(0.2)

    return results


def mark_notified(page_id: str) -> None:
    url = f"{NOTION_API}/pages/{page_id}"
    payload = {
        "properties": {
            PROP_NOTIFIED: {
                "checkbox": True
            }
        }
    }
    resp = requests.patch(url, headers=notion_headers(), data=json.dumps(payload), timeout=30)
    resp.raise_for_status()
    time.sleep(0.2)


def send_slack(message: str) -> None:
    if not SLACK_WEBHOOK_URL:
        return
    resp = requests.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=30)
    resp.raise_for_status()


def send_email(subject: str, body: str) -> None:
    if not (SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASS and EMAIL_TO):
        return

    msg = MIMEText(body, _charset="utf-8")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = EMAIL_TO

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, [EMAIL_TO], msg.as_string())


def main() -> None:
    if not DATABASE_ID:
        raise RuntimeError("NOTION_DATABASE_ID is missing.")

    pages = query_overdue_pages()

    if not pages:
        print("No overdue pages found.")
        return

    lines = []
    for p in pages:
        title = safe_get_title(p)
        borrowers = safe_get_borrowers(p)
        page_id = p.get("id")
        url = p.get("url", "")
        lines.append(f"- {title} / 대여자: {borrowers} / {url}")

        if page_id:
            mark_notified(page_id)

    message = "📚 반납 요청 대상(대여 30일 초과)\n" + "\n".join(lines)

    # Slack + Email(둘 다 설정돼 있으면 둘 다 감)
    send_slack(message)
    send_email("📚 반납 요청 대상(대여 30일 초과)", message)

    print(f"Notified {len(pages)} page(s).")


if __name__ == "__main__":
    main()
