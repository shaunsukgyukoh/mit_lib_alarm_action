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
NOTION_CONTACTS_DB_ID = os.getenv("NOTION_CONTACTS_DB_ID", "").strip()

NOTION_VERSION = "2022-06-28"
NOTION_API = "https://api.notion.com/v1"

# ✅ Notion 속성명(여기 DB에 맞게 수정)
PROP_TITLE = "책 제목"           # Title property name (예: "도서명" / "Name" / "이름")
PROP_BORROWER = "대여자"      # People property name
PROP_OVERDUE = "연체(30일초과)"     # Formula(checkbox result)
PROP_NOTIFIED = "반납알림완료" # Checkbox
CONTACT_PROP_PERSON = "노션이름"   # 사람(Person) 속성
CONTACT_PROP_EMAIL = "E-mail"


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
            PROP_NOTIFIED: {"checkbox": True}
        }
    }
    resp = requests.patch(url, headers=notion_headers(), json=payload, timeout=30)
    if resp.status_code >= 400:
        print("Notion error:", resp.status_code, resp.text)  # ★ 원인 메시지 확인
    resp.raise_for_status()
    time.sleep(0.2)


def send_slack(message: str) -> None:
    if not SLACK_WEBHOOK_URL:
        return
    resp = requests.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=30)
    resp.raise_for_status()

def find_email_by_person_id(person_id: str) -> Optional[str]:
    url = f"{NOTION_API}/databases/{NOTION_CONTACTS_DB_ID}/query"
    payload = {
        "filter": {
            "property": CONTACT_PROP_PERSON,
            "people": {"contains": person_id}
        },
        "page_size": 1
    }

    resp = requests.post(url, headers=notion_headers(), json=payload, timeout=30)
    if resp.status_code >= 400:
        print("Contacts DB query error:", resp.status_code, resp.text)  # 디버그
    resp.raise_for_status()

    results = resp.json().get("results", [])
    if not results:
        return None

    props = results[0].get("properties", {})
    email_prop = props.get(CONTACT_PROP_EMAIL, {})

    if email_prop.get("type") == "email":
        return email_prop.get("email")

    if email_prop.get("type") == "rich_text":
        rt = email_prop.get("rich_text", [])
        return "".join([x.get("plain_text", "") for x in rt]).strip() or None

    return None


# def find_email_by_notion_name(notion_name: str) -> Optional[str]:
#     if not NOTION_CONTACTS_DB_ID:
#         raise RuntimeError("NOTION_CONTACTS_DB_ID is missing.")

#     url = f"{NOTION_API}/databases/{NOTION_CONTACTS_DB_ID}/query"
#     payload = {
#         "filter": {
#             "property": CONTACT_PROP_NAME,
#             "rich_text": {"equals": notion_name}
#         },
#         "page_size": 1
#     }

#     resp = requests.post(url, headers=notion_headers(), json=payload, timeout=30)
#     resp.raise_for_status()
#     data = resp.json()
#     results = data.get("results", [])
#     if not results:
#         return None

#     props = results[0].get("properties", {})
#     email_prop = props.get(CONTACT_PROP_EMAIL, {})

#     # Email property
#     if email_prop.get("type") == "email":
#         return email_prop.get("email")

#     # 혹시 Text로 만들었으면 fallback
#     if email_prop.get("type") == "rich_text":
#         rt = email_prop.get("rich_text", [])
#         return "".join([x.get("plain_text", "") for x in rt]).strip() or None

#     return None

def get_borrower_names(page: Dict[str, Any]) -> List[str]:
    props = page.get("properties", {})
    p = props.get(PROP_BORROWER, {})
    people = p.get("people", []) if p.get("type") == "people" else []
    return [x.get("name", "").strip() for x in people if x.get("name")]

def get_borrower_people(page: Dict[str, Any]) -> List[Dict[str, str]]:
    props = page.get("properties", {})
    p = props.get(PROP_BORROWER, {})
    if p.get("type") != "people":
        return []
    return [{"id": x.get("id"), "name": x.get("name")} for x in p.get("people", [])]
    
# def send_email(subject: str, body: str) -> None:
#     if not (SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASS and EMAIL_TO):
#         return
def send_email(to_email: str, subject: str, body: str) -> None:
    if not (SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASS):
        return
    if not to_email:
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
        
    admin_lines = []
    slack_lines = []
    
    for p in pages:
        title = safe_get_title(p)
        page_id = p.get("id")
        page_url = p.get("url", "")
        borrowers = get_borrower_people(p)  # [{"id": "...", "name": "..."}, ...]

        borrower_names_str = ", ".join(
            [b.get("name", "") for b in borrowers if b.get("name")]
        ) or "(대여자 없음)"
        
        # 책 1권의 알림 메시지(개별 발송용)
        book_msg = f"반납 요청 도서: {title}\n링크: {page_url}\n"
    
        # 대여자 각각에게 메일
        for borrower in borrowers:
            person_id = borrower.get("id")
            person_name = borrower.get("name", "")
            if not person_id:
                continue
            email = find_email_by_person_id(person_id)
            # email = find_email_by_person_id(borrower_name)
            if not email:
                print(f"[WARN] No email found for borrower: {person_name}")
                # print(f"[WARN] No email found for borrower: {borrower_name}")
                continue
            send_email(email, f"📚 반납 요청: {title}", book_msg)

        # --- 관리자/슬랙용 전체 목록에 누적 ---
        admin_lines.append(f"- {title} / 대여자: {borrower_names_str} / {page_url}")
        slack_lines.append(f"- {title} / 대여자: {borrower_names_str} / {page_url}")
        
        # 발송 완료 표시
        if page_id:
            mark_notified(page_id)

    # --- 관리자에게 전체 목록 1통 ---
    if EMAIL_TO and admin_lines:
        admin_msg = "📚 반납 요청 대상(대여 30일 초과) 전체 목록\n" + "\n".join(admin_lines)
        send_email(EMAIL_TO, "📚 반납 요청 대상(전체 목록)", admin_msg)

    # --- Slack도 전체 목록 1번만(원하면 유지) ---
    if slack_lines:
        slack_msg = "📚 반납 요청 대상(대여 30일 초과) 전체 목록\n" + "\n".join(slack_lines)
        send_slack(slack_msg)

    print(f"Notified {len(pages)} page(s).")


if __name__ == "__main__":
    main()
