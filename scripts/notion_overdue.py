import os
import json
import time
import requests
import smtplib
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta, date

NOTION_TOKEN = os.getenv("NOTION_TOKEN", "").strip()
DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "").strip()
NOTION_CONTACTS_DB_ID = os.getenv("NOTION_CONTACTS_DB_ID", "").strip()
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()

SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "0") or "0")
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASS = os.getenv("SMTP_PASS", "").strip()
EMAIL_TO = os.getenv("EMAIL_TO", "").strip()

NOTION_VERSION = "2022-06-28"
NOTION_API = "https://api.notion.com/v1"

# ✅ 도서 DB 속성명
PROP_TITLE = "책 제목"        # Title
PROP_BORROWER = "대여자"      # People
PROP_BORROWED = "대여날짜"    # Date

# ✅ 알림 상태(Checkbox) - Notion 도서 DB에 새로 추가하세요
PROP_ALERT = "반납알림상태"   # Select or Rich text
ALERT_3W = "🟡3주알림완료"
ALERT_4W = "🔴4주알림완료"

# ✅ 연락망 DB 속성명
CONTACT_PROP_PERSON = "노션이름"   # People
CONTACT_PROP_EMAIL = "E-mail"      # Email (또는 Text)

# KST 기준 날짜 계산(서버는 UTC라서 KST로 맞추는 게 안전)
def today_kst() -> date:
    return (datetime.utcnow() + timedelta(hours=9)).date()

def notion_headers() -> Dict[str, str]:
    if not NOTION_TOKEN:
        raise RuntimeError("NOTION_TOKEN is missing.")
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }

def get_alert_status(page: Dict[str, Any]) -> str:
    props = page.get("properties", {})
    p = props.get(PROP_ALERT, {})
    t = p.get("type")

    if t == "select":
        sel = p.get("select")
        return sel.get("name", "") if sel else ""

    if t == "rich_text":
        rt = p.get("rich_text", [])
        return "".join([x.get("plain_text", "") for x in rt]).strip()

    return ""

def set_alert_status(page_id: str, status: str) -> None:
    url = f"{NOTION_API}/pages/{page_id}"
    payload = {
        "properties": {
            PROP_ALERT: {"rich_text": [{"type": "text", "text": {"content": status}}]}
        }
    }
    resp = requests.patch(url, headers=notion_headers(), json=payload, timeout=30)
    if resp.status_code >= 400:
        print("Notion update error:", resp.status_code, resp.text)
    resp.raise_for_status()
    time.sleep(0.2)
    
def safe_get_title(page: Dict[str, Any]) -> str:
    props = page.get("properties", {})
    title_prop = props.get(PROP_TITLE, {})
    title_arr = title_prop.get("title", [])
    if not title_arr:
        for v in props.values():
            if v.get("type") == "title" and v.get("title"):
                title_arr = v["title"]
                break
    if not title_arr:
        return "(제목 없음)"
    return "".join([t.get("plain_text", "") for t in title_arr]).strip() or "(제목 없음)"

def get_borrower_people(page: Dict[str, Any]) -> List[Dict[str, str]]:
    props = page.get("properties", {})
    p = props.get(PROP_BORROWER, {})
    if p.get("type") != "people":
        return []
    return [{"id": x.get("id"), "name": x.get("name")} for x in p.get("people", [])]

def get_borrowed_date(page: Dict[str, Any]) -> Optional[date]:
    props = page.get("properties", {})
    d = props.get(PROP_BORROWED, {})
    if d.get("type") != "date":
        return None
    dv = d.get("date")
    if not dv:
        return None
    start = dv.get("start")
    if not start:
        return None
    # start: "YYYY-MM-DD" or "YYYY-MM-DDTHH:MM:SSZ"
    try:
        if len(start) >= 10:
            return datetime.fromisoformat(start.replace("Z", "+00:00")).date()
    except Exception:
        return None
    return None

def query_candidate_pages() -> List[Dict[str, Any]]:
    """
    후보만 가져오기:
    - 대여자 is_not_empty
    - 대여날짜 is_not_empty
    - (반납알림상태 is_empty OR 반납알림상태 != 🔴4주알림완료)
      -> 4주차(🔴)까지 완료된 건은 더 이상 볼 필요 없으니 제외
    """
    url = f"{NOTION_API}/databases/{DATABASE_ID}/query"
    payload = {
        "filter": {
            "and": [
                {"property": PROP_BORROWER, "people": {"is_not_empty": True}},
                {"property": PROP_BORROWED, "date": {"is_not_empty": True}},
                {
                    "or": [
                        {"property": PROP_ALERT, "rich_text": {"is_empty": True}},
                        {"property": PROP_ALERT, "rich_text": {"does_not_equal": ALERT_4W}},
                    ]
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
        resp = requests.post(url, headers=notion_headers(), json=payload, timeout=30)
        if resp.status_code >= 400:
            print("Notion DB query error:", resp.status_code, resp.text)
        resp.raise_for_status()

        data = resp.json()
        results.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")
        time.sleep(0.2)

    return results

def mark_checkbox(page_id: str, prop_name: str, value: bool = True) -> None:
    url = f"{NOTION_API}/pages/{page_id}"
    payload = {"properties": {prop_name: {"checkbox": value}}}
    resp = requests.patch(url, headers=notion_headers(), json=payload, timeout=30)
    if resp.status_code >= 400:
        print("Notion update error:", resp.status_code, resp.text)
    resp.raise_for_status()
    time.sleep(0.2)

def find_email_by_person_id(person_id: str) -> Optional[str]:
    if not NOTION_CONTACTS_DB_ID:
        raise RuntimeError("NOTION_CONTACTS_DB_ID is missing.")

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
        print("Contacts DB query error:", resp.status_code, resp.text)
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

def send_email(to_email: str, subject: str, body: str) -> None:
    if not (SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASS):
        return
    if not to_email:
        return

    msg = MIMEText(body, _charset="utf-8")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = to_email

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, [to_email], msg.as_string())

def send_slack(message: str) -> None:
    if not SLACK_WEBHOOK_URL:
        return
    resp = requests.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=30)
    resp.raise_for_status()

def main() -> None:
    if not DATABASE_ID:
        raise RuntimeError("NOTION_DATABASE_ID is missing.")

    pages = query_candidate_pages()
    if not pages:
        print("No candidate pages found.")
        return

    today = today_kst()

    admin_lines: List[str] = []
    slack_lines: List[str] = []

    sent_count = 0

    for p in pages:
        title = safe_get_title(p)
        page_id = p.get("id")
        page_url = p.get("url", "")
        borrowers = get_borrower_people(p)
        borrowed = get_borrowed_date(p)

        if not page_id or not borrowed:
            continue

        days = (today - borrowed).days

        # 3주차: 21~27일
        is_week3 = 21 <= days <= 27
        # 4주차: 28일 이상
        is_week4 = days >= 28

        # 현재 체크 상태 읽기
        props = p.get("properties", {})

        current_status = get_alert_status(p)

        # 3주차: 21~27일이면, 아직 🟡/🔴가 아니면 🟡로 만들고 발송
        if is_week3 and current_status not in (ALERT_3W, ALERT_4W):
            stage = "3주차"
            new_status = ALERT_3W
        
        # 4주차: 28일 이상이면, 아직 🔴가 아니면 🔴로 만들고 발송 (🟡면 업그레이드)
        elif is_week4 and current_status != ALERT_4W:
            stage = "4주차"
            new_status = ALERT_4W
        else:
            continue

        borrower_names_str = ", ".join([b.get("name", "") for b in borrowers if b.get("name")]) or "(대여자 없음)"

        subject = f"📚 반납 요청 ({stage}): {title}"
        body = (
            f"[{stage} 반납 요청]\n"
            f"도서: {title}\n"
            f"대여일: {borrowed.isoformat()} (경과 {days}일)\n"
            f"대여자: {borrower_names_str}\n"
            f"링크: {page_url}\n"
        )

        # 대여자 각자에게 발송
        for b in borrowers:
            pid = b.get("id")
            pname = b.get("name", "")
            if not pid:
                continue
            email = find_email_by_person_id(pid)
            if not email:
                print(f"[WARN] No email found for borrower: {pname}")
                continue
            send_email(email, subject, body)

        # 관리자/슬랙용 누적(전체 목록 1통)
        line = f"- ({stage}) {title} / 대여일: {borrowed.isoformat()} / 대여자: {borrower_names_str} / {page_url}"
        admin_lines.append(line)
        slack_lines.append(line)

        set_alert_status(page_id, new_status)
        # # 해당 단계 완료 체크
        # if stage == "3주차":
        #     mark_checkbox(page_id, PROP_NOTIFIED_3W, True)
        # elif stage == "4주차":
        #     mark_checkbox(page_id, PROP_NOTIFIED_4W, True)

        sent_count += 1

    # 관리자에게 전체 목록 1통
    if EMAIL_TO and admin_lines:
        admin_subject = "📚 반납 요청 대상 전체 목록 (3주차/4주차)"
        admin_body = "아래 도서가 대여일 기준 3주차/4주차 반납 요청 대상입니다.\n\n" + "\n".join(admin_lines)
        send_email(EMAIL_TO, admin_subject, admin_body)

    # Slack도 전체 목록 1번
    if slack_lines:
        slack_msg = "📚 반납 요청 대상 전체 목록 (3주차/4주차)\n" + "\n".join(slack_lines)
        send_slack(slack_msg)

    print(f"Sent reminders for {sent_count} page(s).")

if __name__ == "__main__":
    main()
