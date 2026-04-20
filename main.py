import os
import json
import ssl
import certifi

ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())
import sqlite3
import uvicorn
from datetime import datetime, timedelta
from contextlib import contextmanager
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    MessagingApiBlob,
    ReplyMessageRequest,
    PushMessageRequest,
    TextMessage,
)
from linebot.v3.webhooks import (
    MessageEvent,
    TextMessageContent,
    ImageMessageContent,
)
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()

CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
CHANNEL_SECRET = os.environ["LINE_CHANNEL_SECRET"]
DRIVE_FOLDER_ID = os.environ["GOOGLE_DRIVE_FOLDER_ID"]
DRIVE_TEXT_FOLDER_ID = os.environ.get("GOOGLE_DRIVE_TEXT_FOLDER_ID", os.environ["GOOGLE_DRIVE_FOLDER_ID"])
GOOGLE_CREDENTIALS_FILE = os.environ.get("GOOGLE_CREDENTIALS_FILE", "credentials.json")
STAGING_TIMEOUT_SECONDS = 60

app = FastAPI()
handler = WebhookHandler(CHANNEL_SECRET)
configuration = Configuration(access_token=CHANNEL_ACCESS_TOKEN)


# ---------- Database ----------

def init_db():
    with sqlite3.connect("jobs.db") as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS staging (
                user_id     TEXT PRIMARY KEY,
                image_ids   TEXT NOT NULL DEFAULT '[]',
                drive_ids   TEXT NOT NULL DEFAULT '[]',
                expires_at  TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     TEXT NOT NULL,
                text        TEXT,
                image_ids   TEXT NOT NULL DEFAULT '[]',
                drive_ids   TEXT NOT NULL DEFAULT '[]',
                status      TEXT NOT NULL DEFAULT 'pending',
                created_at  TEXT NOT NULL
            )
        """)
        conn.commit()


@contextmanager
def get_db():
    conn = sqlite3.connect("jobs.db")
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


# ---------- Google Drive ----------

def get_drive_service():
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request

    b64 = os.environ.get("GOOGLE_TOKEN_BASE64")
    if b64:
        import base64
        token_data = base64.b64decode(b64).decode()
        creds = Credentials.from_authorized_user_info(
            json.loads(token_data),
            scopes=["https://www.googleapis.com/auth/drive"]
        )
    else:
        token_file = os.environ.get("GOOGLE_TOKEN_FILE", "token.json")
        creds = Credentials.from_authorized_user_file(
            token_file,
            scopes=["https://www.googleapis.com/auth/drive"]
        )

    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    return build("drive", "v3", credentials=creds)


def upload_image_to_drive(message_id: str, data: bytes) -> str:
    service = get_drive_service()
    media = MediaInMemoryUpload(data, mimetype="image/jpeg")
    file_meta = {"name": f"msg_{message_id}.jpg", "parents": [DRIVE_FOLDER_ID]}
    result = service.files().create(body=file_meta, media_body=media, fields="id").execute()
    return result["id"]


def upload_text_to_drive(message_id: str, text: str) -> str:
    service = get_drive_service()
    media = MediaInMemoryUpload(text.encode("utf-8"), mimetype="text/plain")
    file_meta = {"name": f"msg_{message_id}.txt", "parents": [DRIVE_TEXT_FOLDER_ID]}
    result = service.files().create(body=file_meta, media_body=media, fields="id").execute()
    return result["id"]


def download_from_drive(drive_file_id: str) -> bytes:
    service = get_drive_service()
    return service.files().get_media(fileId=drive_file_id).execute()


# ---------- LINE helpers ----------

def push_message(user_id: str, text: str):
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).push_message(
            PushMessageRequest(to=user_id, messages=[TextMessage(text=text)])
        )


# ---------- Staging helpers ----------

def flush_staging(user_id: str, text: str | None = None, text_message_id: str | None = None) -> int | None:
    """สร้าง job จาก staging ของ user แล้วเคลียร์ staging"""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM staging WHERE user_id=?", (user_id,)).fetchone()
        if not row:
            return None

        image_ids = json.loads(row["image_ids"])
        drive_ids = json.loads(row["drive_ids"])

        now = (datetime.utcnow() + timedelta(hours=7)).isoformat()
        cursor = conn.execute(
            "INSERT INTO jobs (user_id, text, image_ids, drive_ids, status, created_at) VALUES (?,?,?,?,?,?)",
            (user_id, text, json.dumps(image_ids), json.dumps(drive_ids), "pending", now),
        )
        job_id = cursor.lastrowid
        conn.execute("DELETE FROM staging WHERE user_id=?", (user_id,))

    if text and text_message_id:
        upload_text_to_drive(text_message_id, text)

    return job_id


def flush_expired_staging():
    """Background job: flush staging ที่หมดเวลา 1 นาที"""
    now = datetime.utcnow().isoformat()
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM staging WHERE expires_at <= ?", (now,)
        ).fetchall()

    for row in rows:
        user_id = row["user_id"]
        job_id = flush_staging(user_id, text=None)
        if job_id:
            print(f"[TIMEOUT] user={user_id} → job #{job_id}")
            push_message(user_id, f"⏱ หมดเวลารอข้อความ สร้างงานจากรูปที่ส่งมาแล้ว (job #{job_id})")


# ---------- Startup ----------

init_db()
scheduler = BackgroundScheduler()
scheduler.add_job(flush_expired_staging, "interval", seconds=15)
scheduler.start()


# ---------- Health ----------

@app.get("/")
def health_check():
    return {"status": "ok"}


# ---------- LINE Webhook ----------

@app.post("/webhook")
async def webhook(request: Request):
    signature = request.headers.get("X-Line-Signature", "")
    body = await request.body()
    try:
        handler.handle(body.decode(), signature)
    except InvalidSignatureError:
        raise HTTPException(status_code=400, detail="Invalid signature")
    return "OK"


@handler.add(MessageEvent, message=ImageMessageContent)
def handle_image(event: MessageEvent):
    user_id = event.source.user_id
    message_id = event.message.id

    # Reply ก่อนทันที (ก่อน token หมดอายุ)
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[TextMessage(text="📥 รับรูปแล้ว กำลังอัปโหลด...")],
            )
        )

    # ดึงรูปจาก LINE หลัง reply แล้ว
    with ApiClient(configuration) as api_client:
        image_bytes = MessagingApiBlob(api_client).get_message_content(message_id)

    # อัปโหลดขึ้น Drive
    drive_id = upload_image_to_drive(message_id, image_bytes)
    expires_at = (datetime.utcnow() + timedelta(seconds=STAGING_TIMEOUT_SECONDS)).isoformat()

    # เพิ่มเข้า staging
    with get_db() as conn:
        row = conn.execute("SELECT * FROM staging WHERE user_id=?", (user_id,)).fetchone()
        if row:
            image_ids = json.loads(row["image_ids"]) + [message_id]
            drive_ids = json.loads(row["drive_ids"]) + [drive_id]
            conn.execute(
                "UPDATE staging SET image_ids=?, drive_ids=?, expires_at=? WHERE user_id=?",
                (json.dumps(image_ids), json.dumps(drive_ids), expires_at, user_id),
            )
        else:
            conn.execute(
                "INSERT INTO staging (user_id, image_ids, drive_ids, expires_at) VALUES (?,?,?,?)",
                (user_id, json.dumps([message_id]), json.dumps([drive_id]), expires_at),
            )

    push_message(user_id, f"✅ อัปโหลดรูปสำเร็จ ({message_id[:8]}...)\nส่งข้อความมาได้เลย หรือรอ 1 นาทีระบบจะเริ่มทำงานอัตโนมัติ")


@handler.add(MessageEvent, message=TextMessageContent)
def handle_text(event: MessageEvent):
    user_id = event.source.user_id
    text = event.message.text

    # Reply ทันที
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[TextMessage(text="📝 รับข้อความแล้ว กำลังสร้างงาน...")],
            )
        )

    # flush staging → สร้าง job
    job_id = flush_staging(user_id, text=text, text_message_id=event.message.id)

    if job_id:
        push_message(user_id, f"🚀 สร้างงาน #{job_id} เรียบร้อย\nกำลังรอ Claude ตรวจสอบ...")
    else:
        push_message(user_id, "⚠️ ยังไม่มีรูปที่ส่งมา กรุณาส่งรูปก่อนแล้วตามด้วยข้อความ")


# ---------- Endpoints สำหรับ Cowork ----------

@app.get("/jobs/next")
def jobs_next():
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM jobs WHERE status='pending' ORDER BY created_at ASC LIMIT 1"
        ).fetchone()
        if not row:
            return {"job": None}

        conn.execute("UPDATE jobs SET status='processing' WHERE id=?", (row["id"],))

    image_ids = json.loads(row["image_ids"])
    return {
        "job": {
            "id": row["id"],
            "user_id": row["user_id"],
            "text": row["text"],
            "image_count": len(image_ids),
            "images": [f"/jobs/{row['id']}/image/{i}" for i in range(len(image_ids))],
            "created_at": row["created_at"],
        }
    }


@app.get("/jobs/{job_id}/image/{index}")
def get_job_image(job_id: int, index: int):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Job not found")

    drive_ids = json.loads(row["drive_ids"])
    if index >= len(drive_ids):
        raise HTTPException(status_code=404, detail="Image index out of range")

    image_bytes = download_from_drive(drive_ids[index])
    return Response(content=image_bytes, media_type="image/jpeg")


class DonePayload(BaseModel):
    result: str


@app.post("/jobs/{job_id}/done")
def job_done(job_id: int, payload: DonePayload):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        conn.execute("UPDATE jobs SET status='done' WHERE id=?", (job_id,))

    push_message(row["user_id"], f"🤖 ผลการตรวจสอบ:\n\n{payload.result}")
    return {"status": "done"}


class NotifyPayload(BaseModel):
    job_id: int
    note: str = ""


@app.post("/notify/processing")
def notify_processing(payload: NotifyPayload):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (payload.job_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Job not found")

    text = f"🔍 Claude กำลังตรวจสอบงาน #{payload.job_id}..."
    if payload.note:
        text += f"\n📝 {payload.note}"
    push_message(row["user_id"], text)
    return {"status": "notified"}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
