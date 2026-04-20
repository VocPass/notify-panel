import asyncio
import json
import os
import uuid
import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from pydantic import BaseModel
from typing import List
from dotenv import load_dotenv

from send_notification import send_notification, make_jwt_token

load_dotenv()

app = FastAPI()

ADMIN_EMAIL = os.environ["DB_EMAIL"]
ADMIN_PASSWORD = os.environ["DB_PASSWD"]
BASE_URL = os.environ["DB_URL"].rstrip("/")
COLLECTION_ID = os.environ["COLLECTION_ID"]

token = ""
jobs: dict[str, asyncio.Queue] = {}


async def get_admin_token(client: httpx.AsyncClient) -> str:
    global token
    r = await client.post(
        f"{BASE_URL}/api/collections/_superusers/auth-with-password",
        json={"identity": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
    )
    r.raise_for_status()
    token = r.json()["token"]
    return token


@app.get("/", response_class=HTMLResponse)
async def index():
    with open("templates/index.html", encoding="utf-8") as f:
        return f.read()


@app.get("/api/users")
async def get_users():
    async with httpx.AsyncClient() as client:
        tok = await get_admin_token(client)
        all_items = []
        page = 1
        while True:
            r = await client.get(
                f"{BASE_URL}/api/collections/{COLLECTION_ID}/records",
                headers={"Authorization": tok},
                params={"perPage": 500, "page": page, "expand": "user"},
            )
            r.raise_for_status()
            data = r.json()
            all_items.extend(data.get("items", []))
            if page >= data.get("totalPages", 1):
                break
            page += 1
        return {"items": all_items, "totalItems": len(all_items)}


class SendRequest(BaseModel):
    user_ids: List[str]
    title: str
    body: str


@app.post("/api/send")
async def send(req: SendRequest):
    if not req.user_ids:
        return JSONResponse({"error": "請至少選擇一位使用者"}, status_code=400)
    if not req.title.strip():
        return JSONResponse({"error": "請輸入標題"}, status_code=400)

    job_id = str(uuid.uuid4())
    queue: asyncio.Queue = asyncio.Queue()
    jobs[job_id] = queue

    asyncio.create_task(do_send(job_id, req))
    return {"job_id": job_id}


BATCH_SIZE = 100  # PocketBase filter chunk size
CONCURRENCY = 500  # max parallel APNs sends


async def fetch_users_batch(
    client: httpx.AsyncClient, tok: str, user_ids: List[str]
) -> dict:
    """Fetch all users in bulk using OR filters instead of N individual requests."""
    users_by_id: dict = {}
    for offset in range(0, len(user_ids), BATCH_SIZE):
        chunk = user_ids[offset : offset + BATCH_SIZE]
        filter_expr = "||".join(f'id="{uid}"' for uid in chunk)
        r = await client.get(
            f"{BASE_URL}/api/collections/{COLLECTION_ID}/records",
            headers={"Authorization": tok},
            params={"filter": f"({filter_expr})", "perPage": BATCH_SIZE},
        )
        r.raise_for_status()
        for item in r.json().get("items", []):
            users_by_id[item["id"]] = item
    return users_by_id


async def do_send(job_id: str, req: SendRequest):
    queue = jobs[job_id]
    total = len(req.user_ids)

    async with httpx.AsyncClient() as db_client:
        try:
            tok = await get_admin_token(db_client)
        except Exception as e:
            await queue.put(
                {"type": "error", "name": "認證", "index": 0, "total": total, "error": str(e)}
            )
            await queue.put({"type": "done", "total": total, "success": 0, "failed": total})
            return

        try:
            users_by_id = await fetch_users_batch(db_client, tok, req.user_ids)
        except Exception as e:
            await queue.put(
                {"type": "error", "name": "批量查詢", "index": 0, "total": total, "error": str(e)}
            )
            await queue.put({"type": "done", "total": total, "success": 0, "failed": total})
            return

    jwt_token = make_jwt_token()
    semaphore = asyncio.Semaphore(CONCURRENCY)
    results: list[bool] = []

    async with httpx.AsyncClient(http2=True) as apns_client:

        async def send_one(i: int, user_id: str) -> bool:
            user = users_by_id.get(user_id, {})
            name = user.get("name") or user.get("username") or user.get("email") or user_id
            apns_token = user.get("apns_token") or user.get("device_token") or ""

            async with semaphore:
                await queue.put({"type": "sending", "name": name, "index": i + 1, "total": total})
                try:
                    await send_notification(req.title, req.body, apns_token, jwt_token, apns_client)
                    await queue.put({"type": "success", "name": name, "index": i + 1, "total": total})
                    return True
                except Exception as e:
                    await queue.put(
                        {"type": "error", "name": name, "index": i + 1, "total": total, "error": str(e)}
                    )
                    return False

        results = list(
            await asyncio.gather(*[send_one(i, uid) for i, uid in enumerate(req.user_ids)])
        )

    success_count = sum(results)
    failed_count = total - success_count
    await queue.put({"type": "done", "total": total, "success": success_count, "failed": failed_count})


@app.get("/api/progress/{job_id}")
async def progress(job_id: str):
    queue = jobs.get(job_id)
    if not queue:
        return JSONResponse({"error": "找不到此工作"}, status_code=404)

    async def event_stream():
        try:
            while True:
                event = await asyncio.wait_for(queue.get(), timeout=60)
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                if event["type"] == "done":
                    break
        except asyncio.TimeoutError:
            yield f"data: {json.dumps({'type': 'timeout'})}\n\n"
        finally:
            jobs.pop(job_id, None)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


if __name__ == "__main__":
    port = int(os.environ.get("port", 8899))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=True)
