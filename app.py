import os
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()

PA_FLOW_URL = os.getenv("PA_FLOW_URL", "")  # Power Automate HTTP trigger URL

@app.post("/line/webhook")
async def line_webhook(request: Request):
    body = await request.json()

    # 抽最小欄位（只處理文字訊息）
    events_out = []
    for ev in body.get("events", []) or []:
        msg = (ev.get("message") or {})
        src = (ev.get("source") or {})
        if ev.get("type") == "message" and msg.get("type") == "text":
            events_out.append({
                "reply_token": ev.get("replyToken"),
                "user_id": src.get("userId"),
                "source_type": src.get("type"),
                "timestamp": ev.get("timestamp"),
                "message_id": msg.get("id"),
                "text": msg.get("text"),
            })

    # 轉發到 Power Automate（就算沒有文字事件也回 200）
    if PA_FLOW_URL and events_out:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(PA_FLOW_URL, json={"provider": "line", "events": events_out})

    return JSONResponse({"ok": True, "forwarded": len(events_out)})
