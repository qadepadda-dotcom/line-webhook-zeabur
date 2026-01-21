import os
import json
import requests
from fastapi import FastAPI, Request

app = FastAPI()

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# ---- 基本檢查 ----
@app.get("/health")
def health():
    return {"ok": True}


# ---- OpenAI Chat Completions ----
def call_openai_chat(user_text: str) -> str:
    """
    Calls OpenAI Chat Completions API and returns assistant message.
    Docs: https://platform.openai.com/docs/api-reference/chat
    """
    if not OPENAI_API_KEY:
        return "尚未設定 OPENAI_API_KEY，因此目前無法呼叫 GPT。"

    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    system_prompt = (
        "你是企業品保/BI 助理。回答要用繁體中文、簡潔、可執行。"
        "如果使用者問的是公司內部 ERP/Fabric 數據，但你沒有拿到查詢結果，"
        "請明確說「目前尚未連接資料源，因此無法提供實際數據」，不要猜數字。"
    )

    payload = {
        "model": OPENAI_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_text},
        ],
        "temperature": 0.2,
        "max_tokens": 400,
    }

    r = requests.post(url, headers=headers, json=payload, timeout=40)
    r.raise_for_status()
    data = r.json()
    return data["choices"][0]["message"]["content"]


# ---- LINE Reply API ----
def line_reply(reply_token: str, text: str) -> None:
    if not LINE_CHANNEL_ACCESS_TOKEN:
        # 沒 token 就只能吞掉（避免 webhook 500）
        return

    url = "https://api.line.me/v2/bot/message/reply"
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "replyToken": reply_token,
        "messages": [{"type": "text", "text": text[:5000]}],  # LINE 文字長度限制保守處理
    }
    requests.post(url, headers=headers, json=payload, timeout=15)


def looks_like_data_question(text: str) -> bool:
    """
    粗略判斷：像是在問 ERP/Fabric 查詢的問題（之後會改成更完整的意圖分類）
    """
    keywords = [
        "近", "最近", "30天", "7天", "本月", "上月",
        "NG", "不良", "IQC", "檢驗", "廠別", "昆山", "增達",
        "Top", "排名", "最多", "趨勢", "比例", "良率", "不良率",
        "cqcr310", "lakehouse", "fabric", "sql"
    ]
    t = (text or "").lower()
    return any(k.lower() in t for k in keywords)


# ---- LINE Webhook ----
@app.post("/line/webhook")
async def line_webhook(req: Request):
    """
    Expected body (from your Zeabur/PA forwarding):
    {
      "events": [
        {
          "reply_token": "...",
          "text": "hello",
          ...
        }
      ]
    }
    """
    try:
        body = await req.json()
    except Exception:
        return {"ok": True}

    events = body.get("events", [])
    if not events:
        return {"ok": True}

    evt = events[0]
    reply_token = evt.get("reply_token", "")
    user_text = evt.get("text", "") or ""

    # 沒有 reply_token 就無法 reply（例如某些 verify/ping）
    if not reply_token:
        return {"ok": True}

    # POC：尚未接 SQL 的情況下，遇到「看起來是查數據」的問題，先誠實回覆
    if looks_like_data_question(user_text):
        msg = (
            "我目前還沒接上 Fabric/Lakehouse SQL，所以無法直接查出實際數據。\n"
            "但我可以先幫你把這句話轉成「可查的 SQL 需求」或「要的指標/篩選條件」。\n\n"
            f"你問的是：{user_text}"
        )
        line_reply(reply_token, msg)
        return {"ok": True}

    # 一般問題：直接用 GPT 回答
    try:
        answer = call_openai_chat(user_text)
    except Exception as e:
        answer = f"GPT 呼叫失敗：{type(e).__name__}"

    line_reply(reply_token, answer)
    return {"ok": True}
