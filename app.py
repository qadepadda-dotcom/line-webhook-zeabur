import os
import re
import json
import time
import random
import requests
from fastapi import FastAPI, Request

app = FastAPI()

# =========================
# Environment Variables
# =========================
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()
PA_SQL_RUNNER_URL = os.getenv("PA_SQL_RUNNER_URL", "").strip()

# =========================
# Safety rails
# =========================
# 先放你已驗證 OK 的表；之後改 view / gold table 再加進來
ALLOWED_FROM = [
    "dbo.cqcr310",
]

BANNED_SQL = re.compile(r"\b(insert|update|delete|drop|alter|create|truncate|merge)\b", re.IGNORECASE)


# =========================
# Basic endpoints
# =========================
@app.get("/health")
def health():
    return {"ok": True}


@app.get("/")
def root():
    return {"ok": True, "service": "line-webhook"}


# =========================
# LINE APIs
# =========================
def line_reply(reply_token: str, text: str) -> None:
    """Reply immediately (replyToken is one-time-use)."""
    if not LINE_CHANNEL_ACCESS_TOKEN or not reply_token:
        print("Missing LINE_CHANNEL_ACCESS_TOKEN or reply_token")
        return

    url = "https://api.line.me/v2/bot/message/reply"
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {"replyToken": reply_token, "messages": [{"type": "text", "text": text[:5000]}]}
    resp = requests.post(url, headers=headers, json=payload, timeout=15)
    print("LINE reply:", resp.status_code, resp.text[:200])


def line_push(user_id: str, text: str) -> None:
    """Push message (for second message after replying)."""
    if not LINE_CHANNEL_ACCESS_TOKEN or not user_id:
        print("Missing LINE_CHANNEL_ACCESS_TOKEN or user_id")
        return

    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {"to": user_id, "messages": [{"type": "text", "text": text[:5000]}]}
    resp = requests.post(url, headers=headers, json=payload, timeout=15)
    print("LINE push:", resp.status_code, resp.text[:200])


# =========================
# OpenAI (with retries)
# =========================
def call_openai(messages):
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not set (Zeabur Variables)")

    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": OPENAI_MODEL,
        "messages": messages,
        "temperature": 0.0,
        "max_tokens": 650,
    }

    # Retry for 429 / 5xx
    for attempt in range(5):
        r = requests.post(url, headers=headers, json=payload, timeout=60)

        if r.status_code == 429 or (500 <= r.status_code < 600):
            wait = (2 ** attempt) + random.uniform(0, 0.8)
            print(f"OpenAI retry {attempt+1}/5, status={r.status_code}, wait={wait:.1f}s, body={r.text[:120]}")
            time.sleep(wait)
            continue

        if r.status_code >= 400:
            raise RuntimeError(f"OpenAI error {r.status_code}: {r.text[:300]}")

        return r.json()["choices"][0]["message"]["content"]

    raise RuntimeError("OpenAI rate limited (429). Please try again later.")


# =========================
# NL -> SQL
# =========================
def generate_sql(question: str) -> str:
    """
    Chinese NL -> SQL Server SQL
    Output only SQL. No markdown.
    """
    system = (
        "你是企業資料庫的 SQL 產生器。"
        "只輸出一段可執行 SQL（不要解釋、不要 markdown）。"
        "限制：只允許 SELECT。"
        f"FROM 只能使用以下白名單：{', '.join(ALLOWED_FROM)}。"
        "若要近30天，請使用 SQL Server 語法：WHERE Inspection_Date >= DATEADD(day,-30, CAST(GETDATE() AS date))。"
        "預設加上 TOP 50 限制避免資料過大。"
        "欄位已知：Plant, Inspection_Date, Product_Number, Product_Name, Supplier_Short_Name, "
        "Inspection_Item_Defect_Cause, Submitted_Quantity, Defect_Quantity, Sample_Size, Inspection_Result, Receiving_Number, Remark。"
        "常見需求：NG率=SUM(Defect_Quantity)/NULLIF(SUM(Submitted_Quantity),0)。"
        "請優先回傳可用於管理者查看的 Top N 結果（ORDER BY ... DESC）。"
    )
    user = f"問題：{question}\n請輸出 SQL："
    sql = call_openai([{"role": "system", "content": system}, {"role": "user", "content": user}]).strip()
    return sql.strip().strip(";")


def validate_sql(sql: str) -> str:
    s = sql.strip().strip(";")
    if not s.lower().startswith("select"):
        raise ValueError("只允許 SELECT")
    if BANNED_SQL.search(s):
        raise ValueError("偵測到禁止的 SQL 關鍵字")

    ok = any(re.search(rf"\bfrom\s+{re.escape(t)}\b", s, re.IGNORECASE) for t in ALLOWED_FROM)
    if not ok:
        raise ValueError(f"FROM 來源不在白名單：{ALLOWED_FROM}")

    return s


# =========================
# Call Power Automate SQL Runner
# =========================
def run_sql_via_pa(sql: str):
    if not PA_SQL_RUNNER_URL:
        raise RuntimeError("PA_SQL_RUNNER_URL not set (Zeabur Variables)")

    payload = {"sql": sql, "top": 50}
    print("Calling PA runner:", PA_SQL_RUNNER_URL[:80], "...")
    print("SQL:", sql[:220])

    r = requests.post(PA_SQL_RUNNER_URL, json=payload, timeout=90)
    print("PA runner status:", r.status_code, r.text[:200])

    if r.status_code >= 400:
        raise RuntimeError(f"PA runner error {r.status_code}: {r.text[:500]}")

    data = r.json()
    return data.get("rows", [])


# =========================
# Local summary (no 2nd OpenAI call)
# =========================
def summarize_locally(question: str, sql: str, rows) -> str:
    if not rows:
        return "查無資料：可能是篩選條件太嚴格或近30天沒有資料。"

    def get_float(x):
        try:
            return float(x)
        except Exception:
            return None

    top = rows[:10]
    first = top[0]

    plant = first.get("plant") or first.get("Plant") or ""
    part_no = first.get("part_no") or first.get("Product_Number") or ""
    part_name = first.get("part_name") or first.get("Product_Name") or ""
    ng_rate = get_float(first.get("ng_rate") or first.get("NG_Rate") or first.get("ngRate"))

    lines = []
    if ng_rate is not None:
        lines.append(f"📌 近30天 NG率最高：{plant} / {part_no}（{part_name}），NG率約 {ng_rate*100:.2f}%")
    else:
        lines.append(f"📌 近30天結果第一名：{plant} / {part_no}（{part_name}）")

    lines.append("前10名如下：")
    for i, r in enumerate(top, 1):
        p = r.get("plant") or r.get("Plant") or ""
        pn = r.get("part_no") or r.get("Product_Number") or ""
        pr = get_float(r.get("ng_rate") or r.get("NG_Rate") or r.get("ngRate"))
        if pr is not None:
            lines.append(f"{i}. {p} / {pn}  NG率 {pr*100:.2f}%")
        else:
            lines.append(f"{i}. {p} / {pn}")

    return "\n".join(lines)[:4500]


# =========================
# LINE Webhook
# =========================
@app.post("/line/webhook")
async def line_webhook(req: Request):
    body = await req.json()
    print("LINE webhook received:", json.dumps(body, ensure_ascii=False)[:600])
    print("OPENAI_API_KEY len:", len(OPENAI_API_KEY or ""))

    events = body.get("events", [])
    if not events:
        return {"ok": True}

    evt = events[0]

    # replyToken (one-time)
    reply_token = evt.get("replyToken") or evt.get("reply_token") or ""

    # userId for push
    source = evt.get("source") or {}
    user_id = source.get("userId") or evt.get("user_id") or ""

    # text
    msg = evt.get("message") or {}
    text = ""
    if isinstance(msg, dict):
        text = (msg.get("text") or "").strip()
    if not text:
        text = (evt.get("text") or "").strip()

    if not reply_token or not text:
        return {"ok": True}

    # 1) immediate reply
    line_reply(reply_token, "收到，查詢中…")

    try:
        sql = generate_sql(text)
        sql = validate_sql(sql)
        rows = run_sql_via_pa(sql)

        # local summary to avoid second OpenAI call (reduce 429 risk)
        answer = summarize_locally(text, sql, rows)

        # 2) push result
        line_push(user_id, answer)

    except Exception as e:
        msg = str(e)
        if "rate limited" in msg.lower() or "429" in msg:
            line_push(user_id, "目前 AI 服務暫時被限流（429），請 1~2 分鐘後再試一次。")
        else:
            line_push(user_id, f"查詢失敗：{type(e).__name__}\n{msg[:350]}")

    return {"ok": True}
