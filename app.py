import os
import re
import json
import requests
from fastapi import FastAPI, Request

app = FastAPI()

# ===== Env =====
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
PA_SQL_RUNNER_URL = os.getenv("PA_SQL_RUNNER_URL", "")

# ===== Safety rails =====
ALLOWED_FROM = [
    "dbo.cqcr310",      # 先用你已驗證 OK 的
    # "erp.cqcr310",    # 之後如果 SQL endpoint 這樣命名再加
]
BANNED_SQL = re.compile(r"\b(insert|update|delete|drop|alter|create|truncate|merge)\b", re.IGNORECASE)

# ===== Utils =====
@app.get("/health")
def health():
    return {"ok": True}


def line_reply(reply_token: str, text: str) -> None:
    """Reply message via LINE Reply API"""
    if not LINE_CHANNEL_ACCESS_TOKEN or not reply_token:
        print("Missing LINE token or reply_token")
        return

    url = "https://api.line.me/v2/bot/message/reply"
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {"replyToken": reply_token, "messages": [{"type": "text", "text": text[:5000]}]}
    resp = requests.post(url, headers=headers, json=payload, timeout=15)
    print("LINE reply:", resp.status_code, resp.text[:200])


def call_openai(messages):
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not set")

    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": OPENAI_MODEL,
        "messages": messages,
        "temperature": 0.0,
        "max_tokens": 650,
    }
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]


def generate_sql(question: str) -> str:
    """
    NL (Chinese) -> SQL
    Output only SQL. No markdown. No explanation.
    Target: SQL Server compatible syntax.
    """
    system = (
        "你是企業資料庫的 SQL 產生器。"
        "只輸出一段可執行 SQL（不要解釋、不要 markdown）。"
        "限制：只允許 SELECT。"
        f"FROM 只能使用以下白名單：{', '.join(ALLOWED_FROM)}。"
        "如果要近30天，請用 SQL Server 語法：WHERE Inspection_Date >= DATEADD(day,-30, CAST(GETDATE() AS date))。"
        "預設加上 TOP 50 限制避免資料過大。"
        "欄位已知：Plant, Inspection_Date, Product_Number, Product_Name, Supplier_Short_Name, "
        "Inspection_Item_Defect_Cause, Submitted_Quantity, Defect_Quantity, Sample_Size, Inspection_Result, Receiving_Number, Remark。"
        "常見需求：NG率=SUM(Defect_Quantity)/NULLIF(SUM(Submitted_Quantity),0)。"
    )
    user = f"問題：{question}\n請輸出 SQL："
    sql = call_openai([{"role": "system", "content": system}, {"role": "user", "content": user}]).strip()
    return sql


def validate_sql(sql: str) -> str:
    s = sql.strip().strip(";")
    if not s.lower().startswith("select"):
        raise ValueError("只允許 SELECT")
    if BANNED_SQL.search(s):
        raise ValueError("偵測到禁止的 SQL 關鍵字")

    ok = any(re.search(rf"\bfrom\s+{re.escape(t)}\b", s, re.IGNORECASE) for t in ALLOWED_FROM)
    if not ok:
        raise ValueError("FROM 來源不在白名單中")

    return s


def run_sql_via_pa(sql: str):
    if not PA_SQL_RUNNER_URL:
        raise RuntimeError("PA_SQL_RUNNER_URL not set")

    payload = {"sql": sql, "top": 50}
    r = requests.post(PA_SQL_RUNNER_URL, json=payload, timeout=90)
    if r.status_code >= 400:
        raise RuntimeError(f"PA runner error {r.status_code}: {r.text[:500]}")
    data = r.json()
    rows = data.get("rows", [])
    return rows


def summarize(question: str, sql: str, rows) -> str:
    """
    Chinese summary grounded on rows only.
    """
    system = (
        "你是品質/製造數據分析助理。"
        "你只能根據提供的 rows 回答，不可以臆測沒有的數字。"
        "回答請用繁體中文，先給結論，再條列重點。"
        "如果 rows 為空，請回覆：查無資料，並列出可能原因（例如：篩選條件太嚴格、日期範圍沒有資料）。"
        "回答長度控制在 6~10 行內。"
    )
    user = (
        f"問題：{question}\n"
        f"SQL：{sql}\n"
        f"rows(JSON，最多50筆)：\n{json.dumps(rows, ensure_ascii=False)[:12000]}"
    )
    return call_openai([{"role": "system", "content": system}, {"role": "user", "content": user}]).strip()


def line_push(user_id: str, text: str) -> None:
    if not LINE_CHANNEL_ACCESS_TOKEN or not user_id:
        print("Missing LINE token or user_id")
        return

    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {"to": user_id, "messages": [{"type": "text", "text": text[:5000]}]}
    resp = requests.post(url, headers=headers, json=payload, timeout=15)
    print("LINE push:", resp.status_code, resp.text[:200])


@app.post("/line/webhook")
async def line_webhook(req: Request):
    body = await req.json()
    print("LINE webhook received:", json.dumps(body, ensure_ascii=False)[:500])

    events = body.get("events", [])
    if not events:
        return {"ok": True}

    evt = events[0]
    reply_token = evt.get("replyToken") or evt.get("reply_token") or ""

    # 取得 userId（Push 需要）
    source = evt.get("source") or {}
    user_id = source.get("userId") or evt.get("user_id") or ""

    msg = evt.get("message") or {}
    text = (msg.get("text") or "").strip()

    if not reply_token or not text:
        return {"ok": True}

    # 1) 先用 reply token 回「收到」
    line_reply(reply_token, "收到，查詢中…")

    try:
        sql = generate_sql(text)
        sql = validate_sql(sql)
        rows = run_sql_via_pa(sql)
        answer = summarize(text, sql, rows)

        # 2) 這裡改用 push（不要再用 reply_token）
        line_push(user_id, answer)

    except Exception as e:
        line_push(user_id, f"查詢失敗：{type(e).__name__}\n{str(e)[:350]}")

    return {"ok": True}
