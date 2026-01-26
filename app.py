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
# Plant normalization
# =========================
PLANT_ALIAS = {
    "越南": ["越南", "vietnam", "VN", "vn", "越廠", "越南廠"],
    "昆山": ["昆山", "KS", "AK", "ks", "昆山廠"],
    "增達": ["增達", "ZD", "zd", "增達廠"],
}

# reverse map alias(lowered) -> cn
ALIAS_TO_CN = {}
for cn, aliases in PLANT_ALIAS.items():
    for a in aliases:
        ALIAS_TO_CN[a.strip().lower()] = cn


def normalize_plant_from_text(text: str) -> str | None:
    """Try find plant from user question text."""
    if not text:
        return None
    t = text.strip().lower()
    # exact alias match by containment
    for alias, cn in ALIAS_TO_CN.items():
        if alias and alias in t:
            return cn
    return None


def normalize_plant_value(raw: str) -> str | None:
    """Normalize a single plant value (from SQL literal) to CN."""
    if raw is None:
        return None
    v = str(raw).strip().strip('"').strip("'").strip()
    if not v:
        return None
    key = v.lower()
    key = key.replace("  ", " ").strip()
    key = key.replace("viet-nam", "vietnam").replace("viet nam", "viet nam")
    # direct match (cn values)
    if v in PLANT_ALIAS:
        return v
    # alias match
    if key in ALIAS_TO_CN:
        return ALIAS_TO_CN[key]
    return None


def enforce_plant_in_sql(sql: str, plant_cn: str | None) -> str:
    """
    Force Plant filter to use Chinese values in SQL.
    - If plant_cn provided (from user question), it overrides SQL's plant.
    - If SQL has Plant='Vietnam' etc, normalize to CN.
    - Supports "=" and "IN (...)"
    """
    if not sql:
        return sql

    original = sql

    # 1) Replace Plant = 'xxx'
    def repl_eq(m: re.Match):
        left = m.group(1)  # Plant or [Plant] etc (keep)
        quote = m.group(2)
        val = m.group(3)
        cn = plant_cn or normalize_plant_value(val) or val
        # SQL Server unicode literal
        return f"{left} = N'{cn}'"

    eq_pattern = re.compile(r"(\bPlant\b|\[Plant\]|\`Plant\`)\s*=\s*(['\"])([^'\"]+)\2", re.IGNORECASE)
    sql = eq_pattern.sub(repl_eq, sql)

    # 2) Replace Plant IN ('a','b',...)
    def repl_in(m: re.Match):
        left = m.group(1)
        inside = m.group(2)
        # split by comma, keep simple parsing
        parts = [p.strip() for p in inside.split(",")]
        new_vals = []
        for p in parts:
            pv = p.strip().strip("'").strip('"').strip()
            cn = plant_cn or normalize_plant_value(pv) or pv
            new_vals.append(f"N'{cn}'")
        return f"{left} IN ({', '.join(new_vals)})"

    in_pattern = re.compile(r"(\bPlant\b|\[Plant\]|\`Plant\`)\s+IN\s*\(([^)]+)\)", re.IGNORECASE)
    sql = in_pattern.sub(repl_in, sql)

    # 3) If user asked for a plant, but SQL has no plant filter -> inject it
    if plant_cn:
        has_plant_filter = re.search(r"(\bPlant\b|\[Plant\]|\`Plant\`)\s*(=|IN)\s*", sql, re.IGNORECASE) is not None
        if not has_plant_filter:
            # insert before GROUP BY / ORDER BY if exists, else append
            inject = f" Plant = N'{plant_cn}' "
            if re.search(r"\bWHERE\b", sql, re.IGNORECASE):
                # add AND before GROUP/ORDER
                sql = re.sub(r"\b(GROUP\s+BY|ORDER\s+BY)\b", rf"AND{inject}\n\1", sql, flags=re.IGNORECASE, count=1)
                if sql == original:
                    sql = sql.rstrip().rstrip(";") + f"\nAND{inject};"
            else:
                # add WHERE before GROUP/ORDER
                sql = re.sub(r"\b(GROUP\s+BY|ORDER\s+BY)\b", rf"WHERE{inject}\n\1", sql, flags=re.IGNORECASE, count=1)
                if sql == original:
                    sql = sql.rstrip().rstrip(";") + f"\nWHERE{inject};"

    if sql != original:
        print("SQL plant normalized.")
    return sql


# =========================
# Defect normalization
# =========================
# "不良" (defect) includes "特採" (special acceptance) and "驗退" (rejection).
# Per user: 不良=特採&驗退, 驗退=判退, 不良=NG
DEFECT_ALIAS = {
    "驗退": ["驗退", "判退"],
    "特採": ["特採"],
    # Assuming '允收' is a valid value for accepted
    "允收": ["允收", "ok", "accept"],
}

# Special group alias for "defect" which maps to multiple values
DEFECT_GROUP_ALIAS = {
    "不良": ["不良", "ng", "不良批"],
}

# Reverse map for single aliases (lowered) -> canonical name
DEFECT_ALIAS_TO_CN = {}
for cn, aliases in DEFECT_ALIAS.items():
    for a in aliases:
        DEFECT_ALIAS_TO_CN[a.strip().lower()] = cn


def normalize_defect_from_text(text: str) -> list[str] | None:
    """Try find defect terms from user question text. Returns a list of canonical defect names."""
    if not text:
        return None
    t = text.strip().lower()

    # Check for group alias first, as it's more specific
    for group_alias in DEFECT_GROUP_ALIAS["不良"]:
        if group_alias in t:
            return ["驗退", "特採"]  # "不良" means both rejected and special acceptance

    # Check for individual aliases
    found_defects = set()
    for alias, cn in DEFECT_ALIAS_TO_CN.items():
        if alias and alias in t:
            found_defects.add(cn)

    if found_defects:
        return list(found_defects)

    return None


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
def strip_code_fence(s: str) -> str:
    s = s.strip()
    s = re.sub(r"^```[a-zA-Z0-9]*\s*", "", s)
    s = re.sub(r"\s*```$", "", s)
    return s.strip()


def generate_sql(question: str) -> str:
    system = (
        "你是企業資料庫的 SQL 產生器。"
        "只輸出一段可執行 SQL（不要解釋、不要 markdown）。"
        "限制：只允許 SELECT。"
        f"FROM 只能使用以下白名單：{', '.join(ALLOWED_FROM)}。"
        "請特別注意：1) Plant 欄位是中文（越南/昆山/增達）。 2) Inspection_Result 欄位也是中文（合格/特採/驗退），其中「不良」或「NG」代表 Inspection_Result 是 '特採' 或 '驗退'。"
        "若要近30天，請使用 SQL Server 語法：WHERE Inspection_Date >= DATEADD(day,-30, CAST(GETDATE() AS date))。"
        "欄位已知：Plant, Inspection_Date, Product_Number, Product_Name, Supplier_Short_Name, "
        "Inspection_Item_Defect_Cause, Submitted_Quantity, Defect_Quantity, Sample_Size, Inspection_Result, Receiving_Number, Remark。"
        "常見需求：NG率=SUM(Defect_Quantity)/NULLIF(SUM(Submitted_Quantity),0)。"
        "請優先回傳可用於管理者查看的 Top N 結果（ORDER BY ... DESC）。"
    )
    user = f"問題：{question}\n請輸出 SQL："
    sql = call_openai([{"role": "system", "content": system}, {"role": "user", "content": user}]).strip()
    sql = strip_code_fence(sql)
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

def summarize_with_llm(question: str, sql: str, rows: list[dict]) -> str:
    """
    用 LLM 把 SQL 結果寫成自然中文回答（避免模板感）
    - 只餵前 N 筆（避免 token 太大）
    """
    # 避免 rows 太大爆 token：只取前 15 筆
    preview_rows = rows[:15]

    system = (
        "你是企業內部品質/製造數據助理。"
        "請用自然、口語但專業的繁體中文回答。"
        "回答要：1) 先一句話結論 2) 再列出重點數據(條列) 3) 如有不確定(例如欄位缺失/資料不足)要說明。"
        "不要提到你是AI，不要貼出SQL全文，除非使用者要求。"
        "數字盡量加上單位/百分比並四捨五入。"
        "若 rows 很少或為空，請直接說查不到資料並給可能原因。"
    )

    user = {
        "question": question,
        "sql_preview": sql[:600],        # 不要太長，避免它照抄
        "rows_preview": preview_rows     # 給它看資料
    }

    content = call_openai([
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(user, ensure_ascii=False)}
    ])

    return content.strip()[:4500]



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


# ===== Dedup (POC: in-memory) =====
PROCESSED = {}  # key -> timestamp
DEDUP_TTL_SECONDS = 15 * 60  # 15 min

def _cleanup_processed(now: float):
    # 清掉太舊的 key，避免記憶體一直長
    old_keys = [k for k, ts in PROCESSED.items() if now - ts > DEDUP_TTL_SECONDS]
    for k in old_keys:
        PROCESSED.pop(k, None)

def is_duplicate(event_key: str) -> bool:
    now = time.time()
    _cleanup_processed(now)
    if event_key in PROCESSED:
        return True
    PROCESSED[event_key] = now
    return False



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

    # 取 message id 當 dedup key（若沒有就用 replyToken + timestamp 組合）
    msg = evt.get("message") or {}
    msg_id = (msg.get("id") or "").strip()
    ts = str(evt.get("timestamp") or "")
    dedup_key = msg_id or f"{reply_token}:{ts}"

    if is_duplicate(dedup_key):
        print("Duplicate event, skip:", dedup_key)
        return {"ok": True}


    # 1) immediate reply
    line_reply(reply_token, "收到，查詢中…")

    try:
        sql = generate_sql(text)
        sql = validate_sql(sql)
        rows = run_sql_via_pa(sql)

        # local summary to avoid second OpenAI call (reduce 429 risk)
        # answer = summarize_locally(text, sql, rows)

        # Summarize answer with llm
        answer = summarize_with_llm(text, sql, rows)


        # 2) push result
        line_push(user_id, answer)

    except Exception as e:
        msg = str(e)
        if "rate limited" in msg.lower() or "429" in msg:
            line_push(user_id, "目前 AI 服務暫時被限流（429），請 1~2 分鐘後再試一次。")
        else:
            line_push(user_id, f"查詢失敗：{type(e).__name__}\n{msg[:350]}")

    return {"ok": True}
