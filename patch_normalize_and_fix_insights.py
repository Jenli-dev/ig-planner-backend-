import re, pathlib, sys

p = pathlib.Path("main.py")
src = p.read_text(encoding="utf-8")

# --- Нормализация: \r, табы, экзотические пробелы ---
src = src.replace("\r\n", "\n").replace("\r", "\n")
src = src.replace("\t", "    ")
# заменим no-break space и др. юникод-пробелы на обычный
src = src.replace("\u00A0", " ").replace("\u2009", " ").replace("\u2007", " ").replace("\u202F", " ")

# --- Декораторы @app.* должны начинаться в колонке 0 ---
lines = src.splitlines()
for i, line in enumerate(lines):
    if re.match(r'^\s*@app\.(get|post|put|patch|delete)\(', line):
        lines[i] = re.sub(r'^\s+', '', line)
src = "\n".join(lines)

def replace_block(src, start_pattern, fixed_block):
    start_re = re.compile(r'^[ \t]*' + start_pattern, re.M)
    m = start_re.search(src)
    if not m:
        return src, False
    start = m.start()
    tail = src[start+1:]
    m2 = re.search(r'\n@app\.(get|post|put|patch|delete)\(', tail)
    end = start + 1 + (m2.start() if m2 else len(tail))
    new_src = src[:start] + fixed_block.rstrip() + "\n\n" + src[end:].lstrip("\n")
    return new_src, True

# --- Готовые ровные блоки ---
account_block = '''
ACCOUNT_INSIGHT_ALLOWED = {"impressions", "reach", "profile_views"}

@app.get("/ig/insights/account")
async def ig_account_insights(
    metrics: str = Query("impressions,reach,profile_views"),
    period: str = Query("day"),
):
    st = await _load_state()
    req_metrics = [m.strip() for m in metrics.split(",") if m.strip()]
    bad = [m for m in req_metrics if m not in ACCOUNT_INSIGHT_ALLOWED]
    if bad:
        raise HTTPException(400, f"Unsupported metrics: {bad}. Allowed: {sorted(ACCOUNT_INSIGHT_ALLOWED)}")

    async with RetryClient() as client:
        r = await client.get(
            f"{GRAPH_BASE}/{st['ig_id']}/insights",
            params={
                "metric": ",".join(req_metrics),
                "period": period,
                "access_token": st["page_token"],
            },
            retries=4,
        )
        r.raise_for_status()
        return r.json()
'''.strip()

media_block = '''
@app.get("/ig/insights/media")
async def ig_media_insights(
    media_id: str = Query(..., description="Media ID"),
    metrics: str = Query("", description="Comma-separated metrics; if empty — auto by media type"),
):
    st = await _load_state()
    async with RetryClient() as client:
        # media_type
        try:
            r1 = await client.get(
                f"{GRAPH_BASE}/{media_id}",
                params={"fields": "media_type", "access_token": st["page_token"]},
                retries=4,
            )
            r1.raise_for_status()
            media_type = (r1.json() or {}).get("media_type", "")
        except httpx.HTTPStatusError as e:
            return {
                "ok": False,
                "stage": "get_media_type",
                "status": (e.response.status_code if e.response is not None else None),
                "error": (e.response.json() if (e.response is not None) else str(e)),
            }

        # product_type (мягко)
        product_type = None
        try:
            r2 = await client.get(
                f"{GRAPH_BASE}/{media_id}",
                params={"fields": "product_type", "access_token": st["page_token"]},
                retries=4,
            )
            if r2.status_code == 200:
                product_type = (r2.json() or {}).get("product_type")
        except Exception:
            pass

        mt_upper = (product_type or media_type or "").upper()
        req_metrics = [m.strip() for m in metrics.split(",") if m.strip()] if metrics else _pick_metrics_for_media(mt_upper)

        if mt_upper in ("IMAGE", "PHOTO", "CAROUSEL", "CAROUSEL_ALBUM", "VIDEO"):
            req_metrics = [m for m in req_metrics if m != "impressions"]

        try:
            ins = await client.get(
                f"{GRAPH_BASE}/{media_id}/insights",
                params={"metric": ",".join(req_metrics), "access_token": st["page_token"]},
            )
            ins.raise_for_status()
        except httpx.HTTPStatusError as e:
            try:
                err_json = e.response.json()
            except Exception:
                err_json = {}
            return {
                "ok": False,
                "stage": "insights",
                "status": (e.response.status_code if e.response is not None else None),
                "error": err_json or str(e),
            }

        data = ins.json() or {}
        return {
            "ok": True,
            "media_type": media_type,
            "product_type": product_type,
            "metrics": req_metrics,
            "data": data,
        }
'''.strip()

# Перезаписать /ig/insights/account
src, ok1 = replace_block(src, r'@app\.get\("/ig/insights/account"\)', account_block)
# Перезаписать /ig/insights/media
src, ok2 = replace_block(src, r'@app\.get\("/ig/insights/media"\)', media_block)

p.write_text(src, encoding="utf-8")
print(f"Patched. account={ok1}, media={ok2}")

# Проверка компиляции, чтобы сразу увидеть точную строку при ошибке
import py_compile
try:
    py_compile.compile(str(p), doraise=True)
    print("Compile OK.")
except Exception as e:
    print("Compile error:", e)
    # показать 20 строк вокруг проблемной
    import traceback
    tb = e.__traceback__
    # В Python3.9 для py_compile это не всегда даёт lineno; пробуем из текста:
    m = re.search(r'line (\d+)', str(e))
    if m:
        ln = int(m.group(1))
        lines = src.splitlines()
        lo = max(1, ln-10); hi = min(len(lines), ln+10)
        width = len(str(hi))
        print(f"\n--- context {lo}-{hi} ---")
        for i in range(lo, hi+1):
            print(str(i).rjust(width), ": ", lines[i-1])
        print("--- end context ---")
        sys.exit(2)
    raise
