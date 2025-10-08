import re, sys, pathlib

path = pathlib.Path("main.py")
src = path.read_text(encoding="utf-8")

def replace_block(src, route_pattern, new_block):
    # найдём начало: строка с @app.get("...") и def на следующей строке
    m = re.search(route_pattern, src)
    if not m:
        print(f"[skip] pattern not found: {route_pattern}")
        return src, False
    start = m.start()

    # от начала — найдём следующий декоратор @app. (начало следующего эндпоинта)
    tail = src[start:]
    m2 = re.search(r'\n@app\.(get|post|put|patch|delete)\(', tail[1:])  # [1:] чтобы не поймать сам себя
    end = start + (m2.start()+1 if m2 else len(tail))

    # заменяем фрагмент
    before = src[:start]
    after  = src[end:]
    # гарантируем один перевод строки слева/справа
    if not before.endswith("\n\n"): before += "\n"
    if not new_block.endswith("\n"): new_block += "\n"
    if not after.startswith("\n"): after = "\n" + after
    return before + new_block + after, True

media_block = r'''
@app.get("/ig/insights/media")
async def ig_media_insights(
    media_id: str = Query(..., description="Media ID"),
    metrics: str = Query("", description="Comma-separated metrics; if empty — auto by media type"),
):
    st = await _load_state()
    async with RetryClient() as client:
        # 1) media_type (надёжно)
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
                "status": e.response.status_code if e.response is not None else None,
                "error": (e.response.json() if (e.response is not None) else str(e)),
            }

        # 2) product_type (мягкая попытка)
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

        # для обычных медиа (image/video) impressions недоступны на media insights — уберём если просили
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
                "status": e.response.status_code if e.response is not None else None,
                "error": err_json or str(e),
            }

        data = ins.json() or {}
        return {"ok": True, "media_type": media_type, "product_type": product_type, "metrics": req_metrics, "data": data}
'''.strip()

account_block = r'''
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
            params={"metric": ",".join(req_metrics), "period": period, "access_token": st["page_token"]},
            retries=4,
        )
        r.raise_for_status()
        return r.json()
'''.strip()

changed = False
src, ok1 = replace_block(src, r'@app\.get\("/ig/insights/media"\)', media_block)
changed |= ok1
src, ok2 = replace_block(src, r'@app\.get\("/ig/insights/account"\)', account_block)
changed |= ok2

if changed:
    path.write_text(src, encoding="utf-8")
    print("Patched insights blocks.")
else:
    print("Nothing patched (patterns not found).")
