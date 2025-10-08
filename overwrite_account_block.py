import re, pathlib

p = pathlib.Path("main.py")
src = p.read_text(encoding="utf-8")

start_re = re.compile(r'^[ \t]*@app\.get\("/ig/insights/account"\)', re.M)

m = start_re.search(src)
if not m:
    print("Не нашёл @app.get(\"/ig/insights/account\") — ничего не делаю.")
    raise SystemExit(0)

start = m.start()

# найти начало следующего эндпоинта @app.(get|post|put|patch|delete)
tail = src[start+1:]
m2 = re.search(r'\n@app\.(get|post|put|patch|delete)\(', tail)
end = start + 1 + (m2.start() if m2 else len(tail))

fixed_block = '''
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
'''.strip() + "\n"

new_src = src[:start] + fixed_block + src[end:]
p.write_text(new_src, encoding="utf-8")
print("Блок /ig/insights/account перезаписан.")
