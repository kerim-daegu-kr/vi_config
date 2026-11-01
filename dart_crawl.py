#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DART 기업개황 크롤러 (GitHub Actions 등 OpenSSL 런타임용).
  · Supabase Edge(Deno)는 OpenDART 와 TLS 핸드셰이크가 안 돼서(레거시 암호스위트),
    크롤러는 OpenSSL 되는 런타임에서 돌리고, 적재는 이미 만든 Supabase RPC 를 REST 로 호출한다.
  · 모드:
      python dart_crawl.py sync    # corpCode.xml 로 목록 전체 동기화(+말소 마감)
      python dart_crawl.py crawl   # 기업개황 배치 수집(일일 상한/런당 상한까지)
환경변수(GitHub Secrets):
  SUPABASE_URL                DART 프로젝트 URL (예: https://xxxx.supabase.co)
  SUPABASE_SERVICE_ROLE_KEY   DART 프로젝트 service_role 키
  OPENDART_KEY_CRAWL          OpenDART API 키#1(크롤용)
선택:
  DART_DAILY_CAP (기본 10000)  하루 개황 콜 상한(OpenDART 2만/일 중 안전 배분)
  DART_PER_RUN   (기본 1500)   이 실행 1회에서 처리할 최대 건수(러너 시간 제한)
  DART_BATCH     (기본 300)    pick/ upsert 배치 크기
  DART_CONCURRENCY (기본 5)    company.json 동시 요청 수
"""
import os, sys, io, re, json, time, zipfile, hashlib
import urllib.request, urllib.parse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

SB_URL = os.environ["SUPABASE_URL"].rstrip("/")
SB_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
DART_KEY = os.environ["OPENDART_KEY_CRAWL"]
DAILY_CAP = int(os.environ.get("DART_DAILY_CAP", "10000"))
PER_RUN = int(os.environ.get("DART_PER_RUN", "1500"))
BATCH = int(os.environ.get("DART_BATCH", "300"))
CONC = int(os.environ.get("DART_CONCURRENCY", "5"))

DART_BASE = "https://opendart.fss.or.kr/api/"

# ---- Supabase RPC (REST) ----------------------------------------------------
def rpc(fn, args):
    data = json.dumps(args).encode("utf-8")
    req = urllib.request.Request(
        f"{SB_URL}/rest/v1/rpc/{fn}", data=data, method="POST",
        headers={"apikey": SB_KEY, "Authorization": f"Bearer {SB_KEY}",
                 "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=90) as r:
        body = r.read().decode("utf-8")
    return json.loads(body) if body else None

# ---- OpenDART ---------------------------------------------------------------
def dart_get(path, params, timeout=60, binary=False):
    url = DART_BASE + path + "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read() if binary else r.read().decode("utf-8")

# ---- 목록 동기화 ------------------------------------------------------------
_FIELD_RE = {t: re.compile(r"<%s>(.*?)</%s>" % (t, t), re.S)
             for t in ("corp_code", "corp_name", "stock_code", "modify_date")}
_LIST_RE = re.compile(r"<list>(.*?)</list>", re.S)

def _f(block, tag):
    m = _FIELD_RE[tag].search(block)
    return m.group(1).strip() if m else ""

def sync_corp():
    started = datetime.now(timezone.utc).isoformat()
    raw = dart_get("corpCode.xml", {"crtfc_key": DART_KEY}, timeout=180, binary=True)
    zf = zipfile.ZipFile(io.BytesIO(raw))
    xmlname = next(n for n in zf.namelist() if n.upper().endswith(".XML"))
    xml = zf.read(xmlname).decode("utf-8")
    rows, up, total = [], 0, 0
    for m in _LIST_RE.finditer(xml):
        b = m.group(1)
        code = _f(b, "corp_code")
        if not code:
            continue
        rows.append({"corp_code": code, "corp_name": _f(b, "corp_name"),
                     "stock_code": _f(b, "stock_code"), "modify_date": _f(b, "modify_date")})
        total += 1
        if len(rows) >= 1000:
            up += rpc("dart_upsert_corp_batch", {"p_rows": rows}) or 0
            rows = []
    if rows:
        up += rpc("dart_upsert_corp_batch", {"p_rows": rows}) or 0
    deact = rpc("dart_corp_finalize_sync", {"p_started": started}) or 0
    print(f"[sync] total={total} upserted={up} deactivated={deact}")

# ---- 개황 수집 --------------------------------------------------------------
_HASH_KEYS = ("ceo_nm", "adres", "induty_code", "corp_cls", "stock_code",
              "est_dt", "acc_mt", "phn_no", "corp_name", "bizr_no", "jurir_no")

def _fetch_profile(code):
    """반환: (row_or_None, counts_this_call). status 020(한도초과)면 ('QUOTA', 1)."""
    try:
        d = json.loads(dart_get("company.json", {"crtfc_key": DART_KEY, "corp_code": code}, timeout=30))
    except Exception:
        return (None, 0)            # 네트워크 실패 → 카운트 안 함, need_refresh 유지(재시도)
    st = str(d.get("status") or "")
    if st in ("020", "021"):        # 사용한도 초과 / 조회가능 회사수 초과
        return ("QUOTA", 1)
    if st not in ("000", "013"):    # 그 외 오류 → 재시도 위해 스킵(카운트는 함)
        return (None, 1)
    adres = (d.get("adres") or "").strip()
    parts = adres.split()
    base = "|".join(str(d.get(k) or "") for k in _HASH_KEYS)
    ch = hashlib.sha256(base.encode("utf-8")).hexdigest()
    row = {"corp_code": code,
           "corp_name": d.get("corp_name"), "corp_name_eng": d.get("corp_name_eng"),
           "stock_name": d.get("stock_name"), "stock_code": d.get("stock_code") or "",
           "ceo_nm": d.get("ceo_nm"), "corp_cls": d.get("corp_cls"),
           "jurir_no": d.get("jurir_no"), "bizr_no": d.get("bizr_no"),
           "adres": adres or None, "sido": (parts[0] if parts else None),
           "sigungu": (parts[1] if len(parts) > 1 else None),
           "hm_url": d.get("hm_url"), "phn_no": d.get("phn_no"), "fax_no": d.get("fax_no"),
           "induty_code": d.get("induty_code"), "est_dt": d.get("est_dt"),
           "acc_mt": d.get("acc_mt"), "content_hash": ch}
    return (row, 1)

def crawl():
    used = rpc("dart_bump_profile_calls", {"p_n": 0}) or 0
    if used >= DAILY_CAP:
        print(f"[crawl] daily cap reached ({used}/{DAILY_CAP})"); return
    budget = min(PER_RUN, DAILY_CAP - used)
    done = 0
    while budget > 0:
        take = min(BATCH, budget)
        targets = rpc("dart_pick_targets", {"p_limit": take}) or []
        codes = [t if isinstance(t, str) else t.get("dart_pick_targets") for t in targets]
        codes = [c for c in codes if c]
        if not codes:
            print("[crawl] no targets"); break
        rows, calls, quota_hit = [], 0, False
        with ThreadPoolExecutor(max_workers=CONC) as ex:
            for res in ex.map(_fetch_profile, codes):
                row, c = res
                calls += c
                if row == "QUOTA":
                    quota_hit = True
                elif row:
                    rows.append(row)
        if rows:
            rpc("dart_upsert_company_batch", {"p_rows": rows})
        if calls:
            used = rpc("dart_bump_profile_calls", {"p_n": calls}) or (used + calls)
        done += calls
        budget -= calls
        if quota_hit:
            print("[crawl] OpenDART quota hit → stop"); break
        time.sleep(0.2)
    print(f"[crawl] processed={done} used_today={used}/{DAILY_CAP}")

# ---- main -------------------------------------------------------------------
if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "crawl"
    if mode == "sync":
        sync_corp()
    elif mode == "crawl":
        crawl()
    else:
        print("usage: dart_crawl.py [sync|crawl]"); sys.exit(2)
