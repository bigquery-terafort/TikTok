"""
TikTok Ads → BigQuery Full Sync (v3.1)
======================================
REPO: bigquery-terafort/TikTok

Extracts ALL available data from TikTok Marketing API v1.3:
  - 9 report tables · 4 dimension tables (campaigns, adgroups, ads, APPS)
  - 1 sync log table

🔴 v3 KE FIX (BigQuery se saabit) — barqarar:

  1. `tiktok_apps_dim` = 0 ROWS — `/app/list/` kuch nahi de raha.
     v3: apps_dim khali → run fail, chup nahi.
  2. DELETE fetch se PEHLE + `except: pass` → v3: PEHLE fetch, phir delete.
  3. Dimension pagination adhoori pe raise.
  4. status "SUCCESS" jhoot bolta tha → v3: PARTIAL pe exit(1).

🔴 v3.1 KE FIX (v3 ka guard khud toot raha tha):

  A. 🚨 ASLI BUG — `sys.exit(1)` `try:` block ke ANDAR tha.
     `SystemExit` `BaseException` se inherit karta hai, `Exception` se NAHI.
     Isliye `except Exception` use pakadta hi nahi tha, `status` "SUCCESS"
     hi raha, aur `finally` ne `tiktok_sync_log` mein **status='SUCCESS'**
     likh diya — jabke run FAIL hui. Yani fix #4 ("SUCCESS jhoot na bole")
     apps_dim guard pe kaam hi nahi kar raha tha.
     Saboot: 26 Jul 20:41 · 27 Jul 06:55 · 27 Jul 09:34 — teeno runs
     failed, teeno sync_log mein SUCCESS, tables_synced=0.
     v3.1: guard ab `RuntimeError` raise karta hai → handler pakadta hai →
     status='FAILED' sach-much likha jaata hai.

  B. `tables_synced += 4` galat tha — apps_dim load hui ho ya na ho, 4 gin
     leta tha (aur guard se pehle exit hone pe 0 rehta tha jabke 3 dim
     tables likhi ja chuki theen).
     v3.1: har load ke saath +1, sach.

  C. ALLOW_EMPTY_APPS_DIM=1 lagane par bhi run RED hoti thi (PARTIAL →
     exit 1). Rozana red build = alert fatigue = asli failure ignore.
     v3.1: jaan-boojh ke waive kiya gaya ho to status
     "SUCCESS_APPS_DIM_WAIVED" aur **exit 0**, magar log mein LOUD warning.
     Report table khali ho to wo ab bhi PARTIAL → exit 1 (wo ghair-mutawaqqa
     hai).

  D. `fetch_report()` ki pagination `api_get_paginated()` jaisi hard NAHI
     thi:
         total = data.get("page_info", {}).get("total_number", 0)
         if len(all_rows) >= total: break
     `total_number` gayab → 0 → `len >= 0` hamesha sach → page 1 ke baad
     chup-chaap break, aur us adhoore set se delete+load. Abhi latent hai
     (~60 rows/din vs PAGE_SIZE 1000) lekin volume barhte hi kaatega.
     v3.1: declared-total ke bagair short-page tak paging, mismatch pe raise.

  E. `datetime.utcnow()` deprecated (3.12+). v3.1: timezone-aware UTC.

ℹ️  MAOJOODA HAALAT (27 Jul 2026, BigQuery se verify shuda):
      tiktok_app_mapping .... 24 rows, ZERO placeholder ✅
      adgroups_dim ......... 99/99 rows mein app_id maujood (24 distinct)
      mapping coverage ..... 24 mein se 23 covered
      UNMAPPED ............. app_id 7449825786103775233  ← package_name chahiye
    Yani spend attribution manual mapping se mehfooz hai — isi liye
    ALLOW_EMPTY_APPS_DIM=1 lagana filhaal SAHI faisla hai.
    NOTE: docstring ka purana zikr ke 7660109995593252871 unmapped hai —
    wo ab `com.tf.ai.voice.changer.app` pe map ho chuka hai.

Auth: Long-lived access token (doesn't expire)
Rate limit: 600 requests/minute · Max date range: 365 days · page_size ≤ 1000
"""

import os
import sys
import json
import time
import argparse
import logging
import requests
from datetime import datetime, timedelta, date, timezone
from typing import Any, Dict, List, Optional

from google.cloud import bigquery
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# =============================================================================
# CONFIG
# =============================================================================

ACCESS_TOKEN    = os.environ.get("TIKTOK_ACCESS_TOKEN", "").strip()
ADVERTISER_ID   = os.environ.get("TIKTOK_ADVERTISER_ID", "").strip()
GCP_PROJECT     = os.environ.get("GCP_PROJECT_ID", "").strip()
GCP_CREDS_JSON  = os.environ.get("GCP_CREDENTIALS_JSON", "").strip()
BQ_DATASET      = os.environ.get("BQ_DATASET", "TikTok").strip()
BQ_LOCATION     = os.environ.get("BQ_LOCATION", "US").strip()
# NOTE: dead knob. main() CLI ke `--days` se chalta hai; ye env var kahin
# use nahi hota. Workflow dono bhejta hai — confusion se bachne ke liye
# yahan rakha hai taake dikhe, lekin faisla hamesha --days ka hai.
LOOKBACK_DAYS   = int(os.environ.get("TIKTOK_LOOKBACK_DAYS", "3"))
def _env_flag(name: str, default: str = "0") -> bool:
    """Tolerant boolean env parsing.

    v3.1 mein ye `os.environ.get(name, "0") == "1"` tha — bilkul strict.
    GitHub Actions kabhi kabhi value ke saath whitespace/newline bhej deta
    hai, aur insaan '1' ke saath saath 'true'/'yes' bhi likhte hain. Strict
    match un sab ko chup-chaap ignore kar deta tha aur guard armed rehta —
    yani flag "set kar diya" lagta tha magar asar koi nahi.
    """
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "on")


# 🛡️ agar apps_dim jaan-boojh ke khali chhodni ho (manual tiktok_app_mapping
#    pe bharosa) to ye "1" set kar do. Default: khali = FAIL.
ALLOW_EMPTY_APPS_DIM = _env_flag("ALLOW_EMPTY_APPS_DIM")

BASE_URL = "https://business-api.tiktok.com/open_api/v1.3"

MAX_RETRIES   = 4
RETRY_BACKOFF = 5
PAGE_SIZE     = 1000

# =============================================================================
# METRICS
# =============================================================================

BASIC_METRICS = [
    "spend", "impressions", "clicks", "reach",
    "ctr", "cpc", "cpm", "cost_per_1000_reached", "frequency",
    "conversion", "cost_per_conversion", "conversion_rate",
    "real_time_conversion", "real_time_cost_per_conversion", "real_time_conversion_rate",
    "result", "cost_per_result", "result_rate",
    "real_time_result", "real_time_cost_per_result", "real_time_result_rate",
    "video_play_actions", "video_watched_2s", "video_watched_6s",
    "average_video_play", "average_video_play_per_user",
    "video_views_p25", "video_views_p50", "video_views_p75", "video_views_p100",
    "engaged_view", "engaged_view_15s",
    "likes", "comments", "shares", "follows", "profile_visits",
    "real_time_app_install", "real_time_app_install_cost",
    "app_install",
]

AUDIENCE_METRICS = [
    "spend", "impressions", "clicks", "reach",
    "ctr", "cpc", "cpm", "frequency",
    "conversion", "cost_per_conversion", "conversion_rate",
    "video_play_actions", "video_watched_2s", "video_watched_6s",
    "video_views_p25", "video_views_p50", "video_views_p75", "video_views_p100",
    "likes", "comments", "shares",
    "real_time_app_install",
]

# =============================================================================
# SCHEMAS
# =============================================================================

S = bigquery.SchemaField

def _report_schema(extra_dims=None):
    fields = [
        S("report_date", "DATE"),
        S("advertiser_id", "STRING"),
        S("run_id", "STRING"),
        S("_ingested_at", "TIMESTAMP"),
    ]
    if extra_dims:
        fields.extend(extra_dims)
    all_metrics = list(set(BASIC_METRICS + AUDIENCE_METRICS))
    for m in sorted(all_metrics):
        fields.append(S(m, "FLOAT64"))
    for name_field in ["campaign_name", "adgroup_name", "ad_name"]:
        if not any(f.name == name_field for f in fields):
            fields.append(S(name_field, "STRING"))
    return fields

SCHEMAS = {
    "tiktok_daily_advertiser": _report_schema(),
    "tiktok_daily_campaign": _report_schema([
        S("campaign_id", "STRING"), S("campaign_name", "STRING"),
    ]),
    "tiktok_daily_adgroup": _report_schema([
        S("campaign_id", "STRING"), S("adgroup_id", "STRING"), S("adgroup_name", "STRING"),
    ]),
    "tiktok_daily_ad": _report_schema([
        S("campaign_id", "STRING"), S("adgroup_id", "STRING"),
        S("ad_id", "STRING"), S("ad_name", "STRING"),
    ]),
    "tiktok_daily_country": _report_schema([
        S("campaign_id", "STRING"), S("country_code", "STRING"),
    ]),
    "tiktok_audience_gender": _report_schema([
        S("campaign_id", "STRING"), S("gender", "STRING"),
    ]),
    "tiktok_audience_age": _report_schema([
        S("campaign_id", "STRING"), S("age", "STRING"),
    ]),
    "tiktok_audience_platform": _report_schema([
        S("campaign_id", "STRING"), S("platform", "STRING"),
    ]),
    "tiktok_audience_language": _report_schema([
        S("campaign_id", "STRING"), S("language", "STRING"),
    ]),

    "tiktok_campaigns_dim": [
        S("campaign_id", "STRING"), S("campaign_name", "STRING"),
        S("objective_type", "STRING"), S("budget", "FLOAT64"),
        S("budget_mode", "STRING"), S("campaign_type", "STRING"),
        S("status", "STRING"), S("create_time", "STRING"),
        S("modify_time", "STRING"),
        S("is_smart_performance_campaign", "BOOLEAN"),
        S("_ingested_at", "TIMESTAMP"),
    ],
    "tiktok_adgroups_dim": [
        S("adgroup_id", "STRING"), S("adgroup_name", "STRING"),
        S("campaign_id", "STRING"), S("placement_type", "STRING"),
        S("placements", "STRING"), S("bid_type", "STRING"),
        S("bid_price", "FLOAT64"), S("budget", "FLOAT64"),
        S("budget_mode", "STRING"), S("optimization_goal", "STRING"),
        S("billing_event", "STRING"), S("pacing", "STRING"),
        S("status", "STRING"), S("age_groups", "STRING"),
        S("gender", "STRING"), S("location_ids", "STRING"),
        S("languages", "STRING"), S("operating_systems", "STRING"),
        S("device_model_ids", "STRING"), S("interest_category_ids", "STRING"),
        S("schedule_start_time", "STRING"), S("schedule_end_time", "STRING"),
        S("dayparting", "STRING"), S("create_time", "STRING"),
        S("modify_time", "STRING"), S("app_id", "STRING"),
        S("app_name", "STRING"), S("promotion_type", "STRING"),
        S("_ingested_at", "TIMESTAMP"),
    ],
    "tiktok_ads_dim": [
        S("ad_id", "STRING"), S("ad_name", "STRING"),
        S("adgroup_id", "STRING"), S("campaign_id", "STRING"),
        S("ad_format", "STRING"), S("ad_text", "STRING"),
        S("call_to_action", "STRING"), S("landing_page_url", "STRING"),
        S("display_name", "STRING"), S("status", "STRING"),
        S("create_time", "STRING"), S("modify_time", "STRING"),
        S("image_ids", "STRING"), S("video_id", "STRING"),
        S("_ingested_at", "TIMESTAMP"),
    ],
    "tiktok_apps_dim": [
        S("app_id", "STRING"), S("app_name", "STRING"),
        S("package_name", "STRING"),     # Android
        S("bundle_id", "STRING"),        # iOS
        S("platform", "STRING"),
        S("app_platform", "STRING"),
        S("download_url", "STRING"), S("category", "STRING"),
        S("rating", "FLOAT64"), S("status", "STRING"),
        S("create_time", "STRING"), S("modify_time", "STRING"),
        S("raw_payload_json", "STRING"),
        S("_ingested_at", "TIMESTAMP"),
    ],
    "tiktok_sync_log": [
        S("run_id", "STRING"), S("run_type", "STRING"),
        S("start_date", "DATE"), S("end_date", "DATE"),
        S("status", "STRING"), S("tables_synced", "INT64"),
        S("total_rows", "INT64"), S("error_message", "STRING"),
        S("duration_seconds", "FLOAT64"), S("_ingested_at", "TIMESTAMP"),
    ],
}

# =============================================================================
# HELPERS
# =============================================================================

def now_ts():
    # v3.1: utcnow() 3.12+ mein deprecated. Aware UTC BigQuery bhi theek parhta hai.
    return datetime.now(timezone.utc).isoformat()

def run_id_now():
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

def safe_float(v):
    if v is None or v == "" or v == "None":
        return None
    try:
        return float(v)
    except Exception:
        return None

def safe_str(v):
    if v is None:
        return None
    if isinstance(v, (list, dict)):
        return json.dumps(v)
    return str(v)

# =============================================================================
# API CLIENT
# =============================================================================

def api_get(endpoint, params, label="call"):
    """Make a GET request to TikTok API with retry."""
    headers = {"Access-Token": ACCESS_TOKEN}
    url = f"{BASE_URL}{endpoint}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=60)
            data = resp.json()

            if data.get("code") == 0:
                return data.get("data", {})
            elif data.get("code") == 40100:
                log.error(f"  [{label}] Auth error: {data.get('message')}")
                raise ValueError(f"Auth failed: {data.get('message')}")
            elif data.get("code") in (40002, 40003):
                wait = RETRY_BACKOFF * attempt
                log.warning(f"  [{label}] Rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            else:
                log.warning(f"  [{label}] API error {data.get('code')}: {data.get('message')}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF * attempt)
                    continue
                return {}

        except requests.exceptions.Timeout:
            log.warning(f"  [{label}] Timeout — attempt {attempt}/{MAX_RETRIES}")
            time.sleep(RETRY_BACKOFF * attempt)
        except Exception as e:
            log.warning(f"  [{label}] Error: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF * attempt)
            else:
                raise

    return {}


def api_get_paginated(endpoint, params, items_key, label="call"):
    """v3: adhoori list KABHI wapas nahi jayegi.

    v2 mein page 2+ khali aane pe chup-chaap `break` — aur dim tables
    truncate=True se load hoti hain, yani baqi sab uda deti.
    """
    all_items = []
    page = 1
    declared_total = None

    while True:
        params["page"] = page
        params["page_size"] = PAGE_SIZE
        data = api_get(endpoint, params, label=f"{label}_p{page}")

        items = data.get(items_key) or data.get("list") or []
        if declared_total is None:
            declared_total = data.get("page_info", {}).get("total_number")

        if not items:
            if page == 1:
                break                                    # sach mein khali
            raise RuntimeError(
                f"[{label}] page {page} returned nothing but total says "
                f"{declared_total} — refusing to return a partial list "
                f"(TRUNCATE would wipe the rest).")

        all_items.extend(items)
        if declared_total and len(all_items) >= int(declared_total):
            break
        page += 1
        time.sleep(0.2)

    # 🛡️ server ne jitna kaha, utna aaya?
    if declared_total is not None and len(all_items) != int(declared_total):
        raise RuntimeError(
            f"[{label}] pagination mismatch: collected {len(all_items)} "
            f"!= declared total {declared_total}")
    return all_items


def fetch_report(report_type, data_level, dimensions, metrics, start_date, end_date, label="report"):
    """Fetch a report with pagination.

    🛡️ v3.1 FIX D — v3 mein yahan yeh tha:
           total = data.get("page_info", {}).get("total_number", 0)
           if len(all_rows) >= total: break
       `total_number` gayab hone par default 0 ban jata tha, aur
       `len(all_rows) >= 0` HAMESHA sach hota hai — yani page 1 ke baad
       chup-chaap break, aur usi adhoore set pe delete+load. Ab:
         - total maloom      -> usi tak paging, aakhir mein exact match lazmi
         - total na maloom   -> short page (< PAGE_SIZE) aane tak paging
         - beech mein khali  -> agar total kehta hai aur aana chahiye to RAISE
    """
    all_rows = []
    page = 1
    declared_total = None

    while True:
        params = {
            "advertiser_id": ADVERTISER_ID,
            "report_type": report_type,
            "data_level": data_level,
            "dimensions": json.dumps(dimensions),
            "metrics": json.dumps(metrics),
            "start_date": str(start_date),
            "end_date": str(end_date),
            "page": page,
            "page_size": PAGE_SIZE,
            "query_lifetime": False,
        }

        data = api_get("/report/integrated/get/", params, label=f"{label}_p{page}")
        rows = data.get("list", [])

        if declared_total is None:
            declared_total = data.get("page_info", {}).get("total_number")

        if not rows:
            # Natural end of pagination — sirf tab alarming hai jab server ne
            # khud kaha ho ke aur rows hain.
            if declared_total is not None and len(all_rows) < int(declared_total):
                raise RuntimeError(
                    f"[{label}] page {page} returned nothing but total says "
                    f"{declared_total} (collected {len(all_rows)}) — refusing "
                    f"a partial report; delete+load would drop the rest.")
            break

        all_rows.extend(rows)

        if declared_total is not None:
            if len(all_rows) >= int(declared_total):
                break
        elif len(rows) < PAGE_SIZE:
            break                                   # short page = last page

        page += 1
        time.sleep(0.3)

    if declared_total is not None and len(all_rows) != int(declared_total):
        raise RuntimeError(
            f"[{label}] pagination mismatch: collected {len(all_rows)} "
            f"!= declared total {declared_total}")

    return all_rows

# =============================================================================
# REPORT PARSERS
# =============================================================================

def parse_report_rows(raw_rows, extra_dim_keys, run_id):
    ts = now_ts()
    parsed = []

    for item in raw_rows:
        dims = item.get("dimensions", {})
        mets = item.get("metrics", {})

        row = {
            "report_date": dims.get("stat_time_day", "")[:10] if dims.get("stat_time_day") else None,
            "advertiser_id": ADVERTISER_ID,
            "run_id": run_id,
            "_ingested_at": ts,
        }

        for key in extra_dim_keys:
            row[key] = safe_str(dims.get(key))

        all_metric_names = list(set(BASIC_METRICS + AUDIENCE_METRICS))
        for m in all_metric_names:
            row[m] = safe_float(mets.get(m))

        for name_field in ["campaign_name", "adgroup_name", "ad_name"]:
            if name_field not in row or row.get(name_field) is None:
                row[name_field] = safe_str(mets.get(name_field))

        parsed.append(row)

    return parsed

# =============================================================================
# DIMENSION FETCHERS
# =============================================================================

def fetch_campaigns():
    log.info("Fetching campaigns...")
    ts = now_ts()
    items = api_get_paginated("/campaign/get/", {"advertiser_id": ADVERTISER_ID},
                              "list", label="campaigns")
    rows = [{
        "campaign_id": safe_str(c.get("campaign_id")),
        "campaign_name": c.get("campaign_name"),
        "objective_type": c.get("objective_type"),
        "budget": safe_float(c.get("budget")),
        "budget_mode": c.get("budget_mode"),
        "campaign_type": c.get("campaign_type"),
        "status": c.get("status") or c.get("operation_status"),
        "create_time": c.get("create_time"),
        "modify_time": c.get("modify_time"),
        "is_smart_performance_campaign": c.get("is_smart_performance_campaign", False),
        "_ingested_at": ts,
    } for c in items]
    log.info(f"  ✓ {len(rows)} campaigns")
    return rows


def fetch_adgroups():
    log.info("Fetching ad groups...")
    ts = now_ts()
    items = api_get_paginated("/adgroup/get/", {"advertiser_id": ADVERTISER_ID},
                              "list", label="adgroups")
    rows = [{
        "adgroup_id": safe_str(a.get("adgroup_id")),
        "adgroup_name": a.get("adgroup_name"),
        "campaign_id": safe_str(a.get("campaign_id")),
        "placement_type": a.get("placement_type"),
        "placements": safe_str(a.get("placements")),
        "bid_type": a.get("bid_type"),
        "bid_price": safe_float(a.get("bid_price") or a.get("bid")),
        "budget": safe_float(a.get("budget")),
        "budget_mode": a.get("budget_mode"),
        "optimization_goal": a.get("optimization_goal"),
        "billing_event": a.get("billing_event"),
        "pacing": a.get("pacing"),
        "status": a.get("status") or a.get("operation_status"),
        "age_groups": safe_str(a.get("age_groups") or a.get("age")),
        "gender": a.get("gender"),
        "location_ids": safe_str(a.get("location_ids") or a.get("location")),
        "languages": safe_str(a.get("languages")),
        "operating_systems": safe_str(a.get("operating_systems")),
        "device_model_ids": safe_str(a.get("device_model_ids")),
        "interest_category_ids": safe_str(a.get("interest_category_ids") or a.get("interest_category_v2")),
        "schedule_start_time": a.get("schedule_start_time"),
        "schedule_end_time": a.get("schedule_end_time"),
        "dayparting": safe_str(a.get("dayparting")),
        "create_time": a.get("create_time"),
        "modify_time": a.get("modify_time"),
        "app_id": safe_str(a.get("app_id")),
        "app_name": a.get("app_name"),
        "promotion_type": a.get("promotion_type"),
        "_ingested_at": ts,
    } for a in items]
    log.info(f"  ✓ {len(rows)} ad groups")
    return rows


def fetch_ads():
    log.info("Fetching ads...")
    ts = now_ts()
    items = api_get_paginated("/ad/get/", {"advertiser_id": ADVERTISER_ID},
                              "list", label="ads")
    rows = [{
        "ad_id": safe_str(a.get("ad_id")),
        "ad_name": a.get("ad_name"),
        "adgroup_id": safe_str(a.get("adgroup_id")),
        "campaign_id": safe_str(a.get("campaign_id")),
        "ad_format": a.get("ad_format"),
        "ad_text": a.get("ad_text"),
        "call_to_action": a.get("call_to_action"),
        "landing_page_url": a.get("landing_page_url"),
        "display_name": a.get("display_name"),
        "status": a.get("status") or a.get("operation_status"),
        "create_time": a.get("create_time"),
        "modify_time": a.get("modify_time"),
        "image_ids": safe_str(a.get("image_ids")),
        "video_id": safe_str(a.get("video_id")),
        "_ingested_at": ts,
    } for a in items]
    log.info(f"  ✓ {len(rows)} ads")
    return rows


def fetch_apps():
    """Fetch all apps registered for this advertiser via /app/list/.
    Bridge table: TikTok app_id → package_name (Android) / bundle_id (iOS).
    """
    log.info("Fetching apps...")
    ts = now_ts()
    items = api_get_paginated("/app/list/", {"advertiser_id": ADVERTISER_ID},
                              "list", label="apps")

    rows = []
    for a in items:
        package_name = (a.get("package_name") or a.get("android_package_name")
                        or a.get("package_id"))
        bundle_id    = (a.get("bundle_id") or a.get("ios_bundle_id")
                        or a.get("app_bundle_id"))
        platform     = (a.get("platform") or a.get("app_platform") or a.get("os"))

        rows.append({
            "app_id":           safe_str(a.get("app_id")),
            "app_name":         safe_str(a.get("app_name") or a.get("name")),
            "package_name":     safe_str(package_name),
            "bundle_id":        safe_str(bundle_id),
            "platform":         safe_str(platform),
            "app_platform":     safe_str(a.get("app_platform")),
            "download_url":     safe_str(a.get("download_url") or a.get("app_download_url")),
            "category":         safe_str(a.get("category") or a.get("app_category")),
            "rating":           safe_float(a.get("rating") or a.get("app_rating")),
            "status":           safe_str(a.get("status") or a.get("operation_status")),
            "create_time":      safe_str(a.get("create_time")),
            "modify_time":      safe_str(a.get("modify_time")),
            "raw_payload_json": json.dumps(a, default=str),
            "_ingested_at":     ts,
        })
    log.info(f"  ✓ {len(rows)} apps")
    return rows

# =============================================================================
# BIGQUERY
# =============================================================================

def get_bq():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GCP_CREDS_JSON),
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(project=GCP_PROJECT, credentials=creds, location=BQ_LOCATION)


def ensure_dataset(bq):
    ds_id = f"{GCP_PROJECT}.{BQ_DATASET}"
    try:
        bq.get_dataset(ds_id)
    except Exception:
        ds = bigquery.Dataset(ds_id)
        ds.location = BQ_LOCATION
        bq.create_dataset(ds)
        log.info(f"  Created dataset: {BQ_DATASET}")


def ensure_table(bq, name):
    ref = f"{GCP_PROJECT}.{BQ_DATASET}.{name}"
    try:
        bq.get_table(ref)
    except Exception:
        t = bigquery.Table(ref, schema=SCHEMAS[name])
        if name.startswith("tiktok_daily_") or name.startswith("tiktok_audience_"):
            t.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="report_date")
        bq.create_table(t)
        log.info(f"  Created table: {name}")


def load_rows(bq, table_name, rows, truncate=False):
    if not rows:
        log.warning(f"  ⚠️  No rows for {table_name} — nothing written")
        return 0

    ref = f"{GCP_PROJECT}.{BQ_DATASET}.{table_name}"
    cfg = bigquery.LoadJobConfig(
        schema=SCHEMAS[table_name],
        write_disposition=(
            bigquery.WriteDisposition.WRITE_TRUNCATE if truncate
            else bigquery.WriteDisposition.WRITE_APPEND
        ),
    )
    bq.load_table_from_json(rows, ref, job_config=cfg).result()
    log.info(f"  ✅ {len(rows):,} rows → {table_name}")
    return len(rows)


def delete_date_range(bq, table_name, start_date, end_date):
    """v3: fail ho to chillao — v2 ka caller `except: pass` karta tha."""
    ref = f"{GCP_PROJECT}.{BQ_DATASET}.{table_name}"
    bq.query(
        f"DELETE FROM `{ref}` WHERE report_date BETWEEN '{start_date}' AND '{end_date}'"
    ).result()

# =============================================================================
# REPORT DEFINITIONS
# =============================================================================

REPORTS = [
    {"table": "tiktok_daily_advertiser", "report_type": "BASIC",
     "data_level": "AUCTION_ADVERTISER", "dimensions": ["stat_time_day"],
     "metrics": BASIC_METRICS, "dim_keys": []},
    {"table": "tiktok_daily_campaign", "report_type": "BASIC",
     "data_level": "AUCTION_CAMPAIGN", "dimensions": ["stat_time_day", "campaign_id"],
     "metrics": BASIC_METRICS, "dim_keys": ["campaign_id"]},
    {"table": "tiktok_daily_adgroup", "report_type": "BASIC",
     "data_level": "AUCTION_ADGROUP", "dimensions": ["stat_time_day", "adgroup_id"],
     "metrics": BASIC_METRICS, "dim_keys": ["campaign_id", "adgroup_id"]},
    {"table": "tiktok_daily_ad", "report_type": "BASIC",
     "data_level": "AUCTION_AD", "dimensions": ["stat_time_day", "ad_id"],
     "metrics": BASIC_METRICS, "dim_keys": ["campaign_id", "adgroup_id", "ad_id"]},
    {"table": "tiktok_daily_country", "report_type": "BASIC",
     "data_level": "AUCTION_CAMPAIGN",
     "dimensions": ["stat_time_day", "campaign_id", "country_code"],
     "metrics": BASIC_METRICS, "dim_keys": ["campaign_id", "country_code"]},
    {"table": "tiktok_audience_gender", "report_type": "AUDIENCE",
     "data_level": "AUCTION_CAMPAIGN",
     "dimensions": ["stat_time_day", "campaign_id", "gender"],
     "metrics": AUDIENCE_METRICS, "dim_keys": ["campaign_id", "gender"]},
    {"table": "tiktok_audience_age", "report_type": "AUDIENCE",
     "data_level": "AUCTION_CAMPAIGN",
     "dimensions": ["stat_time_day", "campaign_id", "age"],
     "metrics": AUDIENCE_METRICS, "dim_keys": ["campaign_id", "age"]},
    {"table": "tiktok_audience_platform", "report_type": "AUDIENCE",
     "data_level": "AUCTION_CAMPAIGN",
     "dimensions": ["stat_time_day", "campaign_id", "platform"],
     "metrics": AUDIENCE_METRICS, "dim_keys": ["campaign_id", "platform"]},
    {"table": "tiktok_audience_language", "report_type": "AUDIENCE",
     "data_level": "AUCTION_CAMPAIGN",
     "dimensions": ["stat_time_day", "campaign_id", "language"],
     "metrics": AUDIENCE_METRICS, "dim_keys": ["campaign_id", "language"]},
]

# =============================================================================
# SYNC
# =============================================================================

def sync(days_back=3):
    rid = run_id_now()
    t0 = time.time()
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=days_back - 1)

    log.info(f"\n{'='*60}")
    log.info(f"TikTok Ads → BigQuery Full Sync (v3.1)")
    log.info(f"  run_id     : {rid}")
    log.info(f"  Date range : {start_date} → {end_date}")
    log.info(f"  Advertiser : {ADVERTISER_ID}")
    # RAW value bhi dikhao — agar variable pohancha hi nahi to yahan
    # '<unset>' ya '' dikhega, aur diagnosis ek nazar mein ho jayegi.
    log.info(f"  apps_dim   : {'WAIVER ON' if ALLOW_EMPTY_APPS_DIM else 'required'} "
             f"(ALLOW_EMPTY_APPS_DIM={os.environ.get('ALLOW_EMPTY_APPS_DIM', '<unset>')!r})")
    log.info(f"{'='*60}")

    bq = get_bq()
    ensure_dataset(bq)
    for name in SCHEMAS:
        ensure_table(bq, name)

    total_rows = 0
    tables_synced = 0
    error_msg = None
    status = "SUCCESS"
    apps_dim_waived = False          # v3.1: PARTIAL se alag rakha gaya

    try:
        # ── DIMENSION TABLES (truncate + reload) ──
        log.info("\n── Dimension Tables ──")
        # 🛡️ v3.1 FIX B: har kamyab load pe +1 — pehle andhadhund `+= 4` tha.
        total_rows += load_rows(bq, "tiktok_campaigns_dim", fetch_campaigns(), truncate=True)
        tables_synced += 1
        total_rows += load_rows(bq, "tiktok_adgroups_dim",  fetch_adgroups(),  truncate=True)
        tables_synced += 1
        total_rows += load_rows(bq, "tiktok_ads_dim",       fetch_ads(),       truncate=True)
        tables_synced += 1

        # 🛡️ FIX 1: apps_dim khali ho to CHILLAO. Ye bridge table hai —
        #    khali rehna manual mapping pe majboori paida karta hai.
        apps = fetch_apps()
        if not apps:
            msg = ("/app/list/ returned 0 apps — apps_dim bridge is DEAD. "
                   "TikTok spend now depends entirely on the manual "
                   "tiktok_app_mapping table. Check endpoint/permissions. "
                   "(Set ALLOW_EMPTY_APPS_DIM=1 to proceed knowingly.)")
            if ALLOW_EMPTY_APPS_DIM:
                log.error(f"🚨 {msg}")
                log.error("   → ALLOW_EMPTY_APPS_DIM=1 set hai: jaan-boojh ke "
                          "aage barh raha hoon. Spend attribution ab poori "
                          "tarah tiktok_app_mapping pe hai — "
                          "tiktok_unmapped_monitor rozana dekhein.")
                apps_dim_waived = True
                error_msg = (error_msg or "") + " | apps_dim empty (WAIVED)"
            else:
                # 🛡️ v3.1 FIX A — YAHAN `sys.exit(1)` tha. SystemExit
                # BaseException se aata hai, Exception se NAHI: neeche wala
                # `except Exception` use pakadta hi nahi tha, `status`
                # "SUCCESS" hi rehta tha, aur `finally` sync_log mein
                # status='SUCCESS' likh deta tha — ek FAILED run par.
                # Normal exception raise karo taake sach record ho.
                raise RuntimeError(msg)
        else:
            total_rows += load_rows(bq, "tiktok_apps_dim", apps, truncate=True)
            tables_synced += 1

        # ── REPORT TABLES ──
        log.info("\n── Report Tables ──")
        for report in REPORTS:
            table = report["table"]
            log.info(f"\nFetching {table}...")

            # 🛡️ FIX 2: PEHLE fetch, PHIR delete.
            raw  = fetch_report(
                report_type=report["report_type"],
                data_level=report["data_level"],
                dimensions=report["dimensions"],
                metrics=report["metrics"],
                start_date=start_date,
                end_date=end_date,
                label=table,
            )
            rows = parse_report_rows(raw, report["dim_keys"], rid)

            if not rows:
                log.error(f"🚨 {table}: API returned 0 rows for "
                          f"{start_date}..{end_date} — SKIPPING delete+load "
                          f"(existing data preserved).")
                error_msg = (error_msg or "") + f" | {table}: empty response"
                status = "PARTIAL"
                continue

            delete_date_range(bq, table, start_date, end_date)   # 🛡️ ab mehfooz
            total_rows += load_rows(bq, table, rows)
            tables_synced += 1
            time.sleep(1)

        # 🛡️ v3.1 FIX C: sirf apps_dim waive hua ho (aur koi report na tooti)
        #    to ye ek MAALOOM, qubool-shuda haalat hai — build red na ho,
        #    warna rozana red = alert fatigue = asli failure ignore.
        if apps_dim_waived and status == "SUCCESS":
            status = "SUCCESS_APPS_DIM_WAIVED"

    except Exception as e:
        status = "FAILED"
        error_msg = str(e)
        log.error(f"FATAL: {e}")
        raise

    finally:
        duration = time.time() - t0
        log.info(f"\n{'='*60}")
        log.info(f"Status: {status}")
        log.info(f"Tables synced: {tables_synced}")
        log.info(f"Total rows: {total_rows:,}")
        log.info(f"Duration: {duration:.1f}s")
        log.info(f"{'='*60}")

        try:
            load_rows(bq, "tiktok_sync_log", [{
                "run_id": rid, "run_type": "sync",
                "start_date": str(start_date), "end_date": str(end_date),
                "status": status, "tables_synced": tables_synced,
                "total_rows": total_rows, "error_message": error_msg,
                "duration_seconds": round(duration, 2),
                "_ingested_at": now_ts(),
            }])
        except Exception as e:
            log.warning(f"  Sync log write failed: {e}")

    # 🛡️ FIX 4 (v3.1 mein durust): "SUCCESS" jhoot na bole — magar
    #    jaan-boojh ke waive ki hui apps_dim ko failure na ginwao.
    if status not in ("SUCCESS", "SUCCESS_APPS_DIM_WAIVED"):
        log.error(f"🚨 Run finished with status={status} — some tables were "
                  f"NOT refreshed. See log above.")
        sys.exit(1)

    if apps_dim_waived:
        log.warning("🟡 Reports sab refresh ho gaye, lekin apps_dim KHALI hai "
                    "(waiver on). Ye asthai hona chahiye — /app/list/ ki "
                    "permission theek karwayein, warna naye app_id chup-chaap "
                    "unmapped rahenge.")


def backfill(start_str, end_str):
    """Backfill historical data in 30-day chunks."""
    rid = run_id_now()
    t0 = time.time()
    start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_str, "%Y-%m-%d").date()

    log.info(f"\n{'='*60}")
    log.info(f"TikTok Ads → BigQuery BACKFILL (v3.1)")
    log.info(f"  run_id     : {rid}")
    log.info(f"  Date range : {start_date} → {end_date}")
    log.info(f"{'='*60}")

    bq = get_bq()
    ensure_dataset(bq)
    for name in SCHEMAS:
        ensure_table(bq, name)

    log.info("\n── Dimension Tables ──")
    load_rows(bq, "tiktok_campaigns_dim", fetch_campaigns(), truncate=True)
    load_rows(bq, "tiktok_adgroups_dim",  fetch_adgroups(),  truncate=True)
    load_rows(bq, "tiktok_ads_dim",       fetch_ads(),       truncate=True)
    apps = fetch_apps()
    if apps:
        load_rows(bq, "tiktok_apps_dim", apps, truncate=True)
    else:
        log.error("🚨 /app/list/ returned 0 apps — apps_dim left untouched")

    total_rows = 0
    partial = False
    chunk_start = start_date

    while chunk_start <= end_date:
        chunk_end = min(chunk_start + timedelta(days=29), end_date)
        log.info(f"\n── Chunk: {chunk_start} → {chunk_end} ──")

        for report in REPORTS:
            table = report["table"]
            log.info(f"  Fetching {table}...")

            raw = fetch_report(
                report_type=report["report_type"],
                data_level=report["data_level"],
                dimensions=report["dimensions"],
                metrics=report["metrics"],
                start_date=chunk_start,
                end_date=chunk_end,
                label=table,
            )
            rows = parse_report_rows(raw, report["dim_keys"], rid)

            if not rows:
                log.error(f"  🚨 {table}: 0 rows for {chunk_start}..{chunk_end} "
                          f"— SKIPPING delete+load")
                partial = True
                continue

            delete_date_range(bq, table, chunk_start, chunk_end)
            total_rows += load_rows(bq, table, rows)
            time.sleep(1)

        chunk_start = chunk_end + timedelta(days=1)

    duration = time.time() - t0
    log.info(f"\n✅ Backfill complete: {total_rows:,} rows in {duration:.1f}s")

    try:
        load_rows(bq, "tiktok_sync_log", [{
            "run_id": rid, "run_type": "backfill",
            "start_date": str(start_date), "end_date": str(end_date),
            "status": "PARTIAL" if partial else "SUCCESS",
            "tables_synced": len(REPORTS) + 4,
            "total_rows": total_rows, "error_message": None,
            "duration_seconds": round(duration, 2),
            "_ingested_at": now_ts(),
        }])
    except Exception:
        pass

    if partial:
        sys.exit(1)

# =============================================================================
# CLI
# =============================================================================

def main():
    p = argparse.ArgumentParser(description="TikTok Ads → BigQuery sync (v3.1)")
    p.add_argument("--days", type=int, default=3, help="Days to sync (default: 3)")
    p.add_argument("--backfill-start", type=str, help="Backfill start YYYY-MM-DD")
    p.add_argument("--backfill-end", type=str, help="Backfill end YYYY-MM-DD")
    args = p.parse_args()

    missing = []
    if not ACCESS_TOKEN: missing.append("TIKTOK_ACCESS_TOKEN")
    if not ADVERTISER_ID: missing.append("TIKTOK_ADVERTISER_ID")
    if not GCP_PROJECT: missing.append("GCP_PROJECT_ID")
    if not GCP_CREDS_JSON: missing.append("GCP_CREDENTIALS_JSON")
    if missing:
        log.error(f"Missing env vars: {', '.join(missing)}")
        sys.exit(1)

    try:
        if args.backfill_start and args.backfill_end:
            backfill(args.backfill_start, args.backfill_end)
        else:
            sync(args.days)
    except SystemExit:
        raise
    except Exception as e:
        log.error(f"FATAL: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
