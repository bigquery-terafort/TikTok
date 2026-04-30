"""
TikTok Ads → BigQuery Full Sync (v2)
====================================
Extracts ALL available data from TikTok Marketing API v1.3:
  - 9 report tables (daily performance at every level + audience breakdowns)
  - 4 dimension tables (campaigns, adgroups, ads, APPS)
  - 1 sync log table

CHANGES IN v2:
  - Added tiktok_apps_dim table fetched from /app/list/ endpoint
  - Returns package_name (Android) and bundle_id (iOS) per app_id
  - This is the bridge to terafort.cross_platform.app_master_v2

Auth: Long-lived access token (doesn't expire)
Rate limit: 600 requests/minute
Max date range: 365 days per request
Pagination: page_size up to 1000
"""

import os
import sys
import json
import time
import argparse
import logging
import requests
from datetime import datetime, timedelta, date
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
LOOKBACK_DAYS   = int(os.environ.get("TIKTOK_LOOKBACK_DAYS", "3"))

BASE_URL = "https://business-api.tiktok.com/open_api/v1.3"

MAX_RETRIES   = 4
RETRY_BACKOFF = 5
PAGE_SIZE     = 1000

# =============================================================================
# ALL METRICS — every single metric TikTok API supports for BASIC reports
# =============================================================================

BASIC_METRICS = [
    # Spend & delivery
    "spend", "impressions", "clicks", "reach",
    "ctr", "cpc", "cpm", "cost_per_1000_reached", "frequency",
    # Conversions
    "conversion", "cost_per_conversion", "conversion_rate",
    "real_time_conversion", "real_time_cost_per_conversion", "real_time_conversion_rate",
    "result", "cost_per_result", "result_rate",
    "real_time_result", "real_time_cost_per_result", "real_time_result_rate",
    # Video
    "video_play_actions", "video_watched_2s", "video_watched_6s",
    "average_video_play", "average_video_play_per_user",
    "video_views_p25", "video_views_p50", "video_views_p75", "video_views_p100",
    # Engagement
    "engaged_view", "engaged_view_15s",
    "likes", "comments", "shares", "follows", "profile_visits",
    # App events
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

# Common report fields
def _report_schema(extra_dims=None):
    fields = [
        S("report_date", "DATE"),
        S("advertiser_id", "STRING"),
        S("run_id", "STRING"),
        S("_ingested_at", "TIMESTAMP"),
    ]
    if extra_dims:
        fields.extend(extra_dims)
    # All metrics as FLOAT (some are counts, some are rates — FLOAT handles both)
    all_metrics = list(set(BASIC_METRICS + AUDIENCE_METRICS))
    for m in sorted(all_metrics):
        fields.append(S(m, "FLOAT64"))
    # Add name fields for IDs
    for name_field in ["campaign_name", "adgroup_name", "ad_name"]:
        if not any(f.name == name_field for f in fields):
            fields.append(S(name_field, "STRING"))
    return fields

SCHEMAS = {
    "tiktok_daily_advertiser": _report_schema(),

    "tiktok_daily_campaign": _report_schema([
        S("campaign_id", "STRING"),
        S("campaign_name", "STRING"),
    ]),

    "tiktok_daily_adgroup": _report_schema([
        S("campaign_id", "STRING"),
        S("adgroup_id", "STRING"),
        S("adgroup_name", "STRING"),
    ]),

    "tiktok_daily_ad": _report_schema([
        S("campaign_id", "STRING"),
        S("adgroup_id", "STRING"),
        S("ad_id", "STRING"),
        S("ad_name", "STRING"),
    ]),

    "tiktok_daily_country": _report_schema([
        S("campaign_id", "STRING"),
        S("country_code", "STRING"),
    ]),

    "tiktok_audience_gender": _report_schema([
        S("campaign_id", "STRING"),
        S("gender", "STRING"),
    ]),

    "tiktok_audience_age": _report_schema([
        S("campaign_id", "STRING"),
        S("age", "STRING"),
    ]),

    "tiktok_audience_platform": _report_schema([
        S("campaign_id", "STRING"),
        S("platform", "STRING"),
    ]),

    "tiktok_audience_language": _report_schema([
        S("campaign_id", "STRING"),
        S("language", "STRING"),
    ]),

    "tiktok_campaigns_dim": [
        S("campaign_id", "STRING"),
        S("campaign_name", "STRING"),
        S("objective_type", "STRING"),
        S("budget", "FLOAT64"),
        S("budget_mode", "STRING"),
        S("campaign_type", "STRING"),
        S("status", "STRING"),
        S("create_time", "STRING"),
        S("modify_time", "STRING"),
        S("is_smart_performance_campaign", "BOOLEAN"),
        S("_ingested_at", "TIMESTAMP"),
    ],

    "tiktok_adgroups_dim": [
        S("adgroup_id", "STRING"),
        S("adgroup_name", "STRING"),
        S("campaign_id", "STRING"),
        S("placement_type", "STRING"),
        S("placements", "STRING"),
        S("bid_type", "STRING"),
        S("bid_price", "FLOAT64"),
        S("budget", "FLOAT64"),
        S("budget_mode", "STRING"),
        S("optimization_goal", "STRING"),
        S("billing_event", "STRING"),
        S("pacing", "STRING"),
        S("status", "STRING"),
        S("age_groups", "STRING"),
        S("gender", "STRING"),
        S("location_ids", "STRING"),
        S("languages", "STRING"),
        S("operating_systems", "STRING"),
        S("device_model_ids", "STRING"),
        S("interest_category_ids", "STRING"),
        S("schedule_start_time", "STRING"),
        S("schedule_end_time", "STRING"),
        S("dayparting", "STRING"),
        S("create_time", "STRING"),
        S("modify_time", "STRING"),
        S("app_id", "STRING"),
        S("app_name", "STRING"),
        S("promotion_type", "STRING"),
        S("_ingested_at", "TIMESTAMP"),
    ],

    "tiktok_ads_dim": [
        S("ad_id", "STRING"),
        S("ad_name", "STRING"),
        S("adgroup_id", "STRING"),
        S("campaign_id", "STRING"),
        S("ad_format", "STRING"),
        S("ad_text", "STRING"),
        S("call_to_action", "STRING"),
        S("landing_page_url", "STRING"),
        S("display_name", "STRING"),
        S("status", "STRING"),
        S("create_time", "STRING"),
        S("modify_time", "STRING"),
        S("image_ids", "STRING"),
        S("video_id", "STRING"),
        S("_ingested_at", "TIMESTAMP"),
    ],

    # NEW IN v2: bridge table from TikTok app_id → package_name / bundle_id
    "tiktok_apps_dim": [
        S("app_id", "STRING"),
        S("app_name", "STRING"),
        S("package_name", "STRING"),     # Android
        S("bundle_id", "STRING"),        # iOS
        S("platform", "STRING"),         # 'ANDROID' / 'IOS' (canonical)
        S("app_platform", "STRING"),     # alternate field name (some API versions)
        S("download_url", "STRING"),
        S("category", "STRING"),
        S("rating", "FLOAT64"),
        S("status", "STRING"),
        S("create_time", "STRING"),
        S("modify_time", "STRING"),
        S("raw_payload_json", "STRING"), # Full API response — useful for debugging field names
        S("_ingested_at", "TIMESTAMP"),
    ],

    "tiktok_sync_log": [
        S("run_id", "STRING"),
        S("run_type", "STRING"),
        S("start_date", "DATE"),
        S("end_date", "DATE"),
        S("status", "STRING"),
        S("tables_synced", "INT64"),
        S("total_rows", "INT64"),
        S("error_message", "STRING"),
        S("duration_seconds", "FLOAT64"),
        S("_ingested_at", "TIMESTAMP"),
    ],
}

# =============================================================================
# HELPERS
# =============================================================================

def now_ts():
    return datetime.utcnow().isoformat()

def run_id_now():
    return datetime.utcnow().strftime("%Y%m%d%H%M%S")

def safe_float(v):
    if v is None or v == "" or v == "None":
        return None
    try:
        return float(v)
    except:
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
                # Rate limit
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
    """Paginate through all results."""
    all_items = []
    page = 1

    while True:
        params["page"] = page
        params["page_size"] = PAGE_SIZE
        data = api_get(endpoint, params, label=f"{label}_p{page}")

        items = data.get(items_key) or data.get("list") or []
        if not items:
            break

        all_items.extend(items)

        total = data.get("page_info", {}).get("total_number", 0)
        if len(all_items) >= total:
            break

        page += 1
        time.sleep(0.2)

    return all_items


def fetch_report(report_type, data_level, dimensions, metrics, start_date, end_date, label="report"):
    """Fetch a report with pagination."""
    all_rows = []
    page = 1

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
        if not rows:
            break

        all_rows.extend(rows)

        total = data.get("page_info", {}).get("total_number", 0)
        if len(all_rows) >= total:
            break

        page += 1
        time.sleep(0.3)

    return all_rows


# =============================================================================
# REPORT PARSERS
# =============================================================================

def parse_report_rows(raw_rows, extra_dim_keys, run_id):
    """Parse TikTok report rows into BQ-ready dicts."""
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

        # Add dimension values
        for key in extra_dim_keys:
            row[key] = safe_str(dims.get(key))

        # Add all metrics
        all_metric_names = list(set(BASIC_METRICS + AUDIENCE_METRICS))
        for m in all_metric_names:
            row[m] = safe_float(mets.get(m))

        # Add name fields from metrics (TikTok puts names in metrics, not dimensions)
        for name_field in ["campaign_name", "adgroup_name", "ad_name"]:
            if name_field not in row or row.get(name_field) is None:
                row[name_field] = safe_str(mets.get(name_field))

        parsed.append(row)

    return parsed


# =============================================================================
# DIMENSION FETCHERS
# =============================================================================

def fetch_campaigns():
    """Fetch all campaigns."""
    log.info("Fetching campaigns...")
    ts = now_ts()
    items = api_get_paginated(
        "/campaign/get/",
        {"advertiser_id": ADVERTISER_ID},
        "list",
        label="campaigns"
    )
    rows = []
    for c in items:
        rows.append({
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
        })
    log.info(f"  ✓ {len(rows)} campaigns")
    return rows


def fetch_adgroups():
    """Fetch all ad groups."""
    log.info("Fetching ad groups...")
    ts = now_ts()
    items = api_get_paginated(
        "/adgroup/get/",
        {"advertiser_id": ADVERTISER_ID},
        "list",
        label="adgroups"
    )
    rows = []
    for a in items:
        rows.append({
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
        })
    log.info(f"  ✓ {len(rows)} ad groups")
    return rows


def fetch_ads():
    """Fetch all ads."""
    log.info("Fetching ads...")
    ts = now_ts()
    items = api_get_paginated(
        "/ad/get/",
        {"advertiser_id": ADVERTISER_ID},
        "list",
        label="ads"
    )
    rows = []
    for a in items:
        rows.append({
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
        })
    log.info(f"  ✓ {len(rows)} ads")
    return rows


def fetch_apps():
    """Fetch all apps registered for this advertiser via /app/list/.
    Returns app metadata including package_name (Android) and bundle_id (iOS).
    This is the bridge table from TikTok app_id → app store identifier.
    """
    log.info("Fetching apps...")
    ts = now_ts()

    # /app/list/ uses pagination but the response shape is slightly different
    # from campaign/adgroup/ad endpoints. We try standard pagination first;
    # if total_number isn't returned (some endpoints don't), we read one page.
    items = api_get_paginated(
        "/app/list/",
        {"advertiser_id": ADVERTISER_ID},
        "list",
        label="apps"
    )

    rows = []
    for a in items:
        # TikTok's /app/list/ response field names vary slightly across API
        # versions. We try multiple known names and store the raw payload
        # for forensics.
        package_name = (
            a.get("package_name")
            or a.get("android_package_name")
            or a.get("package_id")
        )
        bundle_id = (
            a.get("bundle_id")
            or a.get("ios_bundle_id")
            or a.get("app_bundle_id")
        )
        platform = (
            a.get("platform")
            or a.get("app_platform")
            or a.get("os")
        )

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
                type_=bigquery.TimePartitioningType.DAY,
                field="report_date"
            )
        bq.create_table(t)
        log.info(f"  Created table: {name}")


def load_rows(bq, table_name, rows, truncate=False):
    if not rows:
        log.info(f"  No rows for {table_name}")
        return 0

    ref = f"{GCP_PROJECT}.{BQ_DATASET}.{table_name}"
    cfg = bigquery.LoadJobConfig(
        schema=SCHEMAS[table_name],
        write_disposition=(
            bigquery.WriteDisposition.WRITE_TRUNCATE if truncate
            else bigquery.WriteDisposition.WRITE_APPEND
        ),
    )

    job = bq.load_table_from_json(rows, ref, job_config=cfg)
    job.result()
    log.info(f"  ✅ {len(rows):,} rows → {table_name}")
    return len(rows)


def delete_date_range(bq, table_name, start_date, end_date):
    ref = f"{GCP_PROJECT}.{BQ_DATASET}.{table_name}"
    bq.query(
        f"DELETE FROM `{ref}` WHERE report_date BETWEEN '{start_date}' AND '{end_date}'"
    ).result()


# =============================================================================
# REPORT DEFINITIONS
# =============================================================================

REPORTS = [
    {
        "table": "tiktok_daily_advertiser",
        "report_type": "BASIC",
        "data_level": "AUCTION_ADVERTISER",
        "dimensions": ["stat_time_day"],
        "metrics": BASIC_METRICS,
        "dim_keys": [],
    },
    {
        "table": "tiktok_daily_campaign",
        "report_type": "BASIC",
        "data_level": "AUCTION_CAMPAIGN",
        "dimensions": ["stat_time_day", "campaign_id"],
        "metrics": BASIC_METRICS,
        "dim_keys": ["campaign_id"],
    },
    {
        "table": "tiktok_daily_adgroup",
        "report_type": "BASIC",
        "data_level": "AUCTION_ADGROUP",
        "dimensions": ["stat_time_day", "adgroup_id"],
        "metrics": BASIC_METRICS,
        "dim_keys": ["campaign_id", "adgroup_id"],
    },
    {
        "table": "tiktok_daily_ad",
        "report_type": "BASIC",
        "data_level": "AUCTION_AD",
        "dimensions": ["stat_time_day", "ad_id"],
        "metrics": BASIC_METRICS,
        "dim_keys": ["campaign_id", "adgroup_id", "ad_id"],
    },
    {
        "table": "tiktok_daily_country",
        "report_type": "BASIC",
        "data_level": "AUCTION_CAMPAIGN",
        "dimensions": ["stat_time_day", "campaign_id", "country_code"],
        "metrics": BASIC_METRICS,
        "dim_keys": ["campaign_id", "country_code"],
    },
    {
        "table": "tiktok_audience_gender",
        "report_type": "AUDIENCE",
        "data_level": "AUCTION_CAMPAIGN",
        "dimensions": ["stat_time_day", "campaign_id", "gender"],
        "metrics": AUDIENCE_METRICS,
        "dim_keys": ["campaign_id", "gender"],
    },
    {
        "table": "tiktok_audience_age",
        "report_type": "AUDIENCE",
        "data_level": "AUCTION_CAMPAIGN",
        "dimensions": ["stat_time_day", "campaign_id", "age"],
        "metrics": AUDIENCE_METRICS,
        "dim_keys": ["campaign_id", "age"],
    },
    {
        "table": "tiktok_audience_platform",
        "report_type": "AUDIENCE",
        "data_level": "AUCTION_CAMPAIGN",
        "dimensions": ["stat_time_day", "campaign_id", "platform"],
        "metrics": AUDIENCE_METRICS,
        "dim_keys": ["campaign_id", "platform"],
    },
    {
        "table": "tiktok_audience_language",
        "report_type": "AUDIENCE",
        "data_level": "AUCTION_CAMPAIGN",
        "dimensions": ["stat_time_day", "campaign_id", "language"],
        "metrics": AUDIENCE_METRICS,
        "dim_keys": ["campaign_id", "language"],
    },
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
    log.info(f"TikTok Ads → BigQuery Full Sync (v2 with apps_dim)")
    log.info(f"  run_id     : {rid}")
    log.info(f"  Date range : {start_date} → {end_date}")
    log.info(f"  Advertiser : {ADVERTISER_ID}")
    log.info(f"{'='*60}")

    bq = get_bq()
    ensure_dataset(bq)
    for name in SCHEMAS:
        ensure_table(bq, name)

    total_rows = 0
    tables_synced = 0
    error_msg = None
    status = "SUCCESS"

    try:
        # ── DIMENSION TABLES (truncate + reload) ──
        log.info("\n── Dimension Tables ──")
        total_rows += load_rows(bq, "tiktok_campaigns_dim", fetch_campaigns(), truncate=True)
        total_rows += load_rows(bq, "tiktok_adgroups_dim", fetch_adgroups(), truncate=True)
        total_rows += load_rows(bq, "tiktok_ads_dim", fetch_ads(), truncate=True)
        total_rows += load_rows(bq, "tiktok_apps_dim", fetch_apps(), truncate=True)  # NEW IN v2
        tables_synced += 4

        # ── REPORT TABLES (delete date range + append) ──
        log.info("\n── Report Tables ──")
        for report in REPORTS:
            table = report["table"]
            log.info(f"\nFetching {table}...")

            # Delete existing data for date range
            try:
                delete_date_range(bq, table, start_date, end_date)
            except Exception:
                pass  # Table might be empty

            # Fetch report
            raw = fetch_report(
                report_type=report["report_type"],
                data_level=report["data_level"],
                dimensions=report["dimensions"],
                metrics=report["metrics"],
                start_date=start_date,
                end_date=end_date,
                label=table,
            )

            # Parse and load
            rows = parse_report_rows(raw, report["dim_keys"], rid)
            n = load_rows(bq, table, rows)
            total_rows += n
            tables_synced += 1

            time.sleep(1)  # Respect rate limits

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

        # Write sync log
        try:
            load_rows(bq, "tiktok_sync_log", [{
                "run_id": rid,
                "run_type": "sync",
                "start_date": str(start_date),
                "end_date": str(end_date),
                "status": status,
                "tables_synced": tables_synced,
                "total_rows": total_rows,
                "error_message": error_msg,
                "duration_seconds": round(duration, 2),
                "_ingested_at": now_ts(),
            }])
        except Exception as e:
            log.warning(f"  Sync log write failed: {e}")


def backfill(start_str, end_str):
    """Backfill historical data in 30-day chunks."""
    rid = run_id_now()
    t0 = time.time()
    start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_str, "%Y-%m-%d").date()

    log.info(f"\n{'='*60}")
    log.info(f"TikTok Ads → BigQuery BACKFILL (v2 with apps_dim)")
    log.info(f"  run_id     : {rid}")
    log.info(f"  Date range : {start_date} → {end_date}")
    log.info(f"{'='*60}")

    bq = get_bq()
    ensure_dataset(bq)
    for name in SCHEMAS:
        ensure_table(bq, name)

    # Dimension tables first
    log.info("\n── Dimension Tables ──")
    load_rows(bq, "tiktok_campaigns_dim", fetch_campaigns(), truncate=True)
    load_rows(bq, "tiktok_adgroups_dim", fetch_adgroups(), truncate=True)
    load_rows(bq, "tiktok_ads_dim", fetch_ads(), truncate=True)
    load_rows(bq, "tiktok_apps_dim", fetch_apps(), truncate=True)  # NEW IN v2

    # Reports in 30-day chunks
    total_rows = 0
    chunk_start = start_date

    while chunk_start <= end_date:
        chunk_end = min(chunk_start + timedelta(days=29), end_date)
        log.info(f"\n── Chunk: {chunk_start} → {chunk_end} ──")

        for report in REPORTS:
            table = report["table"]
            log.info(f"  Fetching {table}...")

            try:
                delete_date_range(bq, table, chunk_start, chunk_end)
            except Exception:
                pass

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
            total_rows += load_rows(bq, table, rows)
            time.sleep(1)

        chunk_start = chunk_end + timedelta(days=1)

    duration = time.time() - t0
    log.info(f"\n✅ Backfill complete: {total_rows:,} rows in {duration:.1f}s")

    try:
        load_rows(bq, "tiktok_sync_log", [{
            "run_id": rid,
            "run_type": "backfill",
            "start_date": str(start_date),
            "end_date": str(end_date),
            "status": "SUCCESS",
            "tables_synced": len(REPORTS) + 4,  # +4 dim tables (was +3 in v1)
            "total_rows": total_rows,
            "error_message": None,
            "duration_seconds": round(duration, 2),
            "_ingested_at": now_ts(),
        }])
    except Exception:
        pass


# =============================================================================
# CLI
# =============================================================================

def main():
    p = argparse.ArgumentParser(description="TikTok Ads → BigQuery sync (v2)")
    p.add_argument("--days", type=int, default=3, help="Days to sync (default: 3)")
    p.add_argument("--backfill-start", type=str, help="Backfill start YYYY-MM-DD")
    p.add_argument("--backfill-end", type=str, help="Backfill end YYYY-MM-DD")
    args = p.parse_args()

    # Validate config
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
    except Exception as e:
        log.error(f"FATAL: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
