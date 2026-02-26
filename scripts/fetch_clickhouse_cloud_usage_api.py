"""
Backfill observability.cost_summary_daily a partir da API de usage cost do ClickHouse Cloud.
Uso: python scripts/fetch_clickhouse_cloud_usage_api.py --from 2025-01-01 --to 2025-02-09
Requer: CH_CLOUD_API_KEY, CH_CLOUD_API_SECRET, CH_HOST, CH_PORT, CH_USER, CH_PASSWORD.
"""
import argparse
import os
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

try:
    import clickhouse_connect
except ImportError:
    print("Instale: pip install clickhouse-connect pandas requests")
    sys.exit(1)


BASE_URL = "https://api.clickhouse.cloud/v1"
MAX_DAYS_PER_REQUEST = 31
DEFAULT_USD_TO_BRL = 5.22


def get_org_id(api_key: str, api_secret: str) -> str:
    r = requests.get(
        f"{BASE_URL}/organizations",
        auth=(api_key, api_secret),
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    orgs = data.get("result") or []
    if not orgs:
        raise SystemExit("Nenhuma organização encontrada para esta API key")
    return orgs[0]["id"]


def fetch_usage_cost(
    org_id: str,
    from_date: str,
    to_date: str,
    api_key: str,
    api_secret: str,
) -> list:
    r = requests.get(
        f"{BASE_URL}/organizations/{org_id}/usageCost",
        params={"from_date": from_date, "to_date": to_date},
        auth=(api_key, api_secret),
        timeout=30,
    )
    r.raise_for_status()
    return (r.json().get("result") or {}).get("costs") or []


def date_range_chunks(from_dt: datetime, to_dt: datetime):
    cur = from_dt
    while cur <= to_dt:
        end = min(cur + timedelta(days=MAX_DAYS_PER_REQUEST - 1), to_dt)
        yield cur.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
        cur = end + timedelta(days=1)


def aggregate_costs_by_date(records: list) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    rows = []
    for rec in records:
        d = rec.get("date")
        if not d:
            continue
        metrics = rec.get("metrics") or {}
        total = float(rec.get("totalCHC") or 0)
        storage = float(metrics.get("storageCHC") or 0)
        compute = float(metrics.get("computeCHC") or 0)
        rows.append({"date": d, "totalCHC": total, "storageCHC": storage, "computeCHC": compute})
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    agg = df.groupby("date", as_index=False).agg(
        total_usd=("totalCHC", "sum"),
        storage_usd=("storageCHC", "sum"),
        compute_usd=("computeCHC", "sum"),
    )
    agg = agg.rename(columns={"date": "cost_date"})
    return agg


def build_summary_df(agg: pd.DataFrame, usd_to_brl: float) -> pd.DataFrame:
    if agg.empty:
        return agg
    df = agg.copy()
    df["cost_date"] = pd.to_datetime(df["cost_date"]).dt.date
    df["storage_brl"] = (df["storage_usd"].fillna(0) * usd_to_brl).astype(float)
    df["compute_brl"] = (df["compute_usd"].fillna(0) * usd_to_brl).astype(float)
    df["total_brl"] = (df["total_usd"].fillna(0) * usd_to_brl).astype(float)
    df["total_tables"] = 0
    df["total_rows"] = 0
    df["total_compressed_gb"] = 0.0
    df["total_queries"] = 0
    df["total_read_gb"] = 0.0
    df["usd_to_brl_rate"] = usd_to_brl
    summary = df[
        [
            "cost_date",
            "total_tables",
            "total_rows",
            "total_compressed_gb",
            "storage_usd",
            "storage_brl",
            "compute_usd",
            "compute_brl",
            "total_usd",
            "total_brl",
            "total_queries",
            "total_read_gb",
            "usd_to_brl_rate",
        ]
    ].copy()
    summary["total_queries"] = summary["total_queries"].astype("uint64")
    return summary


def get_ch_client():
    host = os.environ.get("CH_HOST")
    port = int(os.environ.get("CH_PORT", "8443"))
    user = os.environ.get("CH_USER", "default")
    password = os.environ.get("CH_PASSWORD", "")
    if not host:
        raise SystemExit("Defina CH_HOST (e CH_PORT, CH_USER, CH_PASSWORD)")
    return clickhouse_connect.get_client(
        host=host, port=port, username=user, password=password, secure=True
    )


def main():
    parser = argparse.ArgumentParser(description="Fetch ClickHouse Cloud usage cost and backfill cost_summary_daily")
    parser.add_argument("--from", dest="from_date", required=True, help="Data inicial YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", required=True, help="Data final YYYY-MM-DD (inclusiva)")
    args = parser.parse_args()

    api_key = os.environ.get("CH_CLOUD_API_KEY")
    api_secret = os.environ.get("CH_CLOUD_API_SECRET")
    if not api_key or not api_secret:
        raise SystemExit("Defina CH_CLOUD_API_KEY e CH_CLOUD_API_SECRET (console: Organization -> API Keys)")

    org_id = os.environ.get("CH_CLOUD_ORGANIZATION_ID")
    if not org_id:
        org_id = get_org_id(api_key, api_secret)
        print(f"Organização: {org_id}")

    from_dt = datetime.strptime(args.from_date, "%Y-%m-%d")
    to_dt = datetime.strptime(args.to_date, "%Y-%m-%d")
    if from_dt > to_dt:
        raise SystemExit("from_date deve ser <= to_date")

    usd_to_brl = float(os.environ.get("USD_TO_BRL", DEFAULT_USD_TO_BRL))
    all_records = []
    for f, t in date_range_chunks(from_dt, to_dt):
        records = fetch_usage_cost(org_id, f, t, api_key, api_secret)
        all_records.extend(records)
        time.sleep(1.2)

    agg = aggregate_costs_by_date(all_records)
    if agg.empty:
        print("Nenhum registro de custo retornado pela API para o período.")
        return
    summary = build_summary_df(agg, usd_to_brl)
    client = get_ch_client()
    client.insert_df("observability.cost_summary_daily", summary)
    print(f"Backfill concluído: {len(summary)} linhas em observability.cost_summary_daily")


if __name__ == "__main__":
    main()
