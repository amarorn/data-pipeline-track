"""
Backfill observability.cost_summary_daily a partir de CSV de faturas passadas.
Uso: python scripts/backfill_clickhouse_cost_history.py path/to/cost_history.csv
Requer: CH_HOST, CH_PORT, CH_USER, CH_PASSWORD (e opcional USD_TO_BRL).
"""
import os
import sys
from datetime import date

import pandas as pd

try:
    import clickhouse_connect
except ImportError:
    print("Instale: pip install clickhouse-connect pandas")
    sys.exit(1)


REQUIRED_COLS = {"cost_date", "total_usd"}
OPTIONAL_COLS = {"storage_usd", "compute_usd", "total_queries", "total_read_gb"}
DEFAULT_USD_TO_BRL = 5.22


def get_client():
    host = os.environ.get("CH_HOST")
    port = int(os.environ.get("CH_PORT", "8443"))
    user = os.environ.get("CH_USER", "default")
    password = os.environ.get("CH_PASSWORD", "")
    if not host:
        raise SystemExit("Defina CH_HOST (e CH_PORT, CH_USER, CH_PASSWORD)")
    return clickhouse_connect.get_client(
        host=host, port=port, username=user, password=password, secure=True
    )


def normalize_date(ser):
    if pd.api.types.is_dat64_any_dtype(ser):
        return ser.dt.date
    return pd.to_datetime(ser).dt.date


def build_summary_df(df: pd.DataFrame, usd_to_brl: float) -> pd.DataFrame:
    df = df.copy()
    for c in REQUIRED_COLS:
        if c not in df.columns:
            raise ValueError(f"Coluna obrigatória ausente: {c}")
    df["cost_date"] = normalize_date(df["cost_date"])
    if "storage_usd" not in df.columns:
        df["storage_usd"] = 0.0
    if "compute_usd" not in df.columns:
        df["compute_usd"] = df["total_usd"] - df["storage_usd"].fillna(0)
    if "total_queries" not in df.columns:
        df["total_queries"] = 0
    if "total_read_gb" not in df.columns:
        df["total_read_gb"] = 0.0
    df["storage_brl"] = (df["storage_usd"].fillna(0) * usd_to_brl).astype(float)
    df["compute_brl"] = (df["compute_usd"].fillna(0) * usd_to_brl).astype(float)
    df["total_brl"] = (df["total_usd"].fillna(0) * usd_to_brl).astype(float)
    df["total_tables"] = 0
    df["total_rows"] = 0
    df["total_compressed_gb"] = 0.0
    df["usd_to_brl_rate"] = usd_to_brl
    df["total_queries"] = df["total_queries"].fillna(0).astype("uint64")
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
    summary["storage_usd"] = summary["storage_usd"].astype(float)
    summary["compute_usd"] = summary["compute_usd"].astype(float)
    summary["total_usd"] = summary["total_usd"].astype(float)
    summary["total_read_gb"] = summary["total_read_gb"].astype(float)
    return summary


def main():
    if len(sys.argv) < 2:
        print("Uso: python backfill_clickhouse_cost_history.py <arquivo.csv>")
        sys.exit(1)
    path = sys.argv[1]
    if not os.path.isfile(path):
        raise SystemExit(f"Arquivo não encontrado: {path}")
    usd_to_brl = float(os.environ.get("USD_TO_BRL", DEFAULT_USD_TO_BRL))
    raw = pd.read_csv(path)
    summary = build_summary_df(raw, usd_to_brl)
    client = get_client()
    client.insert_df("observability.cost_summary_daily", summary)
    print(f"Backfill concluído: {len(summary)} linhas em observability.cost_summary_daily")


if __name__ == "__main__":
    main()
