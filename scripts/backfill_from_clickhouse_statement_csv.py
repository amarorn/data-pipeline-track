"""
Backfill observability.cost_summary_daily a partir de CSVs de statement do ClickHouse Cloud.
Formato: colunas Date, Total ($), Warehouse Storage ($), Warehouse Backups ($).
Uso: python scripts/backfill_from_clickhouse_statement_csv.py file1.csv file2.csv ...
Requer: CH_HOST ou CLICKHOUSE_HOST (e porta, user, password). Opcional: USD_TO_BRL.
"""
import os
import sys

import pandas as pd

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import clickhouse_connect
except ImportError:
    print("Instale: pip install clickhouse-connect pandas")
    sys.exit(1)

DEFAULT_USD_TO_BRL = 5.22
DATE_COL = "Date"
TOTAL_COL = "Total ($)"
STORAGE_COL = "Warehouse Storage ($)"
BACKUPS_COL = "Warehouse Backups ($)"


def get_client():
    host = os.environ.get("CH_HOST") or os.environ.get("CLICKHOUSE_HOST")
    port = int(os.environ.get("CH_PORT") or os.environ.get("CLICKHOUSE_PORT", "8443"))
    user = os.environ.get("CH_USER") or os.environ.get("CLICKHOUSE_USER", "default")
    password = os.environ.get("CH_PASSWORD") or os.environ.get("CLICKHOUSE_PASSWORD", "")
    if not host:
        raise SystemExit("Defina CH_HOST ou CLICKHOUSE_HOST (e porta, user, password)")
    return clickhouse_connect.get_client(
        host=host, port=port, username=user, password=password, secure=True
    )


def parse_statement_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if TOTAL_COL not in df.columns or DATE_COL not in df.columns:
        raise ValueError(f"CSV deve ter colunas '{DATE_COL}' e '{TOTAL_COL}': {path}")
    for col in (TOTAL_COL, STORAGE_COL, BACKUPS_COL):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def aggregate_statements(frames: list) -> pd.DataFrame:
    combined = pd.concat(frames, ignore_index=True)
    storage_col = STORAGE_COL if STORAGE_COL in combined.columns else None
    backups_col = BACKUPS_COL if BACKUPS_COL in combined.columns else None
    agg = combined.groupby(DATE_COL, as_index=False)[TOTAL_COL].sum()
    agg = agg.rename(columns={DATE_COL: "cost_date", TOTAL_COL: "total_usd"})
    if storage_col and backups_col:
        storage_agg = combined.groupby(DATE_COL, as_index=False).agg(
            storage_usd=(storage_col, "sum"),
            backups_usd=(backups_col, "sum"),
        )
        storage_agg["storage_usd"] = storage_agg["storage_usd"] + storage_agg["backups_usd"]
        agg = agg.merge(
            storage_agg[[DATE_COL, "storage_usd"]],
            left_on="cost_date",
            right_on=DATE_COL,
            how="left",
        ).drop(columns=[DATE_COL])
        agg["storage_usd"] = agg["storage_usd"].fillna(0)
    else:
        agg["storage_usd"] = 0.0
    agg["compute_usd"] = (agg["total_usd"] - agg["storage_usd"]).clip(lower=0)
    return agg


def build_summary_df(agg: pd.DataFrame, usd_to_brl: float) -> pd.DataFrame:
    agg = agg.copy()
    agg["cost_date"] = pd.to_datetime(agg["cost_date"]).dt.date
    agg["storage_brl"] = (agg["storage_usd"].fillna(0) * usd_to_brl).astype(float)
    agg["compute_brl"] = (agg["compute_usd"].fillna(0) * usd_to_brl).astype(float)
    agg["total_brl"] = (agg["total_usd"].fillna(0) * usd_to_brl).astype(float)
    agg["total_tables"] = 0
    agg["total_rows"] = 0
    agg["total_compressed_gb"] = 0.0
    agg["total_queries"] = 0
    agg["total_read_gb"] = 0.0
    agg["usd_to_brl_rate"] = usd_to_brl
    summary = agg[
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
    summary["storage_usd"] = summary["storage_usd"].astype(float)
    summary["compute_usd"] = summary["compute_usd"].astype(float)
    summary["total_usd"] = summary["total_usd"].astype(float)
    summary["total_read_gb"] = summary["total_read_gb"].astype(float)
    return summary


def main():
    if len(sys.argv) < 2:
        print("Uso: python backfill_from_clickhouse_statement_csv.py <file1.csv> [file2.csv ...]")
        sys.exit(1)
    paths = [p for p in sys.argv[1:] if os.path.isfile(p)]
    if not paths:
        raise SystemExit("Nenhum arquivo encontrado.")
    usd_to_brl = float(os.environ.get("USD_TO_BRL", DEFAULT_USD_TO_BRL))
    frames = [parse_statement_csv(p) for p in paths]
    agg = aggregate_statements(frames)
    if agg.empty:
        raise SystemExit("Nenhum dado agregado.")
    summary = build_summary_df(agg, usd_to_brl)
    client = get_client()
    client.insert_df("observability.cost_summary_daily", summary)
    print(f"Backfill concluído: {len(summary)} dias em observability.cost_summary_daily")


if __name__ == "__main__":
    main()
