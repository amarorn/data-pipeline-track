#!/usr/bin/env python3
"""
Test script to verify column names from ClickHouse queries
"""

import clickhouse_connect
import os
from dotenv import load_dotenv

load_dotenv()

# Conectar ao ClickHouse
client = clickhouse_connect.get_client(
    host=os.getenv('CLICKHOUSE_HOST'),
    port=int(os.getenv('CLICKHOUSE_PORT', 8443)),
    username=os.getenv('CLICKHOUSE_USER'),
    password=os.getenv('CLICKHOUSE_PASSWORD'),
    secure=True
)

print("=" * 80)
print("Testing SC6030 (itens)")
print("=" * 80)

itens = client.query_df("""
    SELECT
        C6_NUM as num_pedido,
        C6_PRODUTO as produto_id,
        C6_QTDVEN as quantidade,
        C6_PRCVEN as valor_unitario,
        C6_VALOR as valor_total,
        C6_DESCONT as valor_desconto
    FROM track_silver.sc6030
    WHERE C6_NUM IS NOT NULL
    AND C6_PRODUTO IS NOT NULL
    LIMIT 5
""")

print(f"Columns: {list(itens.columns)}")
print(f"Rows: {len(itens)}")
if len(itens) > 0:
    print(f"Sample data:\n{itens.head(1)}")

print("\n" + "=" * 80)
print("Testing SC5030 (pedidos)")
print("=" * 80)

pedidos = client.query_df("""
    SELECT
        C5_NUM as num_pedido,
        C5_CLIENTE as cod_cliente,
        C5_EMISSAO as data_emissao
    FROM track_silver.sc5030
    WHERE C5_NUM IS NOT NULL
    LIMIT 5
""")

print(f"Columns: {list(pedidos.columns)}")
print(f"Rows: {len(pedidos)}")
if len(pedidos) > 0:
    print(f"Sample data:\n{pedidos.head(1)}")

print("\n" + "=" * 80)
print("Testing merge")
print("=" * 80)

if len(itens) > 0 and len(pedidos) > 0:
    try:
        merged = itens.merge(pedidos, on='num_pedido', how='inner')
        print(f"✅ Merge successful! {len(merged)} rows")
        print(f"Merged columns: {list(merged.columns)}")
    except Exception as e:
        print(f"❌ Merge failed: {e}")
