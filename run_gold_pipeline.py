#!/usr/bin/env python3
"""
Pipeline Gold Layer - Analytics & Business Intelligence
Cria modelos dimensionais, agregações e KPIs
"""

import clickhouse_connect
import pandas as pd
import numpy as np
from datetime import datetime
import sys

# Configurações
CH_HOST = 'e1a1lieug8.us-central1.gcp.clickhouse.cloud'
CH_PORT = 8443
CH_USER = 'default'
CH_PASSWORD = '_uv765EvWphL_'
CH_DATABASE_SILVER = 'track_silver'
CH_DATABASE_GOLD = 'track_gold'

def create_gold_layer():
    """Cria estrutura completa da camada Gold"""

    print(f"\n{'='*80}")
    print(f"🥇 CRIANDO CAMADA GOLD - Analytics & BI")
    print(f"{'='*80}\n")

    # Conectar
    client = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD
    )

    version = client.query("SELECT version()").result_rows[0][0]
    print(f"✅ ClickHouse {version}")

    # Criar database
    print(f"\n📦 Criando database Gold: {CH_DATABASE_GOLD}")
    client.command(f"CREATE DATABASE IF NOT EXISTS {CH_DATABASE_GOLD}")
    print(f"✅ Database criada")

    # 1. Criar dim_data (Calendário)
    print(f"\n📅 Criando dim_data...")

    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE_GOLD}.dim_data (
            data_id Int32,
            data Date,
            ano Int16,
            mes Int8,
            dia Int8,
            trimestre Int8,
            semestre Int8,
            semana_ano Int8,
            dia_semana Int8,
            dia_semana_nome String,
            mes_nome String,
            eh_fim_semana UInt8,
            eh_feriado UInt8,
            nome_feriado Nullable(String)
        ) ENGINE = MergeTree()
        ORDER BY data_id
    """)

    # Gerar dados
    dates = pd.date_range(start='2020-01-01', end='2026-12-31', freq='D')
    dim_data = pd.DataFrame({
        'data_id': [(d.year * 10000 + d.month * 100 + d.day) for d in dates],
        'data': dates,
        'ano': dates.year,
        'mes': dates.month,
        'dia': dates.day,
        'trimestre': dates.quarter,
        'semestre': (dates.month - 1) // 6 + 1,
        'semana_ano': dates.isocalendar().week,
        'dia_semana': dates.dayofweek + 1,
        'dia_semana_nome': dates.day_name(),
        'mes_nome': dates.month_name(),
        'eh_fim_semana': (dates.dayofweek >= 5).astype(int),
        'eh_feriado': 0,
        'nome_feriado': None
    })

    client.command(f"TRUNCATE TABLE {CH_DATABASE_GOLD}.dim_data")
    client.insert_df(f"{CH_DATABASE_GOLD}.dim_data", dim_data)
    print(f"✅ dim_data: {len(dim_data):,} registros (2020-2026)")

    # 2. Criar dim_cliente
    print(f"\n👥 Criando dim_cliente (SCD Type 2)...")

    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE_GOLD}.dim_cliente (
            cliente_sk Int64,
            cod_cliente Nullable(String),
            nome_cliente Nullable(String),
            tipo_cliente Nullable(String),
            segmento Nullable(String),
            cidade Nullable(String),
            estado Nullable(String),
            regiao Nullable(String),
            data_inicio Date,
            data_fim Nullable(Date),
            versao Int32,
            eh_atual UInt8,
            data_carga DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (cliente_sk, versao)
    """)

    try:
        clientes = client.query_df(f"""
            SELECT DISTINCT
                cod_cliente,
                nome_cliente
            FROM {CH_DATABASE_SILVER}.depara_cliente
            WHERE cod_cliente IS NOT NULL
            LIMIT 1000
        """)

        if len(clientes) > 0:
            dim_cliente = pd.DataFrame({
                'cliente_sk': range(1, len(clientes) + 1),
                'cod_cliente': clientes['cod_cliente'],
                'nome_cliente': clientes['nome_cliente'],
                'tipo_cliente': 'PESSOA_FISICA',
                'segmento': 'VAREJO',
                'cidade': None,
                'estado': None,
                'regiao': None,
                'data_inicio': datetime.now().date(),
                'data_fim': None,
                'versao': 1,
                'eh_atual': 1,
                'data_carga': datetime.now()
            })

            client.command(f"TRUNCATE TABLE {CH_DATABASE_GOLD}.dim_cliente")
            client.insert_df(f"{CH_DATABASE_GOLD}.dim_cliente", dim_cliente)
            print(f"✅ dim_cliente: {len(dim_cliente):,} registros")
        else:
            print(f"⚠️  Sem dados de cliente")
    except Exception as e:
        print(f"⚠️  Erro ao criar dim_cliente: {e}")

    # 3. Criar fact_vendas
    print(f"\n📦 Criando fact_vendas...")

    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE_GOLD}.fact_vendas (
            venda_id String,
            data_id Int32,
            cliente_sk Int64,
            produto_id Nullable(String),
            quantidade Nullable(Float64),
            valor_unitario Nullable(Float64),
            valor_total Nullable(Float64),
            valor_desconto Nullable(Float64),
            valor_liquido Nullable(Float64),
            margem Nullable(Float64),
            data_emissao Nullable(Date),
            status Nullable(String),
            origem String DEFAULT 'SILVER',
            data_carga DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (data_id, venda_id)
    """)

    print(f"✅ fact_vendas criada")

    # 4. Criar agregações
    print(f"\n📈 Criando agregações...")

    # Diária
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE_GOLD}.agg_vendas_diarias (
            data Date,
            data_id Int32,
            total_vendas Float64,
            quantidade_pedidos Int64,
            ticket_medio Float64,
            clientes_unicos Int64,
            produtos_unicos Int64,
            valor_desconto_total Float64,
            margem_media Float64,
            data_atualizacao DateTime DEFAULT now()
        ) ENGINE = SummingMergeTree()
        ORDER BY (data, data_id)
    """)
    print(f"  ✓ agg_vendas_diarias")

    # Mensal
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE_GOLD}.agg_vendas_mensais (
            ano Int16,
            mes Int8,
            mes_ano String,
            total_vendas Float64,
            quantidade_pedidos Int64,
            ticket_medio Float64,
            clientes_unicos Int64,
            crescimento_pct Nullable(Float64),
            data_atualizacao DateTime DEFAULT now()
        ) ENGINE = SummingMergeTree()
        ORDER BY (ano, mes)
    """)
    print(f"  ✓ agg_vendas_mensais")

    # 5. Criar KPIs
    print(f"\n🎯 Criando KPIs...")

    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE_GOLD}.kpi_snapshot (
            kpi_id String,
            kpi_nome String,
            kpi_categoria String,
            valor_atual Float64,
            valor_anterior Nullable(Float64),
            variacao_pct Nullable(Float64),
            meta Nullable(Float64),
            atingimento_pct Nullable(Float64),
            unidade String,
            periodo String,
            data_referencia Date,
            data_calculo DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(data_calculo)
        ORDER BY (kpi_id, data_referencia)
    """)

    # Inserir KPIs iniciais
    kpis_initial = pd.DataFrame([
        {
            'kpi_id': 'KPI001',
            'kpi_nome': 'Receita Total',
            'kpi_categoria': 'FINANCEIRO',
            'valor_atual': 0.0,
            'valor_anterior': None,
            'variacao_pct': None,
            'meta': 1000000.0,
            'atingimento_pct': 0.0,
            'unidade': 'R$',
            'periodo': 'MENSAL',
            'data_referencia': datetime.now().date(),
            'data_calculo': datetime.now()
        },
        {
            'kpi_id': 'KPI002',
            'kpi_nome': 'Ticket Médio',
            'kpi_categoria': 'VENDAS',
            'valor_atual': 0.0,
            'valor_anterior': None,
            'variacao_pct': None,
            'meta': 500.0,
            'atingimento_pct': 0.0,
            'unidade': 'R$',
            'periodo': 'MENSAL',
            'data_referencia': datetime.now().date(),
            'data_calculo': datetime.now()
        },
        {
            'kpi_id': 'KPI003',
            'kpi_nome': 'Taxa de Conversão',
            'kpi_categoria': 'VENDAS',
            'valor_atual': 0.0,
            'valor_anterior': None,
            'variacao_pct': None,
            'meta': 15.0,
            'atingimento_pct': 0.0,
            'unidade': '%',
            'periodo': 'MENSAL',
            'data_referencia': datetime.now().date(),
            'data_calculo': datetime.now()
        }
    ])

    client.insert_df(f"{CH_DATABASE_GOLD}.kpi_snapshot", kpis_initial)
    print(f"✅ KPIs criados: {len(kpis_initial)}")

    # 6. Criar Views
    print(f"\n🔍 Criando views...")

    # View: Qualidade
    client.command(f"""
        CREATE OR REPLACE VIEW {CH_DATABASE_GOLD}.view_quality_summary AS
        SELECT
            table_name,
            COUNT(*) as total_execucoes,
            MAX(execution_timestamp) as ultima_execucao,
            AVG(quality_score) as quality_score_medio,
            MAX(total_rows_output) as max_linhas,
            SUM(rows_duplicates) as total_duplicatas_removidas,
            AVG(completeness_score) as completeness_medio
        FROM {CH_DATABASE_SILVER}.quality_metrics
        GROUP BY table_name
    """)
    print(f"  ✓ view_quality_summary")

    # View: Performance
    client.command(f"""
        CREATE OR REPLACE VIEW {CH_DATABASE_GOLD}.view_performance_summary AS
        SELECT
            table_name,
            COUNT(*) as total_execucoes,
            AVG(duration_seconds) as duracao_media_seg,
            AVG(throughput_rows_per_sec) as throughput_medio,
            MAX(rows_processed) as max_rows_processed,
            SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as execucoes_sucesso,
            SUM(CASE WHEN status != 'success' THEN 1 ELSE 0 END) as execucoes_falha
        FROM {CH_DATABASE_SILVER}.performance_metrics
        GROUP BY table_name
    """)
    print(f"  ✓ view_performance_summary")

    # View: Calendário
    client.command(f"""
        CREATE OR REPLACE VIEW {CH_DATABASE_GOLD}.view_calendario AS
        SELECT
            data_id,
            data,
            ano,
            mes,
            dia,
            trimestre,
            semestre,
            semana_ano,
            dia_semana,
            dia_semana_nome,
            mes_nome,
            eh_fim_semana,
            eh_feriado,
            CONCAT(ano, '-Q', trimestre) as ano_trimestre,
            CONCAT(ano, '-S', semestre) as ano_semestre,
            CONCAT(ano, '-', lpad(toString(mes), 2, '0')) as ano_mes
        FROM {CH_DATABASE_GOLD}.dim_data
    """)
    print(f"  ✓ view_calendario")

    # 7. Validação
    print(f"\n{'='*80}")
    print(f"📊 VALIDAÇÃO DA CAMADA GOLD")
    print(f"{'='*80}")

    gold_tables = client.query_df(f"""
        SELECT
            name as table_name,
            engine,
            total_rows,
            formatReadableSize(total_bytes) as size
        FROM system.tables
        WHERE database = '{CH_DATABASE_GOLD}'
        ORDER BY
            CASE
                WHEN name LIKE 'dim_%' THEN 1
                WHEN name LIKE 'fact_%' THEN 2
                WHEN name LIKE 'agg_%' THEN 3
                WHEN name LIKE 'kpi_%' THEN 4
                ELSE 5
            END,
            name
    """)

    print(f"\n📋 TABELAS CRIADAS:")
    print(gold_tables.to_string(index=False))

    # Contar por tipo
    dims = len(gold_tables[gold_tables['table_name'].str.startswith('dim_')])
    facts = len(gold_tables[gold_tables['table_name'].str.startswith('fact_')])
    aggs = len(gold_tables[gold_tables['table_name'].str.startswith('agg_')])
    kpis = len(gold_tables[gold_tables['table_name'].str.startswith('kpi_')])

    print(f"\n📊 RESUMO:")
    print(f"  Dimensões: {dims}")
    print(f"  Fatos: {facts}")
    print(f"  Agregações: {aggs}")
    print(f"  KPIs: {kpis}")
    print(f"  Total: {len(gold_tables)}")

    # Views
    gold_views = client.query_df(f"""
        SELECT name as view_name
        FROM system.tables
        WHERE database = '{CH_DATABASE_GOLD}'
        AND engine LIKE '%View%'
        ORDER BY name
    """)

    if len(gold_views) > 0:
        print(f"\n👁️  VIEWS ({len(gold_views)}):")
        for view in gold_views['view_name']:
            print(f"  • {view}")

    print(f"\n{'='*80}")
    print(f"✅ CAMADA GOLD CRIADA COM SUCESSO!")
    print(f"{'='*80}\n")

    client.close()
    return 0


if __name__ == "__main__":
    sys.exit(create_gold_layer())
