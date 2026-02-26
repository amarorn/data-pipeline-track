#!/usr/bin/env python3
"""
Popular fact_vendas com dados do Silver
Integra dados de pedidos, itens e notas fiscais
"""

import clickhouse_connect
import pandas as pd
import numpy as np
from datetime import datetime
import uuid

# Configurações
CH_HOST = 'e1a1lieug8.us-central1.gcp.clickhouse.cloud'
CH_PORT = 8443
CH_USER = 'default'
CH_PASSWORD = '_uv765EvWphL_'
CH_DATABASE_SILVER = 'track_silver'
CH_DATABASE_GOLD = 'track_gold'

def populate_fact_vendas():
    """Popula fact_vendas com dados do Silver"""

    print(f"\n{'='*80}")
    print(f"📦 POPULANDO FACT_VENDAS")
    print(f"{'='*80}\n")

    client = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD
    )

    # 1. Buscar dados de itens de pedidos (SC6030)
    print("📥 1. Lendo itens de pedidos (SC6030)...")

    try:
        # Pegar itens com informações relevantes
        itens_pedido = client.query_df(f"""
            SELECT
                C6_NUM as num_pedido,
                C6_ITEM as item,
                C6_PRODUTO as produto_id,
                C6_QTDVEN as quantidade,
                C6_PRCVEN as valor_unitario,
                C6_VALOR as valor_total,
                C6_DESCONT as valor_desconto
            FROM {CH_DATABASE_SILVER}.sc6030
            WHERE C6_NUM IS NOT NULL
            AND C6_PRODUTO IS NOT NULL
            LIMIT 10000
        """)

        print(f"   ✅ {len(itens_pedido):,} itens lidos")

        # 2. Buscar dados de cabeçalho de pedidos (SC5030) para pegar cliente e data
        print("📥 2. Lendo cabeçalho de pedidos (SC5030)...")

        pedidos = client.query_df(f"""
            SELECT
                C5_NUM as num_pedido,
                C5_CLIENTE as cod_cliente,
                C5_EMISSAO as data_emissao
            FROM {CH_DATABASE_SILVER}.sc5030
            WHERE C5_NUM IS NOT NULL
            LIMIT 10000
        """)

        print(f"   ✅ {len(pedidos):,} pedidos lidos")

        # 3. Juntar itens com pedidos
        print("🔗 3. Integrando dados...")

        fact_data = itens_pedido.merge(
            pedidos,
            on='num_pedido',
            how='left'
        )

        print(f"   ✅ {len(fact_data):,} registros integrados")

        # 4. Buscar mapeamento de clientes
        print("👥 4. Mapeando clientes...")

        try:
            dim_cliente = client.query_df(f"""
                SELECT
                    cliente_sk,
                    cod_cliente
                FROM {CH_DATABASE_GOLD}.dim_cliente
                WHERE cod_cliente IS NOT NULL
            """)

            # Criar dicionário de mapeamento
            cliente_map = dict(zip(dim_cliente['cod_cliente'], dim_cliente['cliente_sk']))
            fact_data['cliente_sk'] = fact_data['cod_cliente'].map(cliente_map).fillna(1).astype(int)

        except:
            # Se não tiver clientes, usar SK padrão
            fact_data['cliente_sk'] = 1

        # 5. Processar data_emissao e criar data_id
        print("📅 5. Processando datas...")

        # Converter data_emissao (formato YYYYMMDD)
        fact_data['data_emissao_date'] = pd.to_datetime(
            fact_data['data_emissao'].astype(str),
            format='%Y%m%d',
            errors='coerce'
        )

        # Criar data_id (YYYYMMDD como int)
        fact_data['data_id'] = fact_data['data_emissao_date'].apply(
            lambda x: int(x.strftime('%Y%m%d')) if pd.notna(x) else 20260101
        )

        # 6. Calcular métricas
        print("🧮 6. Calculando métricas...")

        # Valor líquido = valor total - desconto
        fact_data['valor_liquido'] = (
            fact_data['valor_total'].fillna(0) - fact_data['valor_desconto'].fillna(0)
        )

        # Margem estimada (exemplo: 30%)
        fact_data['margem'] = fact_data['valor_liquido'] * 0.30

        # 7. Preparar DataFrame final
        print("📋 7. Preparando dados finais...")

        fact_vendas = pd.DataFrame({
            'venda_id': [f"VENDA-{i:08d}" for i in range(len(fact_data))],
            'data_id': fact_data['data_id'],
            'cliente_sk': fact_data['cliente_sk'],
            'produto_id': fact_data['produto_id'],
            'quantidade': fact_data['quantidade'].fillna(0),
            'valor_unitario': fact_data['valor_unitario'].fillna(0),
            'valor_total': fact_data['valor_total'].fillna(0),
            'valor_desconto': fact_data['valor_desconto'].fillna(0),
            'valor_liquido': fact_data['valor_liquido'],
            'margem': fact_data['margem'],
            'data_emissao': fact_data['data_emissao_date'],
            'status': 'PROCESSADO',
            'origem': 'SILVER_SC6030',
            'data_carga': datetime.now()
        })

        # Limpar NAs
        fact_vendas = fact_vendas.replace({pd.NA: None, np.nan: None})

        # 8. Inserir no ClickHouse
        print("💾 8. Inserindo em fact_vendas...")

        # Truncar tabela
        client.command(f"TRUNCATE TABLE {CH_DATABASE_GOLD}.fact_vendas")

        # Inserir dados
        client.insert_df(f"{CH_DATABASE_GOLD}.fact_vendas", fact_vendas)

        # Verificar
        count = client.query(f"SELECT count() FROM {CH_DATABASE_GOLD}.fact_vendas").result_rows[0][0]

        print(f"   ✅ {count:,} registros inseridos")

        # 9. Estatísticas
        print(f"\n{'='*80}")
        print(f"📊 ESTATÍSTICAS")
        print(f"{'='*80}")

        stats = client.query_df(f"""
            SELECT
                COUNT(*) as total_vendas,
                COUNT(DISTINCT cliente_sk) as clientes_unicos,
                COUNT(DISTINCT produto_id) as produtos_unicos,
                SUM(quantidade) as quantidade_total,
                SUM(valor_total) as receita_total,
                SUM(valor_desconto) as desconto_total,
                SUM(valor_liquido) as receita_liquida,
                AVG(valor_total) as ticket_medio
            FROM {CH_DATABASE_GOLD}.fact_vendas
        """)

        print(f"\nTotal de vendas: {stats['total_vendas'][0]:,}")
        print(f"Clientes únicos: {stats['clientes_unicos'][0]:,}")
        print(f"Produtos únicos: {stats['produtos_unicos'][0]:,}")
        print(f"Quantidade total: {stats['quantidade_total'][0]:,.2f}")
        print(f"Receita total: R$ {stats['receita_total'][0]:,.2f}")
        print(f"Desconto total: R$ {stats['desconto_total'][0]:,.2f}")
        print(f"Receita líquida: R$ {stats['receita_liquida'][0]:,.2f}")
        print(f"Ticket médio: R$ {stats['ticket_medio'][0]:,.2f}")

        # 10. Atualizar agregações diárias
        print(f"\n{'='*80}")
        print(f"📈 ATUALIZANDO AGREGAÇÕES")
        print(f"{'='*80}\n")

        print("📅 Agregação diária...")

        client.command(f"TRUNCATE TABLE {CH_DATABASE_GOLD}.agg_vendas_diarias")

        client.command(f"""
            INSERT INTO {CH_DATABASE_GOLD}.agg_vendas_diarias
            SELECT
                data_emissao as data,
                data_id,
                SUM(valor_total) as total_vendas,
                COUNT(DISTINCT venda_id) as quantidade_pedidos,
                AVG(valor_total) as ticket_medio,
                COUNT(DISTINCT cliente_sk) as clientes_unicos,
                COUNT(DISTINCT produto_id) as produtos_unicos,
                SUM(valor_desconto) as valor_desconto_total,
                AVG(margem / NULLIF(valor_liquido, 0) * 100) as margem_media,
                now() as data_atualizacao
            FROM {CH_DATABASE_GOLD}.fact_vendas
            WHERE data_emissao IS NOT NULL
            GROUP BY data_emissao, data_id
        """)

        count_agg = client.query(f"SELECT count() FROM {CH_DATABASE_GOLD}.agg_vendas_diarias").result_rows[0][0]
        print(f"   ✅ {count_agg:,} dias agregados")

        # Agregação mensal
        print("📅 Agregação mensal...")

        client.command(f"TRUNCATE TABLE {CH_DATABASE_GOLD}.agg_vendas_mensais")

        client.command(f"""
            INSERT INTO {CH_DATABASE_GOLD}.agg_vendas_mensais
            SELECT
                toYear(data_emissao) as ano,
                toMonth(data_emissao) as mes,
                CONCAT(toString(toYear(data_emissao)), '-', lpad(toString(toMonth(data_emissao)), 2, '0')) as mes_ano,
                SUM(valor_total) as total_vendas,
                COUNT(DISTINCT venda_id) as quantidade_pedidos,
                AVG(valor_total) as ticket_medio,
                COUNT(DISTINCT cliente_sk) as clientes_unicos,
                0 as crescimento_pct,
                now() as data_atualizacao
            FROM {CH_DATABASE_GOLD}.fact_vendas
            WHERE data_emissao IS NOT NULL
            GROUP BY ano, mes, mes_ano
        """)

        count_mensal = client.query(f"SELECT count() FROM {CH_DATABASE_GOLD}.agg_vendas_mensais").result_rows[0][0]
        print(f"   ✅ {count_mensal:,} meses agregados")

        # 11. Atualizar KPIs
        print(f"\n📊 Atualizando KPIs...")

        # Receita Total
        receita_atual = stats['receita_liquida'][0]

        client.command(f"""
            INSERT INTO {CH_DATABASE_GOLD}.kpi_snapshot
            SELECT
                'KPI001' as kpi_id,
                'Receita Total' as kpi_nome,
                'FINANCEIRO' as kpi_categoria,
                {receita_atual} as valor_atual,
                NULL as valor_anterior,
                NULL as variacao_pct,
                1000000.0 as meta,
                ({receita_atual} / 1000000.0 * 100) as atingimento_pct,
                'R$' as unidade,
                'MENSAL' as periodo,
                today() as data_referencia,
                now() as data_calculo
        """)

        # Ticket Médio
        ticket_atual = stats['ticket_medio'][0]

        client.command(f"""
            INSERT INTO {CH_DATABASE_GOLD}.kpi_snapshot
            SELECT
                'KPI002' as kpi_id,
                'Ticket Médio' as kpi_nome,
                'VENDAS' as kpi_categoria,
                {ticket_atual} as valor_atual,
                NULL as valor_anterior,
                NULL as variacao_pct,
                500.0 as meta,
                ({ticket_atual} / 500.0 * 100) as atingimento_pct,
                'R$' as unidade,
                'MENSAL' as periodo,
                today() as data_referencia,
                now() as data_calculo
        """)

        print(f"   ✅ KPIs atualizados")

        print(f"\n{'='*80}")
        print(f"✅ FACT_VENDAS POPULADA COM SUCESSO!")
        print(f"{'='*80}\n")

        client.close()
        return 0

    except Exception as e:
        print(f"\n❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        client.close()
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(populate_fact_vendas())
