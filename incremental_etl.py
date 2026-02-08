#!/usr/bin/env python3
"""
ETL Incremental - Processa apenas dados novos ou modificados
Usa watermarks para controlar última execução e evitar reprocessamento
"""

import clickhouse_connect
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
from pathlib import Path

# Configurações
CH_HOST = 'e1a1lieug8.us-central1.gcp.clickhouse.cloud'
CH_PORT = 8443
CH_USER = 'default'
CH_PASSWORD = '_uv765EvWphL_'

CH_DATABASE_BRONZE = 'default'
CH_DATABASE_SILVER = 'track_silver'
CH_DATABASE_GOLD = 'track_gold'

# Arquivo de controle de watermarks
WATERMARK_FILE = Path(__file__).parent / 'etl_watermarks.json'


class WatermarkManager:
    """Gerencia watermarks de execução do ETL"""

    def __init__(self, watermark_file):
        self.watermark_file = Path(watermark_file)
        self.watermarks = self._load()

    def _load(self):
        """Carrega watermarks do arquivo"""
        if self.watermark_file.exists():
            with open(self.watermark_file, 'r') as f:
                return json.load(f)
        return {}

    def _save(self):
        """Salva watermarks no arquivo"""
        self.watermark_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.watermark_file, 'w') as f:
            json.dump(self.watermarks, f, indent=2, default=str)

    def get_watermark(self, table_name, layer='silver'):
        """Retorna último watermark para uma tabela"""
        key = f"{layer}.{table_name}"
        return self.watermarks.get(key, {}).get('last_processed_timestamp')

    def set_watermark(self, table_name, timestamp, layer='silver', stats=None):
        """Define watermark para uma tabela"""
        key = f"{layer}.{table_name}"
        self.watermarks[key] = {
            'last_processed_timestamp': timestamp.isoformat() if isinstance(timestamp, datetime) else timestamp,
            'last_execution': datetime.now().isoformat(),
            'stats': stats or {}
        }
        self._save()

    def get_all_watermarks(self):
        """Retorna todos os watermarks"""
        return self.watermarks


class IncrementalETL:
    """Pipeline ETL Incremental"""

    def __init__(self):
        self.client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASSWORD
        )
        self.watermark_mgr = WatermarkManager(WATERMARK_FILE)

    def process_silver_incremental(self, table_name, force_full=False):
        """
        Processa incrementalmente Bronze → Silver

        Args:
            table_name: Nome da tabela
            force_full: Se True, reprocessa tudo
        """

        print(f"\n{'='*80}")
        print(f"🔄 ETL INCREMENTAL: {table_name}")
        print(f"{'='*80}")

        try:
            # 1. Verificar última execução
            last_watermark = self.watermark_mgr.get_watermark(table_name, 'silver')

            if force_full or not last_watermark:
                print(f"📋 Modo: CARGA COMPLETA")
                where_clause = ""
            else:
                print(f"⚡ Modo: INCREMENTAL")
                print(f"   Último processamento: {last_watermark}")

                # Processar apenas registros novos/modificados
                # Assumindo que tabelas têm coluna de timestamp (ajustar conforme schema)
                where_clause = f"WHERE _silver_ingestion_timestamp > '{last_watermark}'"

            # 2. Contar registros novos
            count_query = f"""
                SELECT count() as cnt
                FROM {CH_DATABASE_BRONZE}.{table_name}
                {where_clause}
            """

            try:
                new_records = self.client.query(count_query).result_rows[0][0]
            except:
                # Se coluna não existe, fazer full load
                print(f"   ⚠️  Coluna de controle não encontrada, usando carga completa")
                new_records = self.client.query(f"SELECT count() FROM {CH_DATABASE_BRONZE}.{table_name}").result_rows[0][0]
                where_clause = ""

            if new_records == 0:
                print(f"✅ Nenhum registro novo para processar")
                return {'status': 'success', 'new_records': 0, 'processed': 0}

            print(f"📥 Registros novos: {new_records:,}")

            # 3. Ler dados novos
            query = f"SELECT * FROM {CH_DATABASE_BRONZE}.{table_name} {where_clause} LIMIT 10000"
            df = self.client.query_df(query)

            # 4. Transformar
            initial_count = len(df)
            df = df.drop_duplicates()
            duplicates_removed = initial_count - len(df)

            # Limpar valores
            df = df.replace({pd.NA: None, np.nan: None})

            # Padronizar strings (primeiras 5 colunas)
            str_cols = df.select_dtypes(include=['object']).columns[:5]
            for col in str_cols:
                try:
                    df[col] = df[col].str.strip().str.upper()
                except:
                    pass

            print(f"🔧 Transformado: {len(df):,} linhas ({duplicates_removed} duplicatas)")

            # 5. Criar tabela Silver se não existir
            # DDL com Nullable
            type_map = {
                'int64': 'Nullable(Int64)',
                'float64': 'Nullable(Float64)',
                'object': 'Nullable(String)',
                'datetime64[ns]': 'Nullable(DateTime64)',
                'bool': 'Nullable(UInt8)'
            }

            col_defs = [f"`{col}` {type_map.get(str(df[col].dtype), 'Nullable(String)')}"
                        for col in df.columns]

            silver_table = f"{CH_DATABASE_SILVER}.{table_name}"

            try:
                # Verificar se tabela existe
                self.client.query(f"SELECT 1 FROM {silver_table} LIMIT 1")
                table_exists = True
            except:
                table_exists = False

            if not table_exists:
                print(f"📦 Criando tabela Silver...")
                create_ddl = f"CREATE TABLE {silver_table} ({', '.join(col_defs)}) ENGINE = MergeTree() ORDER BY tuple()"
                self.client.command(create_ddl)

            # 6. Inserir (modo APPEND para incremental)
            print(f"💾 Inserindo em Silver...")
            self.client.insert_df(silver_table, df)

            # 7. Atualizar watermark
            current_timestamp = datetime.now()
            stats = {
                'records_processed': len(df),
                'duplicates_removed': duplicates_removed,
                'new_records': new_records
            }

            self.watermark_mgr.set_watermark(
                table_name,
                current_timestamp,
                'silver',
                stats
            )

            print(f"✅ Processado: {len(df):,} linhas")
            print(f"📊 Watermark atualizado: {current_timestamp.isoformat()}")

            return {
                'status': 'success',
                'new_records': new_records,
                'processed': len(df),
                'duplicates_removed': duplicates_removed
            }

        except Exception as e:
            print(f"❌ Erro: {e}")
            import traceback
            traceback.print_exc()
            return {'status': 'failed', 'error': str(e)}

    def process_gold_incremental(self):
        """
        Processa incrementalmente Silver → Gold (fact_vendas)
        """

        print(f"\n{'='*80}")
        print(f"🥇 ETL INCREMENTAL: GOLD LAYER")
        print(f"{'='*80}")

        try:
            # 1. Verificar última execução Gold
            last_watermark = self.watermark_mgr.get_watermark('fact_vendas', 'gold')

            if not last_watermark:
                print(f"📋 Modo: CARGA INICIAL")
                # Processar todos os dados disponíveis
                date_filter = None
            else:
                print(f"⚡ Modo: INCREMENTAL")
                print(f"   Último processamento: {last_watermark}")
                # Processar últimas 24 horas
                date_filter = f"C5_EMISSAO >= '{(datetime.now() - timedelta(days=1)).strftime('%Y%m%d')}'"

            # 2. Ler itens novos (SC6030)
            print(f"📥 Lendo itens de pedidos (SC6030)...")

            itens = self.client.query_df(f"""
                SELECT
                    C6_NUM,
                    C6_PRODUTO,
                    C6_QTDVEN,
                    C6_PRCVEN,
                    C6_VALOR,
                    C6_DESCONT
                FROM {CH_DATABASE_SILVER}.sc6030
                WHERE C6_NUM IS NOT NULL
                AND C6_PRODUTO IS NOT NULL
                LIMIT 10000
            """)

            # Renomear colunas
            itens = itens.rename(columns={
                'C6_NUM': 'num_pedido',
                'C6_PRODUTO': 'produto_id',
                'C6_QTDVEN': 'quantidade',
                'C6_PRCVEN': 'valor_unitario',
                'C6_VALOR': 'valor_total',
                'C6_DESCONT': 'valor_desconto'
            })

            print(f"   ✅ {len(itens):,} itens lidos")

            if len(itens) == 0:
                print(f"✅ Nenhum pedido novo")
                return {'status': 'success', 'new_records': 0}

            # Converter valores
            itens['quantidade'] = pd.to_numeric(itens['quantidade'], errors='coerce').fillna(0)
            itens['valor_total'] = pd.to_numeric(itens['valor_total'], errors='coerce').fillna(0)
            itens['valor_desconto'] = pd.to_numeric(itens['valor_desconto'], errors='coerce').fillna(0)

            # 3. Ler pedidos (SC5030)
            print(f"📥 Lendo cabeçalho de pedidos (SC5030)...")
            pedidos = self.client.query_df(f"""
                SELECT
                    C5_NUM,
                    C5_CLIENTE,
                    C5_EMISSAO
                FROM {CH_DATABASE_SILVER}.sc5030
                WHERE C5_NUM IS NOT NULL
                {f'AND {date_filter}' if date_filter else ''}
                LIMIT 10000
            """)

            # Renomear colunas
            pedidos = pedidos.rename(columns={
                'C5_NUM': 'num_pedido',
                'C5_CLIENTE': 'cod_cliente',
                'C5_EMISSAO': 'data_emissao'
            })

            print(f"   ✅ {len(pedidos):,} pedidos lidos")

            # 4. Integrar itens com pedidos
            print(f"🔗 Integrando dados...")
            fact = itens.merge(pedidos, on='num_pedido', how='left')

            if len(fact) == 0:
                print(f"✅ Nenhum registro para processar")
                return {'status': 'success', 'new_records': 0}

            print(f"   ✅ {len(fact):,} registros integrados")

            # 5. Processar
            fact['cliente_sk'] = 1
            fact['data_emissao_date'] = pd.to_datetime(
                fact['data_emissao'].astype(str),
                format='%Y%m%d',
                errors='coerce'
            )
            fact['data_id'] = fact['data_emissao_date'].apply(
                lambda x: int(x.strftime('%Y%m%d')) if pd.notna(x) else 20260208
            )
            fact['valor_liquido'] = fact['valor_total'] - fact['valor_desconto']
            fact['margem'] = fact['valor_liquido'] * 0.30

            # 6. Preparar fact_vendas
            fact_vendas = pd.DataFrame({
                'venda_id': [f"INCR-{datetime.now():%Y%m%d}-{i:06d}" for i in range(len(fact))],
                'data_id': fact['data_id'],
                'cliente_sk': fact['cliente_sk'],
                'produto_id': fact['produto_id'],
                'quantidade': fact['quantidade'],
                'valor_unitario': pd.to_numeric(fact['valor_unitario'], errors='coerce').fillna(0),
                'valor_total': fact['valor_total'],
                'valor_desconto': fact['valor_desconto'],
                'valor_liquido': fact['valor_liquido'],
                'margem': fact['margem'],
                'data_emissao': fact['data_emissao_date'],
                'status': 'PROCESSADO',
                'origem': 'INCREMENTAL',
                'data_carga': datetime.now()
            })

            fact_vendas = fact_vendas.replace({pd.NA: None, np.nan: None})

            # 7. Inserir (APPEND)
            print(f"💾 Inserindo {len(fact_vendas):,} vendas...")
            self.client.insert_df(f"{CH_DATABASE_GOLD}.fact_vendas", fact_vendas)

            # 8. Atualizar agregações apenas para datas afetadas
            print(f"📈 Atualizando agregações...")

            # Deletar agregações antigas das datas afetadas
            datas_afetadas = fact_vendas['data_id'].unique()
            for data_id in datas_afetadas:
                self.client.command(f"""
                    DELETE FROM {CH_DATABASE_GOLD}.agg_vendas_diarias
                    WHERE data_id = {data_id}
                """)

            # Recalcular agregações
            self.client.command(f"""
                INSERT INTO {CH_DATABASE_GOLD}.agg_vendas_diarias
                SELECT
                    data_emissao as data,
                    data_id,
                    SUM(valor_total) as total_vendas,
                    COUNT(*) as quantidade_pedidos,
                    AVG(valor_total) as ticket_medio,
                    COUNT(DISTINCT cliente_sk) as clientes_unicos,
                    COUNT(DISTINCT produto_id) as produtos_unicos,
                    SUM(valor_desconto) as valor_desconto_total,
                    30.0 as margem_media,
                    now() as data_atualizacao
                FROM {CH_DATABASE_GOLD}.fact_vendas
                WHERE data_id IN ({','.join(map(str, datas_afetadas))})
                GROUP BY data_emissao, data_id
            """)

            # 9. Atualizar watermark
            current_timestamp = datetime.now()
            stats = {
                'records_processed': len(fact_vendas),
                'dates_affected': len(datas_afetadas)
            }

            self.watermark_mgr.set_watermark(
                'fact_vendas',
                current_timestamp,
                'gold',
                stats
            )

            print(f"✅ Processado: {len(fact_vendas):,} vendas")
            print(f"📊 {len(datas_afetadas)} datas afetadas")
            print(f"📊 Watermark atualizado: {current_timestamp.isoformat()}")

            return {
                'status': 'success',
                'new_records': len(fact_vendas),
                'dates_affected': len(datas_afetadas)
            }

        except Exception as e:
            print(f"❌ Erro: {e}")
            import traceback
            traceback.print_exc()
            return {'status': 'failed', 'error': str(e)}

    def run_full_incremental_pipeline(self, tables=None):
        """
        Executa pipeline incremental completo

        Args:
            tables: Lista de tabelas (None = todas)
        """

        print(f"\n{'='*80}")
        print(f"🚀 PIPELINE ETL INCREMENTAL")
        print(f"{'='*80}")
        print(f"Início: {datetime.now()}")
        print(f"{'='*80}\n")

        if tables is None:
            tables = ['sc5030', 'sc6030', 'sd2030', 'sf2030']

        results = {
            'silver': {},
            'gold': {}
        }

        # 1. Processar Silver
        print(f"\n🥈 FASE 1: SILVER LAYER")
        print(f"{'='*80}")

        for table in tables:
            result = self.process_silver_incremental(table)
            results['silver'][table] = result

        # 2. Processar Gold
        print(f"\n🥇 FASE 2: GOLD LAYER")
        print(f"{'='*80}")

        result = self.process_gold_incremental()
        results['gold']['fact_vendas'] = result

        # 3. Resumo
        print(f"\n{'='*80}")
        print(f"📊 RESUMO DO PIPELINE")
        print(f"{'='*80}")

        silver_success = sum(1 for r in results['silver'].values() if r['status'] == 'success')
        silver_total = len(results['silver'])
        silver_records = sum(r.get('processed', 0) for r in results['silver'].values())

        print(f"\n🥈 Silver:")
        print(f"   Sucesso: {silver_success}/{silver_total}")
        print(f"   Registros processados: {silver_records:,}")

        gold_records = results['gold']['fact_vendas'].get('new_records', 0)

        print(f"\n🥇 Gold:")
        print(f"   Registros processados: {gold_records:,}")

        print(f"\n⏱️  Fim: {datetime.now()}")
        print(f"{'='*80}")

        # 4. Mostrar watermarks
        print(f"\n📊 WATERMARKS ATUAIS:")
        print(f"{'='*80}")

        watermarks = self.watermark_mgr.get_all_watermarks()
        for key, value in watermarks.items():
            print(f"{key}:")
            print(f"  Última execução: {value['last_execution']}")
            print(f"  Último timestamp: {value['last_processed_timestamp']}")
            if value.get('stats'):
                print(f"  Stats: {value['stats']}")

        print(f"{'='*80}\n")

        return results

    def close(self):
        """Fecha conexão"""
        self.client.close()


def main():
    """Executa ETL incremental"""

    etl = IncrementalETL()

    try:
        # Executar pipeline incremental
        results = etl.run_full_incremental_pipeline()

        # Verificar se houve erros
        has_errors = any(
            r['status'] == 'failed'
            for layer_results in results.values()
            for r in layer_results.values()
        )

        return 1 if has_errors else 0

    finally:
        etl.close()


if __name__ == "__main__":
    import sys
    sys.exit(main())
