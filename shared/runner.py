"""
Orquestrador de pipeline - Executa camadas do medalhão
"""
import argparse
from typing import List
from datetime import datetime

from shared.utils.logger import setup_logger
from shared.config.settings import settings


class PipelineRunner:
    """Orquestra execução do pipeline"""

    def __init__(self):
        self.logger = setup_logger(
            "PipelineRunner",
            log_file=settings.LOGS_PATH / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )

    def run_bronze(self):
        """Executa camada Bronze"""
        self.logger.info("=" * 50)
        self.logger.info("Executando camada BRONZE")
        self.logger.info("=" * 50)

        try:
            from layers.bronze.oracle_ingestion import OracleBronzeIngestion

            ingestion = OracleBronzeIngestion()

            # Exemplo: configure suas tabelas aqui
            tables = [
                {
                    "table": "TABELA_FONTE",
                    "schema": "SCHEMA",
                    "partition_column": "ID",
                    "num_partitions": 10
                }
            ]

            ingestion.ingest_multiple_tables(tables)

            self.logger.info("Camada BRONZE concluída com sucesso")

        except Exception as e:
            self.logger.error(f"Erro na camada BRONZE: {str(e)}")
            raise

    def run_silver(self):
        """Executa camada Silver"""
        self.logger.info("=" * 50)
        self.logger.info("Executando camada SILVER")
        self.logger.info("=" * 50)

        try:
            from layers.silver.data_quality import DataQuality

            dq = DataQuality()

            # Exemplo: configure suas transformações aqui
            transformations = {
                "clean_strings": {
                    "columns": ["nome", "descricao"],
                    "trim_spaces": True
                },
                "deduplicate": {
                    "subset": ["id"]
                }
            }

            dq.process_bronze_to_silver(
                source="oracle",
                entity="tabela_fonte",
                domain="vendas",
                transformations=transformations
            )

            self.logger.info("Camada SILVER concluída com sucesso")

        except Exception as e:
            self.logger.error(f"Erro na camada SILVER: {str(e)}")
            raise

    def run_gold(self):
        """Executa camada Gold"""
        self.logger.info("=" * 50)
        self.logger.info("Executando camada GOLD")
        self.logger.info("=" * 50)

        try:
            from layers.gold.aggregations import GoldAggregations

            gold = GoldAggregations()

            # Exemplo: configure suas métricas aqui
            gold.create_daily_summary(
                domain="vendas",
                entity="tabela_fonte",
                date_column="_ingestion_date",
                metrics={
                    "id": ["count"]
                }
            )

            self.logger.info("Camada GOLD concluída com sucesso")

        except Exception as e:
            self.logger.error(f"Erro na camada GOLD: {str(e)}")
            raise

    def run(self, layers: List[str]):
        """
        Executa camadas especificadas

        Args:
            layers: Lista de camadas ['bronze', 'silver', 'gold']
        """
        start_time = datetime.now()
        self.logger.info(f"Iniciando pipeline: {', '.join(layers)}")

        try:
            if "bronze" in layers:
                self.run_bronze()

            if "silver" in layers:
                self.run_silver()

            if "gold" in layers:
                self.run_gold()

            duration = (datetime.now() - start_time).total_seconds()
            self.logger.info(f"Pipeline concluído em {duration:.2f}s")

        except Exception as e:
            self.logger.error(f"Pipeline falhou: {str(e)}")
            raise

        finally:
            # Cleanup
            from shared.infrastructure.spark.spark_session import SparkSessionManager
            SparkSessionManager.stop_session()


def main():
    """Ponto de entrada CLI"""
    parser = argparse.ArgumentParser(description="Executa pipeline de dados")
    parser.add_argument(
        "--layer",
        type=str,
        default="all",
        help="Camadas a executar: bronze, silver, gold, all (separadas por vírgula)"
    )

    args = parser.parse_args()

    # Parse layers
    if args.layer == "all":
        layers = ["bronze", "silver", "gold"]
    else:
        layers = [l.strip() for l in args.layer.split(",")]

    # Valida layers
    valid_layers = ["bronze", "silver", "gold"]
    for layer in layers:
        if layer not in valid_layers:
            print(f"Erro: camada inválida '{layer}'")
            return

    # Executa
    runner = PipelineRunner()
    runner.run(layers)


if __name__ == "__main__":
    main()
