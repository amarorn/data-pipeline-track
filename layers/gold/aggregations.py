"""
Pipeline de agregações e métricas para camada Gold
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, max as _max, min as _min,
    countDistinct, first, last, current_timestamp
)
from typing import List, Dict, Optional

from shared.infrastructure.storage.storage_manager import StorageManager
from shared.infrastructure.spark.spark_session import SparkSessionManager
from shared.utils.logger import get_pipeline_logger


class GoldAggregations:
    """Cria agregações e métricas para camada Gold"""

    def __init__(self):
        self.spark = SparkSessionManager.get_session(app_name="Gold-Aggregations")
        self.storage = StorageManager()
        self.logger = get_pipeline_logger("gold", "aggregations")

    def create_aggregation(
        self,
        domain: str,
        entity: str,
        group_by: List[str],
        aggregations: Dict[str, List[str]],
        filters: Optional[Dict] = None
    ) -> DataFrame:
        """
        Cria agregação customizada

        Args:
            domain: Domínio de negócio
            entity: Entidade na Silver
            group_by: Colunas para agrupar
            aggregations: Dict {coluna: [funções]}
                         Ex: {"valor": ["sum", "avg"], "id": ["count"]}
            filters: Filtros a aplicar
        """
        self.logger.info(f"Criando agregação para {domain}.{entity}")

        # Lê da Silver
        df = self.storage.read_silver(domain=domain, entity=entity)

        # Aplica filtros
        if filters:
            for column, value in filters.items():
                df = df.filter(col(column) == value)

        # Prepara agregações
        agg_exprs = []

        for column, functions in aggregations.items():
            for func in functions:
                if func == "count":
                    agg_exprs.append(count(col(column)).alias(f"{column}_count"))
                elif func == "sum":
                    agg_exprs.append(_sum(col(column)).alias(f"{column}_sum"))
                elif func == "avg":
                    agg_exprs.append(avg(col(column)).alias(f"{column}_avg"))
                elif func == "max":
                    agg_exprs.append(_max(col(column)).alias(f"{column}_max"))
                elif func == "min":
                    agg_exprs.append(_min(col(column)).alias(f"{column}_min"))
                elif func == "countDistinct":
                    agg_exprs.append(
                        countDistinct(col(column)).alias(f"{column}_distinct")
                    )

        # Executa agregação
        result = df.groupBy(*group_by).agg(*agg_exprs)

        # Adiciona timestamp
        result = result.withColumn("_aggregation_timestamp", current_timestamp())

        count = result.count()
        self.logger.info(f"Agregação resultou em {count} registros")

        return result

    def save_aggregation(
        self,
        df: DataFrame,
        domain: str,
        metric_name: str
    ):
        """
        Salva agregação na camada Gold

        Args:
            df: DataFrame agregado
            domain: Domínio
            metric_name: Nome da métrica
        """
        path = self.storage.write_gold(
            df=df,
            domain=domain,
            entity=metric_name,
            mode="overwrite"
        )

        self.logger.info(f"Agregação salva em: {path}")

        return path

    def create_daily_summary(
        self,
        domain: str,
        entity: str,
        date_column: str,
        metrics: Dict[str, List[str]]
    ):
        """
        Cria resumo diário

        Args:
            domain: Domínio
            entity: Entidade
            date_column: Coluna de data
            metrics: Métricas a agregar
        """
        df = self.create_aggregation(
            domain=domain,
            entity=entity,
            group_by=[date_column],
            aggregations=metrics
        )

        self.save_aggregation(
            df=df,
            domain=domain,
            metric_name=f"{entity}_daily_summary"
        )

        return df


if __name__ == "__main__":
    # Exemplo de uso
    gold = GoldAggregations()

    # Cria resumo diário de vendas
    gold.create_daily_summary(
        domain="vendas",
        entity="pedidos",
        date_column="data_pedido",
        metrics={
            "valor": ["sum", "avg", "max", "min"],
            "pedido_id": ["count"],
            "cliente_id": ["countDistinct"]
        }
    )
