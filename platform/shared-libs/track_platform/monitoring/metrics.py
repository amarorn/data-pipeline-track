"""
Track Platform - Metrics Collector
Coleta e reporta métricas de pipelines
"""
from typing import Dict, Optional
from datetime import datetime
import json


class MetricsCollector:
    """Coleta métricas de execução de pipelines"""

    def __init__(self, domain: str, layer: str):
        self.domain = domain
        self.layer = layer
        self.metrics: Dict = {
            "domain": domain,
            "layer": layer,
            "start_time": None,
            "end_time": None,
            "status": "running",
            "rows_processed": 0,
            "rows_failed": 0,
            "errors": []
        }

    def start(self):
        """Inicia coleta de métricas"""
        self.metrics["start_time"] = datetime.now().isoformat()

    def end(self, status: str = "success"):
        """Finaliza coleta de métricas"""
        self.metrics["end_time"] = datetime.now().isoformat()
        self.metrics["status"] = status

        # Calcula duração
        if self.metrics["start_time"]:
            start = datetime.fromisoformat(self.metrics["start_time"])
            end = datetime.fromisoformat(self.metrics["end_time"])
            self.metrics["duration_seconds"] = (end - start).total_seconds()

    def add_rows_processed(self, count: int):
        """Adiciona contagem de linhas processadas"""
        self.metrics["rows_processed"] += count

    def add_rows_failed(self, count: int):
        """Adiciona contagem de linhas com erro"""
        self.metrics["rows_failed"] += count

    def add_error(self, error: str):
        """Registra erro"""
        self.metrics["errors"].append({
            "timestamp": datetime.now().isoformat(),
            "error": str(error)
        })

    def get_metrics(self) -> Dict:
        """Retorna métricas coletadas"""
        return self.metrics

    def save_metrics(self, path: str):
        """Salva métricas em arquivo JSON"""
        with open(path, 'w') as f:
            json.dump(self.metrics, f, indent=2)
