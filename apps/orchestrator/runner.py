"""
Track Platform - Orquestrador Central de Pipelines
Dispara pipelines entre domínios e camadas
"""
import argparse
import yaml
from pathlib import Path
from typing import List, Dict
from datetime import datetime
import sys

from track_platform.logging import setup_logger


class PipelineOrchestrator:
    """Orquestra execução de pipelines entre domínios"""

    def __init__(self, config_path: Optional[Path] = None):
        self.logger = setup_logger(
            "Orchestrator",
            log_file=Path("logs") / f"orchestrator_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        self.config = self._load_config(config_path)

    def _load_config(self, config_path: Optional[Path]) -> Dict:
        """Carrega configuração de orquestração"""
        if config_path is None:
            config_path = Path(__file__).parent / "configs" / "pipelines.yaml"

        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def run_domain_pipeline(self, domain: str, layer: str):
        """
        Executa pipeline de um domínio específico

        Args:
            domain: Nome do domínio (ex: data-pipeline)
            layer: Camada (bronze, silver, gold)
        """
        self.logger.info(f"=" * 60)
        self.logger.info(f"Executando: {domain} - {layer}")
        self.logger.info(f"=" * 60)

        try:
            # Importa dinamicamente o runner do domínio
            domain_module = f"domains.{domain.replace('-', '_')}.{layer}.runner"
            module = __import__(domain_module, fromlist=['run'])

            # Executa
            module.run()

            self.logger.info(f"✓ {domain}/{layer} concluído com sucesso")

        except Exception as e:
            self.logger.error(f"✗ Erro em {domain}/{layer}: {str(e)}")
            raise

    def run_all_domains(self, layer: Optional[str] = None):
        """
        Executa todos os domínios configurados

        Args:
            layer: Se especificado, executa apenas essa camada
        """
        start_time = datetime.now()

        domains = self.config.get('domains', [])
        layers = [layer] if layer else ['bronze', 'silver', 'gold']

        for domain in domains:
            for layer_name in layers:
                try:
                    self.run_domain_pipeline(domain['name'], layer_name)
                except Exception as e:
                    if domain.get('fail_fast', False):
                        raise
                    continue

        duration = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"Pipeline completo executado em {duration:.2f}s")


def main():
    """Ponto de entrada CLI"""
    parser = argparse.ArgumentParser(
        description="Track Platform - Orquestrador de Pipelines"
    )
    parser.add_argument(
        "--domain",
        type=str,
        help="Domínio específico (ex: data-pipeline)"
    )
    parser.add_argument(
        "--layer",
        type=str,
        choices=['bronze', 'silver', 'gold', 'all'],
        default='all',
        help="Camada a executar"
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Caminho para arquivo de configuração customizado"
    )

    args = parser.parse_args()

    # Cria orquestrador
    config_path = Path(args.config) if args.config else None
    orchestrator = PipelineOrchestrator(config_path=config_path)

    # Executa
    if args.domain:
        layer = args.layer if args.layer != 'all' else None
        orchestrator.run_domain_pipeline(args.domain, layer or 'bronze')
    else:
        layer = args.layer if args.layer != 'all' else None
        orchestrator.run_all_domains(layer=layer)


if __name__ == "__main__":
    main()
