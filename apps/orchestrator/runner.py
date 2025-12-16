from __future__ import annotations

import argparse
import importlib.util
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import yaml

from track_platform.logging import setup_logger


class PipelineOrchestrator:
    def __init__(self, config_path: Optional[Path] = None):
        self.logger = setup_logger("orchestrator")
        self.config = self._load_config(config_path)

    def _load_config(self, config_path: Optional[Path]) -> Dict:
        path = config_path or Path(__file__).parent / "configs" / "pipelines.yaml"
        if not path.exists():
            raise FileNotFoundError(f"Configuração não encontrada em {path}")
        config = yaml.safe_load(path.read_text())
        if not config or "domains" not in config:
            raise ValueError("Configuração de domínios inválida")
        return config

    def _resolve_domain(self, domain_name: str) -> Dict:
        domains = self.config.get("domains", [])
        for domain_config in domains:
            if domain_config.get("name") == domain_name:
                return domain_config
        raise ValueError(f"Domínio {domain_name} não configurado")

    def _resolve_layers(self, domain_config: Dict, target_layer: Optional[str]) -> List[str]:
        layers = domain_config.get("layers", [])
        if not layers:
            raise ValueError(f"Domínio {domain_config.get('name')} sem camadas configuradas")
        if target_layer is None or target_layer == "all":
            return layers
        if target_layer not in layers:
            raise ValueError(
                f"Camada {target_layer} não configurada para {domain_config.get('name')}"
            )
        return [target_layer]

    def _execute_runner(self, domain_name: str, layer_name: str) -> None:
        runner_path = Path("domains") / domain_name / layer_name / "runner.py"
        if not runner_path.exists():
            raise FileNotFoundError(f"Runner inexistente em {runner_path}")
        spec = importlib.util.spec_from_file_location(
            f"{domain_name}_{layer_name}_runner", str(runner_path)
        )
        if spec is None or spec.loader is None:
            raise ImportError(f"Não foi possível carregar {runner_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        run_callable = getattr(module, "run", None)
        if run_callable is None:
            raise AttributeError(f"Runner {runner_path} sem função run")
        run_callable()

    def run_domain(self, domain_name: str, layer: Optional[str] = None) -> None:
        domain_config = self._resolve_domain(domain_name)
        if not domain_config.get("enabled", False):
            self.logger.info(f"Domínio {domain_name} desabilitado")
            return
        layers = self._resolve_layers(domain_config, layer)
        for layer_name in layers:
            start_time = datetime.now()
            self.logger.info(f"Início {domain_name}/{layer_name}")
            try:
                self._execute_runner(domain_name, layer_name)
                duration = (datetime.now() - start_time).total_seconds()
                self.logger.info(f"Conclusão {domain_name}/{layer_name} em {duration:.2f}s")
            except Exception as exc:
                self.logger.error(f"Falha em {domain_name}/{layer_name}: {exc}")
                if domain_config.get("fail_fast", False):
                    raise

    def run_all_domains(self, layer: Optional[str] = None) -> None:
        start_time = datetime.now()
        for domain_config in self.config.get("domains", []):
            domain_name = domain_config.get("name")
            if domain_name is None:
                continue
            try:
                self.run_domain(domain_name, layer)
            except Exception as exc:
                self.logger.error(f"Domínio {domain_name} interrompido: {exc}")
                if domain_config.get("fail_fast", False):
                    raise
        duration = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"Execução completa em {duration:.2f}s")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Track Platform Orchestrator")
    parser.add_argument("--domain", type=str, help="Domínio alvo")
    parser.add_argument(
        "--layer",
        type=str,
        choices=["bronze", "silver", "gold", "all"],
        default="all",
    )
    parser.add_argument("--config", type=str, help="Caminho para configuração")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    orchestrator = PipelineOrchestrator(config_path=Path(args.config) if args.config else None)
    target_layer = args.layer if args.layer != "all" else None
    if args.domain:
        orchestrator.run_domain(args.domain, target_layer)
    else:
        orchestrator.run_all_domains(target_layer)


if __name__ == "__main__":
    main()
