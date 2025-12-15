#!/usr/bin/env python3
"""
Valida configurações YAML dos domínios
Garante que configs estão corretos e completos
"""
import sys
import yaml
from pathlib import Path
from typing import Dict, List


def validate_table_config(config: Dict, domain_path: Path) -> List[str]:
    """Valida configuração de tabela"""
    errors = []

    required_fields = ['name', 'source', 'ingestion', 'metadata']

    for table in config.get('bronze_tables', []):
        # Campos obrigatórios
        for field in required_fields:
            if field not in table:
                errors.append(f"Tabela {table.get('name', 'unknown')}: campo '{field}' obrigatório")

        # Valida source
        if 'source' in table:
            if 'type' not in table['source']:
                errors.append(f"Tabela {table['name']}: source.type obrigatório")

        # Proíbe incremental no domínio data-pipeline
        if table.get('ingestion', {}).get('mode') != 'snapshot_full':
            errors.append(
                f"Tabela {table.get('name','unknown')}: ingestion.mode deve ser 'snapshot_full'"
            )

        # Exige reference_date no domínio data-pipeline
        if 'reference_date' not in table.get('ingestion', {}):
            errors.append(
                f"Tabela {table.get('name','unknown')}: ingestion.reference_date obrigatório"
            )

        # Valida metadata
        if 'metadata' in table:
            if 'owner' not in table['metadata']:
                errors.append(f"Tabela {table['name']}: metadata.owner obrigatório")

    return errors


def validate_domain_configs(domain_path: Path) -> bool:
    """Valida todos os configs de um domínio"""
    print(f"📁 Validando domínio: {domain_path.name}")

    configs_dir = domain_path / "configs"
    if not configs_dir.exists():
        print(f"  ⚠️  Sem pasta configs/")
        return True

    all_valid = True

    for config_file in configs_dir.glob("*.yaml"):
        print(f"  📄 {config_file.name}...", end=" ")

        try:
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)

            # Valida estrutura
            errors = validate_table_config(config, domain_path)

            if errors:
                print("❌")
                for error in errors:
                    print(f"      ↳ {error}")
                all_valid = False
            else:
                print("✅")

        except yaml.YAMLError as e:
            print(f"❌ Erro YAML: {e}")
            all_valid = False
        except Exception as e:
            print(f"❌ Erro: {e}")
            all_valid = False

    return all_valid


def main():
    """Valida configs de todos os domínios"""
    print("🔍 Validando configurações dos domínios\n")

    domains_dir = Path("domains")

    if not domains_dir.exists():
        print("❌ Pasta domains/ não encontrada")
        sys.exit(1)

    all_valid = True

    for domain_path in domains_dir.iterdir():
        if domain_path.is_dir() and not domain_path.name.startswith('.'):
            if not validate_domain_configs(domain_path):
                all_valid = False

    print()
    if all_valid:
        print("✅ Todas as configurações estão válidas!")
        sys.exit(0)
    else:
        print("❌ Erros encontrados nas configurações")
        sys.exit(1)


if __name__ == "__main__":
    main()
