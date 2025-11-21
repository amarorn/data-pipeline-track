"""
Track Platform - Metadata Service
Gerencia lineage, catálogo, contratos e schemas
"""
from typing import Dict, List, Optional
from datetime import datetime
from dataclasses import dataclass, asdict
import json
from pathlib import Path

from track_platform.logging import setup_logger


@dataclass
class TableMetadata:
    """Metadados de uma tabela"""
    domain: str
    layer: str
    name: str
    source_system: str
    owner: str
    description: Optional[str] = None
    pii: bool = False
    retention_days: int = 365
    created_at: datetime = None
    updated_at: datetime = None
    row_count: Optional[int] = None
    size_mb: Optional[float] = None

    def to_dict(self) -> Dict:
        data = asdict(self)
        if self.created_at:
            data['created_at'] = self.created_at.isoformat()
        if self.updated_at:
            data['updated_at'] = self.updated_at.isoformat()
        return data


@dataclass
class DataLineage:
    """Lineage de dados"""
    source_table: str
    target_table: str
    transformation_type: str
    pipeline_name: str
    execution_timestamp: datetime

    def to_dict(self) -> Dict:
        data = asdict(self)
        data['execution_timestamp'] = self.execution_timestamp.isoformat()
        return data


class MetadataService:
    """Serviço centralizado de metadados"""

    def __init__(self, metadata_dir: Optional[Path] = None):
        self.metadata_dir = metadata_dir or Path("platform/metadata")
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        self.logger = setup_logger("MetadataService")

    def register_table(self, metadata: TableMetadata):
        """Registra metadados de uma tabela"""
        table_id = f"{metadata.domain}_{metadata.layer}_{metadata.name}"
        metadata_file = self.metadata_dir / f"{table_id}.json"

        metadata.updated_at = datetime.now()
        if metadata.created_at is None:
            metadata.created_at = datetime.now()

        with open(metadata_file, 'w') as f:
            json.dump(metadata.to_dict(), f, indent=2)

        self.logger.info(f"Metadados registrados: {table_id}")

    def register_lineage(self, lineage: DataLineage):
        """Registra lineage de dados"""
        lineage_file = self.metadata_dir / "lineage.jsonl"

        with open(lineage_file, 'a') as f:
            f.write(json.dumps(lineage.to_dict()) + '\n')

        self.logger.info(
            f"Lineage registrado: {lineage.source_table} → {lineage.target_table}"
        )

    def get_table_metadata(self, domain: str, layer: str, name: str) -> Optional[TableMetadata]:
        """Recupera metadados de uma tabela"""
        table_id = f"{domain}_{layer}_{name}"
        metadata_file = self.metadata_dir / f"{table_id}.json"

        if not metadata_file.exists():
            return None

        with open(metadata_file, 'r') as f:
            data = json.load(f)
            return TableMetadata(**data)

    def get_lineage(self, table_name: str) -> List[DataLineage]:
        """Recupera lineage de uma tabela"""
        lineage_file = self.metadata_dir / "lineage.jsonl"

        if not lineage_file.exists():
            return []

        lineages = []
        with open(lineage_file, 'r') as f:
            for line in f:
                data = json.loads(line)
                if data['source_table'] == table_name or data['target_table'] == table_name:
                    lineages.append(DataLineage(**data))

        return lineages

    def generate_catalog(self) -> Dict:
        """Gera catálogo completo de dados"""
        catalog = {
            "generated_at": datetime.now().isoformat(),
            "tables": []
        }

        for metadata_file in self.metadata_dir.glob("*.json"):
            if metadata_file.name == "lineage.json":
                continue

            with open(metadata_file, 'r') as f:
                catalog["tables"].append(json.load(f))

        # Salva catálogo
        catalog_file = self.metadata_dir / "catalog.json"
        with open(catalog_file, 'w') as f:
            json.dump(catalog, f, indent=2)

        self.logger.info(f"Catálogo gerado com {len(catalog['tables'])} tabelas")

        return catalog


if __name__ == "__main__":
    # Exemplo de uso
    service = MetadataService()

    # Registra metadados
    metadata = TableMetadata(
        domain="data-pipeline",
        layer="bronze",
        name="oracle_customers",
        source_system="oracle",
        owner="data-team@track.com",
        description="Tabela de clientes do Oracle",
        pii=True,
        retention_days=2555
    )
    service.register_table(metadata)

    # Registra lineage
    lineage = DataLineage(
        source_table="bronze.oracle_customers",
        target_table="silver.sales.customers",
        transformation_type="clean_and_deduplicate",
        pipeline_name="data-pipeline-silver",
        execution_timestamp=datetime.now()
    )
    service.register_lineage(lineage)

    # Gera catálogo
    service.generate_catalog()
