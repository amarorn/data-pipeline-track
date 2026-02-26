import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
from dotenv import load_dotenv

env_path = project_root / ".env"
load_dotenv(env_path)

print("Variáveis de ambiente do ClickHouse:")
print(f"CLICKHOUSE_HOST: {os.getenv('CLICKHOUSE_HOST', 'NÃO DEFINIDO')}")
print(f"CLICKHOUSE_PORT: {os.getenv('CLICKHOUSE_PORT', 'NÃO DEFINIDO')}")
print(f"CLICKHOUSE_USER: {os.getenv('CLICKHOUSE_USER', 'NÃO DEFINIDO')}")
print(f"CLICKHOUSE_PASSWORD: {'*' * len(os.getenv('CLICKHOUSE_PASSWORD', '')) if os.getenv('CLICKHOUSE_PASSWORD') else 'NÃO DEFINIDO'}")
print(f"CLICKHOUSE_DATABASE: {os.getenv('CLICKHOUSE_DATABASE', 'NÃO DEFINIDO')}")
print(f"CLICKHOUSE_SECURE: {os.getenv('CLICKHOUSE_SECURE', 'NÃO DEFINIDO')}")
print(f"CLICKHOUSE_VERIFY: {os.getenv('CLICKHOUSE_VERIFY', 'NÃO DEFINIDO')}")
print(f"\nArquivo .env existe: {env_path.exists()}")
if env_path.exists():
    print(f"Caminho do .env: {env_path}")

