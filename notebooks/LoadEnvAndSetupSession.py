import os
import sys
from pathlib import Path

# Carrega .env simples (KEY=VALUE) para o ambiente do notebook
repo_root = Path.cwd()
while repo_root.name != "data-pipeline-track" and repo_root.parent != repo_root:
    repo_root = repo_root.parent

# Adiciona o site-packages do ambiente virtual ao PYTHONPATH (se existir)
venv_path = repo_root / ".venv"
if venv_path.exists():
    site_packages = venv_path / "lib" / "python3.11" / "site-packages"
    if site_packages.exists():
        sys.path.insert(0, str(site_packages))
        print(f"[INFO] Ambiente virtual detectado: {venv_path}")
    else:
        # Tenta encontrar a versão do Python em uso
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
        site_packages = venv_path / "lib" / f"python{python_version}" / "site-packages"
        if site_packages.exists():
            sys.path.insert(0, str(site_packages))
            print(f"[INFO] Ambiente virtual detectado: {venv_path}")

# Garante que o pacote track_platform (platform/shared-libs) esteja no PYTHONPATH
sys.path.insert(0, str(repo_root))
sys.path.insert(0, str(repo_root / "platform" / "shared-libs"))

env_path = repo_root / ".env"
print("Repo root:", repo_root)
print(".env exists?", env_path.exists())

# Diagnóstico: verifica se pyspark está acessível
try:
    import pyspark
    print(f"[OK] pyspark encontrado: {pyspark.__version__}")
except ImportError:
    print("[WARN] pyspark não encontrado no PYTHONPATH atual")
    print(f"[INFO] sys.path: {sys.path[:3]}...")  # Mostra apenas os primeiros 3

if env_path.exists():
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        
        # Remove comentários inline (tudo após # que não está dentro de aspas)
        if "#" in line:
            key_part, value_part = line.split("=", 1)
            value_stripped = value_part.strip()
            
            # Verifica se o valor está entre aspas
            in_quotes = False
            quote_char = None
            comment_pos = -1
            
            for i, char in enumerate(value_part):
                if char in ('"', "'") and (i == 0 or value_part[i-1] != '\\'):
                    if not in_quotes:
                        in_quotes = True
                        quote_char = char
                    elif char == quote_char:
                        in_quotes = False
                        quote_char = None
                elif char == '#' and not in_quotes:
                    comment_pos = i
                    break
            
            if comment_pos >= 0:
                value_part = value_part[:comment_pos].rstrip()
                line = f"{key_part}={value_part}"
        
        k, v = line.split("=", 1)
        value = v.strip()
        # Remove aspas apenas se o valor inteiro estiver entre aspas
        if (value.startswith('"') and value.endswith('"')) or \
           (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        os.environ[k.strip()] = value

# Importa libs do projeto
try:
    from track_platform import SparkSessionManager, OracleConnection
except ImportError as e:
    print(f"[ERRO] Falha ao importar track_platform: {e}")
    print("[INFO] Verifique se pyspark está instalado no ambiente")
    raise

# Diagnóstico opcional (mostra de onde o pacote está sendo importado)
try:
    import track_platform
    print("track_platform:", getattr(track_platform, "__file__", track_platform))
except Exception as e:
    print("[WARN] Não foi possível inspecionar track_platform:", e)

# Cria sessão Spark apenas se pyspark estiver disponível
try:
    # Inclui o driver JDBC do Oracle na sessão Spark (necessário para spark.read.jdbc)
    # Você pode sobrescrever via env ORACLE_JDBC_COORD (ex.: com.oracle.database.jdbc:ojdbc8:21.9.0.0)
    # ou apontar JAR local via EXTRA_JARS (lista separada por vírgula)
    oracle_jdbc_coord = os.getenv("ORACLE_JDBC_COORD", "com.oracle.database.jdbc:ojdbc8:21.9.0.0")
    extra_jars = os.getenv("EXTRA_JARS")  # caminho(s) para .jar locais, separados por vírgula

    spark_extra_config = {
        "spark.jars.packages": oracle_jdbc_coord,
    }
    if extra_jars:
        spark_extra_config["spark.jars"] = extra_jars

    spark = SparkSessionManager.get_session(app_name="Oracle-Exploratorio", config=spark_extra_config)
    print("[OK] SparkSession criada com sucesso")
    
    # Garante que spark esteja no escopo global do notebook
    import __main__
    if hasattr(__main__, '__dict__'):
        __main__.__dict__['spark'] = spark
    
    spark
except Exception as e:
    print(f"[ERRO] Falha ao criar SparkSession: {e}")
    print("[INFO] Certifique-se de que pyspark está instalado: pip install pyspark")
    raise
