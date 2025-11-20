FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y gcc libaio1 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app /app/app
COPY config /config

CMD ["python", "-m", "app.runner"]
FROM python:3.11-slim

# Metadata
LABEL maintainer="Data Team"
LABEL description="Data Pipeline - Medallion Architecture"

# Variáveis de ambiente
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Instala dependências do sistema
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    curl \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Instala Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz \
    && tar xzf spark-3.4.1-bin-hadoop3.tgz -C /opt/ \
    && mv /opt/spark-3.4.1-bin-hadoop3 /opt/spark \
    && rm spark-3.4.1-bin-hadoop3.tgz

# Diretório de trabalho
WORKDIR /app

# Copia requirements e instala dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Baixa JARs necessários (Hadoop AWS, Oracle JDBC, ClickHouse JDBC)
RUN cd $SPARK_HOME/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    wget -q https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/21.9.0.0/ojdbc8-21.9.0.0.jar && \
    wget -q https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-all.jar

# Copia código do projeto
COPY layers/ /app/layers/
COPY shared/ /app/shared/

# Cria diretórios necessários
RUN mkdir -p /app/logs /app/temp /app/metadata

# Comando padrão
CMD ["python", "-m", "shared.runner", "--layer", "all"]