# Guia de Uso de Notebooks Jupyter no Cursor

## 1. Instalação da Extensão Jupyter

### Passo a Passo:
1. Abra o Cursor
2. Pressione `Cmd+Shift+X` (Mac) ou `Ctrl+Shift+X` (Windows/Linux) para abrir a paleta de extensões
3. Procure por: `Jupyter`
4. Instale a extensão oficial: **Jupyter** (ID: `ms-toolsai.jupyter`, publicada por `ms-toolsai`)

### Verificação:
- Após instalar, os arquivos `.ipynb` devem ser reconhecidos automaticamente
- Você verá ícones de execução nas células dos notebooks

## 2. Configuração do Ambiente Python

### Selecionar Interpretador:
1. Abra qualquer notebook (ex: `notebooks/00_setup_and_ddl.ipynb`)
2. Clique no seletor de kernel no canto superior direito do notebook
3. Selecione o interpretador Python do seu ambiente virtual ou conda

### Configurar Variáveis de Ambiente:
- Certifique-se de que o Cursor está usando o mesmo ambiente onde as dependências estão instaladas
- Verifique `requirements-jupyter.txt` para garantir todas as bibliotecas necessárias

## 3. Funcionalidades Disponíveis

### Execução Inline de Células:
- **Executar célula**: Clique no botão "Run" acima da célula ou pressione `Shift+Enter`
- **Executar e avançar**: `Shift+Enter`
- **Executar e inserir abaixo**: `Alt+Enter`
- **Interromper**: Botão "Stop" ou `Cmd/Ctrl+C`

### Tab (Autocompletar):
- Funciona normalmente em notebooks como em arquivos `.py`
- Reconhece bibliotecas de ciência de dados: `pandas`, `numpy`, `scikit-learn`, etc.
- Suporta comandos mágicos SQL quando `ipython-sql` está instalado

### Inline Edit:
- Clique duas vezes em qualquer célula para editar inline
- Funciona com código Python e markdown

### Agents (IA):
- Use `Cmd+K` (Mac) ou `Ctrl+K` (Windows/Linux) para edição assistida
- Use `Cmd+L` (Mac) ou `Ctrl+L` (Windows/Linux) para chat com o agente
- O agente reconhece o contexto do notebook e pode ajudar com:
  - Transformações de dados
  - Queries SQL
  - Visualizações
  - Debugging

## 4. Integração com Bancos de Dados

### Via MCP (Model Context Protocol):
O Cursor suporta integração com bancos de dados através de servidores MCP.

#### Para ClickHouse:
1. Configure um servidor MCP para ClickHouse (se disponível)
2. O agente poderá se conectar e executar queries diretamente
3. Use comandos mágicos SQL no notebook: `%sql` ou `%%sql`

#### Exemplo de Uso com SQL Magic no Projeto:

**Opção 1: Usando ipython-sql com ClickHouse:**
```python
%load_ext sql
from config.settings import clickhouse_config

connection_string = f"clickhouse://{clickhouse_config.user}:{clickhouse_config.password}@{clickhouse_config.host}:{clickhouse_config.port}/{clickhouse_config.database}?secure={clickhouse_config.secure}"
%sql $connection_string

%%sql
SELECT 
    ref_date,
    table_name,
    count(*) as total_rows
FROM bronze.snapshot_raw
WHERE ref_date >= today() - 7
GROUP BY ref_date, table_name
ORDER BY ref_date DESC
```

**Opção 2: Usando ClickHouseClient (já configurado no projeto):**
```python
from connectors.clickhouse_client import ClickHouseClient
from config.settings import clickhouse_config

client = ClickHouseClient()
result = client.execute_query_with_result("""
    SELECT * FROM bronze.snapshot_raw 
    WHERE ref_date = today() 
    LIMIT 10
""")
print(result.result_rows)
```

**Opção 3: Integração com pandas:**
```python
import pandas as pd
from connectors.clickhouse_client import ClickHouseClient

client = ClickHouseClient()
df = pd.DataFrame(client.execute_query_with_result("""
    SELECT * FROM bronze.snapshot_raw 
    WHERE ref_date >= today() - 30
""").result_rows, 
columns=client.execute_query_with_result("""
    SELECT * FROM bronze.snapshot_raw LIMIT 0
""").column_names)
```

### Via Extensões:
- Instale extensões específicas do banco de dados (ex: ClickHouse, Oracle)
- Essas extensões integram o Editor aos bancos de dados
- Permitem visualização de schemas, execução de queries, etc.

## 5. Boas Práticas para Notebooks no Cursor

### Estrutura:
- Use células markdown para documentação
- Mantenha células de código pequenas e focadas
- Execute células na ordem correta

### Performance:
- Evite `collect()`, `show()`, `count()` em transformações Spark
- Use visualizações apenas quando necessário
- Limpe outputs grandes para manter o notebook leve

### Versionamento:
- Limpe outputs antes de commitar (use "Clear All Outputs")
- Mantenha células executáveis e determinísticas
- Documente dependências e pré-requisitos

## 6. Troubleshooting

### Kernel não encontrado:
- Verifique se o Python está instalado e no PATH
- Selecione manualmente o interpretador: `Cmd+Shift+P` → "Python: Select Interpreter"

### Extensão não funciona:
- Reinicie o Cursor
- Verifique se a extensão Jupyter está habilitada
- Atualize a extensão para a versão mais recente

### Imports não funcionam:
- Verifique o `sys.path` nas primeiras células
- Configure o PYTHONPATH no ambiente
- Use caminhos absolutos se necessário

## 7. Exemplos Práticos para o Projeto

### Executando DDLs no ClickHouse:
```python
from connectors.clickhouse_client import ClickHouseClient

client = ClickHouseClient()

ddl = """
CREATE TABLE IF NOT EXISTS bronze.snapshot_raw
(
    ref_date Date,
    table_name String,
    primary_key String,
    row_hash String,
    data String,
    ingestion_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ref_date)
ORDER BY (ref_date, table_name, primary_key)
"""

client.execute_query(ddl)
```

### Usando Agents para Gerar Queries:
1. Selecione uma célula com código Python ou SQL
2. Pressione `Cmd+K` (Mac) ou `Ctrl+K` (Windows/Linux)
3. Digite: "Otimize esta query para performance no ClickHouse"
4. O agente irá sugerir melhorias baseadas nas melhores práticas

### Debugging com Inline Edit:
1. Execute uma célula que retorna erro
2. Clique duas vezes no erro ou na célula
3. Use Tab para autocompletar correções
4. O agente pode sugerir fixes automaticamente

### Visualização de Dados:
```python
import pandas as pd
import matplotlib.pyplot as plt
from connectors.clickhouse_client import ClickHouseClient

client = ClickHouseClient()
result = client.execute_query_with_result("""
    SELECT ref_date, count(*) as cnt 
    FROM bronze.snapshot_raw 
    GROUP BY ref_date 
    ORDER BY ref_date
""")

df = pd.DataFrame(result.result_rows, columns=['ref_date', 'cnt'])
df.plot(x='ref_date', y='cnt', kind='line')
plt.show()
```

## 8. Recursos Adicionais

### Comandos Úteis:
- `Cmd+Shift+P` → "Jupyter: Select Interpreter" - Selecionar kernel
- `Cmd+Shift+P` → "Jupyter: Clear All Outputs" - Limpar outputs
- `Cmd+Shift+P` → "Jupyter: Export" - Exportar notebook

### Atalhos de Teclado:
- `Shift+Enter`: Executar célula
- `Alt+Enter`: Executar e inserir abaixo
- `Ctrl+Enter`: Executar célula sem avançar
- `A`: Inserir célula acima
- `B`: Inserir célula abaixo
- `DD`: Deletar célula
