# Como Visualizar Função Oracle

## Opção 1: Jupyter Lab (Recomendado - Usa Spark)

1. **Acesse o Jupyter Lab:**
   ```bash
   # Jupyter já está rodando, acesse:
   http://localhost:8888/lab
   ```

2. **Abra o notebook:**
   - Navegue até `notebooks/view_oracle_function.ipynb`
   - Execute as células sequencialmente

3. **O notebook detectará automaticamente:**
   - Se estiver no Docker: usa Spark/JDBC (funciona sem Oracle Client)
   - Se estiver local: tenta usar oracledb (pode falhar sem Oracle Client)

## Opção 2: Query SQL Manual

Execute em qualquer cliente SQL conectado ao Oracle:

```sql
SELECT line, text
FROM all_source
WHERE owner = UPPER('BISTAGE')
  AND name = UPPER('F_CONSULTA_INST_BI')
  AND type = 'FUNCTION'
ORDER BY line;
```

Arquivo pronto: `scripts/view_oracle_function_sql.sql`

## Opção 3: Script Python Local

```bash
python scripts/view_oracle_function.py
```

**Nota:** Requer Oracle Instant Client se o Oracle exigir criptografia nativa.

## Troubleshooting

### Erro: "PySpark não disponível"
- **Causa:** Notebook executado localmente, não no Jupyter Lab
- **Solução:** Acesse http://localhost:8888/lab e execute lá

### Erro: "DPY-3001: Native Network Encryption"
- **Causa:** Oracle exige criptografia nativa (modo thick)
- **Solução:** Use Jupyter Lab (Spark/JDBC) ou instale Oracle Instant Client

### Erro: "DPY-4011: connection closed"
- **Causa:** Túnel SSH não está funcionando ou Oracle rejeitou conexão
- **Solução:** Verifique túnel SSH: `lsof -i :1521`
