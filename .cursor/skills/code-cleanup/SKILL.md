---
name: code-cleanup
description: Remove emojis, comments desnecessarios, codigo morto, funcoes nao utilizadas, scripts .sh obsoletos e celulas de notebook fora do fluxo. Garante sequencia de execucao, titulos numerados em celulas e indentacao PEP 8. Usar ao limpar codigo, notebooks, ou quando o usuario solicitar limpeza da base de codigo.
---

# Limpeza de Codigo

## Escopo

Remove elementos dispensaveis e padroniza a base de codigo conforme regras do projeto:
- Emojis em qualquer arquivo
- Comentarios desnecessarios (manter apenas essenciais)
- Codigo morto, funcoes nao utilizadas
- Scripts .sh obsoletos
- Celulas de notebook fora do pipeline ou usadas apenas para teste
- Garante sequencia de execucao em notebooks
- Insere titulo numerado acima de cada celula de codigo
- Indentacao PEP 8 consistente

## Fluxo de Execucao

### 1. Codigo Python e Outros Arquivos

1. Remover todos os emojis (regex: `[\u{1F300}-\u{1F9FF}]` ou equivalente Unicode)
2. Remover comentarios:
   - Manter apenas comentarios essenciais (ex.: explicacao de parametro complexo, workaround documentado)
   - Remover comentarios obvios, TODOs resolvidos, blocos comentados
3. Identificar codigo morto: funcoes/variaveis nao referenciadas em nenhum arquivo do projeto
4. Remover funcoes nao utilizadas apos confirmar ausencia de referencia
5. Aplicar PEP 8: indentacao 4 espacos, linhas ate 88-100 caracteres
6. Condensar codigo apenas quando mantiver legibilidade

### 2. Scripts .sh

1. Listar scripts em `scripts/` e raiz do projeto
2. Identificar scripts obsoletos (nao referenciados em README, pipelines, orchestrator)
3. Verificar referencias com grep antes de propor remocao
4. Propor exclusao apenas de scripts confirmados obsoletos

### 3. Notebooks Jupyter

1. Identificar fluxo do pipeline: setup -> extracao -> transformacao -> carga
2. Remover celulas que:
   - Foram usadas apenas para teste/debug
   - Nao participam do fluxo principal
   - Sao redundantes ou duplicadas
3. Garantir sequencia logica: imports -> config -> extracao -> transformacao -> escrita
4. Inserir titulo markdown acima de cada celula de codigo:
   - Formato: `## N. Descricao breve`
   - N = numero sequencial da celula de codigo (1, 2, 3...)
   - Colocar titulo em celula markdown imediatamente acima da celula de codigo
5. Indentacao: 4 espacos em Python dentro das celulas
6. Limpar outputs antes de finalizar (opcional, informar ao usuario)

## Indentacao Obrigatoria (PEP 8)

Aplicar em todo codigo Python (.py e celulas de notebooks):

| Regra | Valor |
|-------|-------|
| Caractere | Espacos (proibido tab) |
| Tamanho por nivel | 4 espacos |
| Continuacao de linha | Alinhar com abertura ou +4 espacos |

Exemplo:
```
if condicao:
    bloco_nivel_1()
    for item in lista:
        bloco_nivel_2()
```

## Padrao de Titulos em Notebooks

Cada celula de codigo deve ter uma celula markdown anterior com titulo:

```markdown
## 1. Imports e Configuracao
```

```markdown
## 2. Conexao Oracle e Leitura
```

## Checklist de Limpeza

- [ ] Emojis removidos
- [ ] Comentarios desnecessarios removidos
- [ ] Funcoes nao referenciadas identificadas e removidas
- [ ] Scripts .sh obsoletos identificados e removidos
- [ ] Celulas de teste/debug removidas dos notebooks
- [ ] Sequencia de celulas validada
- [ ] Titulos numerados inseridos acima de celulas de codigo
- [ ] Indentacao PEP 8 aplicada

## Regras do Projeto (respeitar)

- Codigo autoexplicativo, sem comentarios desnecessarios
- Sem emojis
- Indentacao: 4 espacos obrigatorio (proibido tab)
- PEP 8 rigoroso
- Funcoes pequenas e deterministicas
- Cobertura Bronze/Silver/Gold na arquitetura
