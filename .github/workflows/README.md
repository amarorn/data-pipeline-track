# GitHub Actions - Auto PR Description

## 📋 Descrição

Este workflow automaticamente gera e atualiza a descrição de Pull Requests baseado nos commits incluídos no PR.

## 🚀 Como Funciona

Quando um PR é aberto, atualizado ou reaberto, o workflow:

1. Analisa todos os commits entre a branch base e a branch do PR
2. Categoriza os commits usando Conventional Commits:
   - `feat:` / `feature:` → ✨ Novas Funcionalidades
   - `fix:` / `bugfix:` → 🐛 Correções
   - `refactor:` / `refactoring:` → ♻️ Refatorações
   - `docs:` / `documentation:` → 📚 Documentação
   - `chore:` / `ci:` / `build:` → 🔧 Chores / CI / Build
   - Outros → 📝 Outras Mudanças
3. Gera estatísticas (total de commits, arquivos alterados, linhas)
4. Lista todos os commits incluídos
5. Atualiza automaticamente a descrição do PR

## 📝 Formato de Commits Recomendado

Para melhor categorização, use o formato Conventional Commits:

```
feat(scope): descrição da funcionalidade
fix(scope): descrição da correção
refactor(scope): descrição da refatoração
docs(scope): descrição da documentação
chore(scope): descrição do chore
```

Exemplos:
- `feat(pipeline): adiciona ingestão Oracle para Bronze`
- `fix(docker): corrige versão do Java no orchestrator`
- `refactor(notebooks): reorganiza células do notebook de setup`

## 🔧 Uso Local

Antes de abrir um PR, você pode gerar a descrição localmente:

```bash
./scripts/generate-pr-description.sh [branch-name] [base-branch]
```

Exemplo:
```bash
./scripts/generate-pr-description.sh feature/nova-funcionalidade main
```

Isso gera um arquivo `pr_description.md` que você pode copiar e colar ao criar o PR manualmente, ou revisar antes do workflow automático atualizar.

