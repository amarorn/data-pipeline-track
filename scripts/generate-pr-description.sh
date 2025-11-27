#!/bin/bash

set -e

BRANCH_NAME=${1:-$(git branch --show-current)}
BASE_BRANCH=${2:-main}

if ! git rev-parse --verify "$BASE_BRANCH" > /dev/null 2>&1; then
    echo "Erro: branch base '$BASE_BRANCH' não encontrada"
    exit 1
fi

BASE_SHA=$(git merge-base "$BASE_BRANCH" "$BRANCH_NAME" 2>/dev/null || git rev-parse "$BASE_BRANCH")
HEAD_SHA=$(git rev-parse "$BRANCH_NAME")

echo "## 📋 Resumo do Pull Request" > pr_description.md
echo "" >> pr_description.md
echo "**Branch**: \`$BRANCH_NAME\` → \`$BASE_BRANCH\`" >> pr_description.md
echo "" >> pr_description.md

echo "### 🔍 Escopo das Mudanças" >> pr_description.md
echo "" >> pr_description.md

COMMITS=$(git log --format="%s" ${BASE_SHA}..${HEAD_SHA})

if [ -z "$COMMITS" ]; then
    echo "⚠️ Nenhum commit encontrado entre $BASE_BRANCH e $BRANCH_NAME" >> pr_description.md
    cat pr_description.md
    exit 0
fi

FEATURES=()
FIXES=()
REFACTORS=()
DOCS=()
CHORES=()
OTHERS=()

while IFS= read -r commit_msg; do
    if [[ $commit_msg =~ ^(feat|feature)(\(.+\))?: ]]; then
        FEATURES+=("$commit_msg")
    elif [[ $commit_msg =~ ^(fix|bugfix)(\(.+\))?: ]]; then
        FIXES+=("$commit_msg")
    elif [[ $commit_msg =~ ^(refactor|refactoring)(\(.+\))?: ]]; then
        REFACTORS+=("$commit_msg")
    elif [[ $commit_msg =~ ^(docs|documentation)(\(.+\))?: ]]; then
        DOCS+=("$commit_msg")
    elif [[ $commit_msg =~ ^(chore|ci|build)(\(.+\))?: ]]; then
        CHORES+=("$commit_msg")
    else
        OTHERS+=("$commit_msg")
    fi
done <<< "$COMMITS"

if [ ${#FEATURES[@]} -gt 0 ]; then
    echo "#### ✨ Novas Funcionalidades" >> pr_description.md
    for feat in "${FEATURES[@]}"; do
        echo "- $feat" >> pr_description.md
    done
    echo "" >> pr_description.md
fi

if [ ${#FIXES[@]} -gt 0 ]; then
    echo "#### 🐛 Correções" >> pr_description.md
    for fix in "${FIXES[@]}"; do
        echo "- $fix" >> pr_description.md
    done
    echo "" >> pr_description.md
fi

if [ ${#REFACTORS[@]} -gt 0 ]; then
    echo "#### ♻️ Refatorações" >> pr_description.md
    for ref in "${REFACTORS[@]}"; do
        echo "- $ref" >> pr_description.md
    done
    echo "" >> pr_description.md
fi

if [ ${#DOCS[@]} -gt 0 ]; then
    echo "#### 📚 Documentação" >> pr_description.md
    for doc in "${DOCS[@]}"; do
        echo "- $doc" >> pr_description.md
    done
    echo "" >> pr_description.md
fi

if [ ${#CHORES[@]} -gt 0 ]; then
    echo "#### 🔧 Chores / CI / Build" >> pr_description.md
    for chore in "${CHORES[@]}"; do
        echo "- $chore" >> pr_description.md
    done
    echo "" >> pr_description.md
fi

if [ ${#OTHERS[@]} -gt 0 ]; then
    echo "#### 📝 Outras Mudanças" >> pr_description.md
    for other in "${OTHERS[@]}"; do
        echo "- $other" >> pr_description.md
    done
    echo "" >> pr_description.md
fi

echo "### 📊 Estatísticas" >> pr_description.md
echo "" >> pr_description.md

TOTAL_COMMITS=$(git rev-list --count ${BASE_SHA}..${HEAD_SHA})
FILES_CHANGED=$(git diff --name-only ${BASE_SHA}..${HEAD_SHA} 2>/dev/null | wc -l | tr -d ' ')
STATS=$(git diff --shortstat ${BASE_SHA}..${HEAD_SHA} 2>/dev/null || echo "0 files changed")

echo "- **Total de commits**: $TOTAL_COMMITS" >> pr_description.md
echo "- **Arquivos alterados**: $FILES_CHANGED" >> pr_description.md
echo "- **Estatísticas**: $STATS" >> pr_description.md
echo "" >> pr_description.md

echo "### 🔗 Commits Incluídos" >> pr_description.md
echo "" >> pr_description.md
git log --format="- %s (%h)" ${BASE_SHA}..${HEAD_SHA} >> pr_description.md

echo ""
echo "✅ Descrição do PR gerada em: pr_description.md"
echo ""
echo "📋 Preview:"
echo "---"
cat pr_description.md

