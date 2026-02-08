#!/bin/bash
# Script para diagnosticar e corrigir problema de porta do Jupyter

echo "=========================================="
echo "🔍 DIAGNÓSTICO JUPYTER"
echo "=========================================="

# 1. Verificar container
echo -e "\n1️⃣ Status do container:"
docker ps | grep jupyter

# 2. Verificar portas do container
echo -e "\n2️⃣ Portas expostas:"
docker port track-jupyter

# 3. Verificar se porta 8889 está escutando no host
echo -e "\n3️⃣ Porta 8889 no host:"
netstat -an | grep 8889 || ss -tulpn | grep 8889

# 4. Testar conexão local no servidor
echo -e "\n4️⃣ Teste local (servidor):"
curl -I http://localhost:8889 2>&1 | head -5

# 5. Ver logs do Jupyter
echo -e "\n5️⃣ Últimos logs do Jupyter:"
docker logs track-jupyter --tail 30

# 6. Ver variáveis de ambiente do container
echo -e "\n6️⃣ Variáveis de ambiente:"
docker exec track-jupyter env | grep -E "(PORT|JUPYTER)"

# 7. Verificar processo Jupyter dentro do container
echo -e "\n7️⃣ Processos Jupyter no container:"
docker exec track-jupyter ps aux | grep jupyter

echo -e "\n=========================================="
echo "✅ Diagnóstico completo!"
echo "=========================================="
