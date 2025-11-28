#!/bin/bash
# scripts/gateway.sh
# Script para compilar e rodar o GatewayServer

# Ir para a raiz do projeto
cd "$(dirname "$0")/.." || exit 1

echo "Construindo projeto com Maven..."
mvn clean package

if [ $? -ne 0 ]; then
    echo "Erro ao construir o projeto!"
    exit 1
fi

echo "Iniciando GatewayServer..."
java -jar target/gateway-jar-with-dependencies.jar
