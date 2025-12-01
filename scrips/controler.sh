#!/bin/bash
# scripts/controler.sh
# Script para compilar e rodar o ControleServer

# Ir para a raiz do projeto (onde está o pom.xml)
cd "$(dirname "$0")/.." || exit 1

echo "Construindo projeto com Maven..."
mvn clean package -Dmaven.test.skip=true

if [ $? -ne 0 ]; then
    echo "Erro ao construir o projeto!"
    exit 1
fi

echo "Iniciando ControleServer..."
java -jar target/controle-jar-with-dependencies.jar
