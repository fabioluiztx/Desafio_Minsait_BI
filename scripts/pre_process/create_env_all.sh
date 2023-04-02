#!/bin/bash

# Criação das pastas

DADOS=("vendas" "clientes" "endereco" "regiao" "divisao")

echo "Criando a estrutura de pastas no hdfs"

for i in "${DADOS[@]}"
do
	echo "Criando a pasta datalake/raw/$i"
    cd ../../raw/
    hdfs dfs -mkdir /datalake/raw/$i
    hdfs dfs -chmod 777 /datalake/raw/$i
	
	echo "Enviando arquivo $i para datalake/raw/$i"
    hdfs dfs -copyFromLocal $i.csv /datalake/raw/$i
    beeline -u jdbc:hive2://localhost:10000 -f ../../scripts/hql/create_table_$i.hql 
done
