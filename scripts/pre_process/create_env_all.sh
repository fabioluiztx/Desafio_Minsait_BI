#!/bin/bash

#Trocando os espaços em brancos do header por underline ("_") para manipulação das tabelas no hive
TABELAS=("CLIENTES" "DIVISAO" "ENDERECO" "REGIAO" "VENDAS")

for i in "${TABELAS[@]}"

do
    echo "Alterando o header das colunas do $i.CSV para manipulação"
    chmod 777 $i.csv
    sed -i '1,1s/ /_/g' $i.csv

done

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
