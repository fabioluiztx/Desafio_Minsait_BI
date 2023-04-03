#!/bin/bash

#criando a pasta datalake e a gold

hdfs dfs -mkdir /datalake/
hdfs dfs -mkdir /datalake/raw
hdfs dfs -mkdir /datalake/gold

#Escolhendo o diretório raw
#Trocando os espaços em brancos do header por underline ("_") para manipulação das tabelas no hive
#e colocando em upper case pra bater com o hql modificado

cd workspace/Desafio_Minsait_BI/raw/

TABELAS=("CLIENTES" "DIVISAO" "ENDERECO" "REGIAO" "VENDAS")

#Percorrendo as tabelas
for j in "${TABELAS[@]}"

do
    echo "Alterando o header das colunas do $i.CSV para manipulação"
    chmod 777 $j.csv #dando permissão para modificar o arquivo
    sed -i '1,1s/ /_/g' $j.csv #percorrendo apenas a primerira linha da tabela alterando o espaço por underline (_)
    sed -i '1,1s/[A-Z]/\L&/g' $j.csv #colocando tudo em letra minúscula pra facilitar a manipulação no hive
    

done

# Criação das pastas

DADOS=("vendas" "clientes" "endereco" "regiao" "divisao")

echo "Criando a estrutura de pastas no hdfs"

for i in "${DADOS[@]}"
do
	echo "Criando a pasta datalake/raw/$i"
    cd workspace/Desafio_Minsait_BI/raw/
    hdfs dfs -mkdir /datalake/raw/$i
    hdfs dfs -chmod 777 /datalake/raw/$i
	
	echo "Enviando arquivo $i para datalake/raw/$i"
    cd workspace/Desafio_Minsait_BI/raw/$i
    hdfs dfs -copyFromLocal $i.csv /datalake/raw/$i
    beeline -u jdbc:hive2://localhost:10000 -f ../Desafio_Minsait_BI/scripts/hql/create_table_$i.hql 
    
done
