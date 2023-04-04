#!/bin/bash

#Escolhendo o diretório raw
cd /input/desafio_curso/raw

#Trocando os espaços em brancos do header por underline ("_") para manipulação das tabelas no hive
#e colocando em lower case pra bater com o hql.
TABELAS=("CLIENTES" "DIVISAO" "ENDERECO" "REGIAO" "VENDAS")

#Percorrendo as tabelas
for j in "${TABELAS[@]}"

do
    echo "Alterando o header das colunas do $j.csv para manipulação"
    chmod 777 $j.csv #dando permissão para modificar o arquivo
    sed -i '1,1s/ /_/g' $j.csv #percorrendo apenas a primerira linha da tabela alterando o espaço por underline (_)
    sed -i '1,1s/[A-Z]/\L&/g' $j.csv #colocando tudo em letra minúscula pra facilitar a manipulação no hive
    

done

# Criação das pastas

DADOS=("vendas" "clientes" "endereco" "regiao" "divisao")

echo "Criando a estrutura de pastas no hdfs"

#criando a pasta datalake,raw e a gold
echo "Criando a pasta /datalake"
hdfs dfs -mkdir /datalake/
echo "Criando a pasta /datalake/raw"
hdfs dfs -mkdir /datalake/raw
echo "Criando a pasta /datalake/gold"
hdfs dfs -mkdir /datalake/gold

    for i in "${DADOS[@]}"
        do
            echo "Criando a pasta datalake/raw/$i"
            cd /input/desafio_curso/raw
            hdfs dfs -mkdir /datalake/raw/$i
            hdfs dfs -chmod 777 /datalake/raw/$i
            
            echo "Enviando arquivo ${i^^}.csv para datalake/raw/$i"
            hdfs dfs -copyFromLocal ${i^^}.csv /datalake/raw/$i
            beeline -u jdbc:hive2://localhost:10000 -f /input/desafio_curso/scripts/hql/create_table_$i.hql 
    
        done
