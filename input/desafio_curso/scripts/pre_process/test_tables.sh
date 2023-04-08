#!/bin/bash
#SCRIPT PARA TESTE DA TABELAS CRIADAS NO HIVE PELO CREATE_ENV_ALL

echo "TESTANDO AS TABELAS NO HIVE"
beeline -u jdbc:hive2://localhost:10000 -f /input/desafio_curso/scripts/hql/test_tables.hql