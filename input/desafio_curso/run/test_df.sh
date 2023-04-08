#!/bin/bash
#para testes 
# Este shell deve ser executado dentro  do SPARK SERVER

spark-submit \
    --master local[*] \
    --deploy-mode client \
    ../scripts/process/test_df.py