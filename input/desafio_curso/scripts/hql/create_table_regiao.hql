CREATE EXTERNAL TABLE IF NOT EXISTS DESAFIO_CURSO.TBL_REGIAO ( 
region_code string,
region_name string
    )
COMMENT 'Tabela de Regioes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/regiao/'
TBLPROPERTIES ("skip.header.line.count"="1");