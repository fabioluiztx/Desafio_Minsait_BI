CREATE EXTERNAL TABLE IF NOT EXISTS desafio.categoria ( 
        id_categoria string,
        ds_categoria string,
        perc_parceiro string
    )
COMMENT 'Tabela de Categoria'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/categoria/'
TBLPROPERTIES ("skip.header.line.count"="1");