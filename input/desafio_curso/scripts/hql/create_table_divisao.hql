CREATE EXTERNAL TABLE IF NOT EXISTS DESAFIO_CURSO.TBL_DIVISAO ( 
	division string,
	division_name string
    )
COMMENT 'Tabela de Divisões'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/divisao/'
TBLPROPERTIES ("skip.header.line.count"="1");