CREATE EXTERNAL TABLE IF NOT EXISTS DESAFIO_CURSO.TBL_ENDERECO ( 
    address_number string,
	city string,	
	country string,	
	customer_address_1 string,
	customer_address_2 string,
	customer_address_3 string,	
	customer_address_4 string,	
	state string,	
	zip_code string
    )
COMMENT 'Tabela de Endereço'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/endereco/'
TBLPROPERTIES ("skip.header.line.count"="1");