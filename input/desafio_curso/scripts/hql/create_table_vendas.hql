create database DESAFIO_CURSO;

use DESAFIO_CURSO;

CREATE EXTERNAL TABLE IF NOT EXISTS DESAFIO_CURSO.TBL_VENDAS ( 
	actual_delivery_date string,
	customerkey string,	
	datekey	 string,
	discount_amount float,	
	invoice_date string,	
	invoice_number string,
	item_class string,	
	item_number string,
	item string,
	line_number string,
	list_price float,
	order_number string,
	promised_delivery_date string,	
	sales_amount float,
	sales_amount_based_on_list_price float,	
	sales_cost_amount float,
	sales_margin_amount float,
	sales_price float,
	sales_quantity int,
	sales_rep string,
	u_m string
    )
COMMENT 'Tabela de Vendas'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/vendas/'
TBLPROPERTIES ("skip.header.line.count"="1");