
#Importando os pacotes

from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql import functions as f
import os
import re


# função para salvar os dados
def salvar_df(df, file):
    output = "/input/desafio_curso/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /input/desafio_curso/gold/"+file+".csv"
    print(rename)
    
    
    df.coalesce(1).write\
        .format("csv")\
        .option("header", True)\
        .option("delimiter", ";")\
        .mode("overwrite")\
        .save("/datalake/gold/"+file+"/")

    os.system(erase)
    os.system(rename)



#construindo uma sessão no spark
spark = SparkSession.builder.master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()


# In[120]:


#Criando dataframes diretamente do Hive, tive uma complicação com as tabelas com o header duplicado o distinct funcionou
#nos 4 primeiros dfs com o header mas aparecia "um header no footer" na última linha então usei o where que foi a melhor solução que encontrei
df_clientes = spark.sql("""SELECT DISTINCT * FROM DESAFIO_CURSO.TBL_CLIENTES WHERE DESAFIO_CURSO.TBL_CLIENTES.address_number != "address_number" """)
df_vendas = spark.sql("""SELECT DISTINCT * FROM DESAFIO_CURSO.TBL_VENDAS WHERE DESAFIO_CURSO.TBL_VENDAS.actual_delivery_date != "actual_delivery_date" """)
df_endereco = spark.sql("""SELECT DISTINCT* FROM DESAFIO_CURSO.TBL_ENDERECO WHERE DESAFIO_CURSO.TBL_ENDERECO.address_number != "address_number" """)
df_regiao = spark.sql("""SELECT DISTINCT * FROM DESAFIO_CURSO.TBL_REGIAO WHERE DESAFIO_CURSO.TBL_REGIAO.region_name != "region_name" """)
df_divisao = spark.sql("""SELECT DISTINCT * FROM DESAFIO_CURSO.TBL_DIVISAO WHERE DESAFIO_CURSO.TBL_DIVISAO.division != "division" """)


# In[121]:


#testando os dataframes
df_clientes.show(5, truncate=False)


# In[122]:


#testando os dataframes
df_vendas.show(5, truncate=False)


# In[123]:


#testando os dataframes
df_endereco.show(5, truncate=False)


# In[8]:


#testando os dataframes
df_regiao.show(5, truncate=False)


# In[124]:


#testando os dataframes
df_divisao.show(5, truncate=False)


# In[125]:


#Criando temp view dos dataframes para juntar as tabelas pelo spark SQL
df_clientes.createOrReplaceTempView("clientes")
df_vendas.createOrReplaceTempView("vendas")
df_endereco.createOrReplaceTempView("endereco")
df_regiao.createOrReplaceTempView("regiao")
df_divisao.createOrReplaceTempView("divisao")


# In[126]:


#Juntando as tabelas e organizando para montar as dimensões com mais facilidade, ao analisar a tabela vi que já 
#tinha o valor total fiz os cálculos e eles bateram certinho
#O total se encontra em 
#sales_amount = sales_margin_amount + sales_cost_amount
#sales_cost_amount = sales_amount_based_on_list_price - discount_amount
#sales_amount = sales_price * sales_quantity
#sales_amount_based_on_list_price = list_price * sales_quantity

df_stage = spark.sql(""" 
SELECT
     v.invoice_number,
     v.invoice_date, 
     v.order_number,
     v.item_number,
     v.item,
     v.line_number,
     v.item_class,
     v.list_price,
     v.sales_price,
     v.sales_quantity,
     v.sales_amount_based_on_list_price,
     v.discount_amount,
     v.sales_cost_amount,
     v.sales_margin_amount,
     v.sales_amount as total,
     v.actual_delivery_date,
     v.datekey,
     v.promised_delivery_date,
     v.sales_rep,
     v.u_m,

     v.customerkey,
     c.customer,
     c.phone,
     c.customer_type,
     c.business_family,
     c.business_unit,
     c.line_of_business,
     c.regional_sales_mgr,
     c.search_type,

     c.address_number,
     e.customer_address_1,
     e.customer_address_2,
     e.customer_address_3,
     e.customer_address_4,
     e.zip_code,
     e.city,
     e.state,
     e.country,
     r.region_code,
     r.region_name,
     d.division,
     d.division_name FROM vendas v INNER JOIN clientes c ON v.customerkey = c.customerkey
                                    LEFT JOIN endereco e ON c.address_number = e.address_number
                                     LEFT JOIN regiao r ON c.region_code = r.region_code
                                      LEFT JOIN divisao d ON c.division = d.division
                                      
                                      
""")


# In[127]:


#Testando a df_stage com as colunas organizadas
df_stage.show(10, truncate=False)


# In[51]:


#Deixo em observação que tentei tratar os dados nulos e vazios por aqui de diversas formas e a única solução que encontrei foi com 
#o regexp_replace porém o dado estava sendo repetido na célula exemplo do código abaixo

#df = df_stage.withColumn('line_of_business',regexp_replace('line_of_business',"", 'null'))

#Saída:

#+-------------------+
#|   line_of_business|
#+-------------------+
#|null null null null|
#|null null null null|
#|null null null null|
#|null null null null|
#|null null null null|
#+-------------------+

#Assim eu tbm não conseguiria porém teria que converter o vazio para nulo e estava repetindo conforme dito acima
#df_stage1 = df_stage.fillna('not informed', subset=None) converte string nulos para não informado
#df_stage2 = df_stage1.fillna(value=0, subset=None) converte integer nulos para 0


# In[128]:


#adicionando uma coluna com base na coluna invoice_date convertida para timestamp para manipulação da data com mais facilidade
df_stage = df_stage.withColumn("newDate", date_format(unix_timestamp(df_stage.invoice_date , 
"dd/mm/yyyy").cast("timestamp"),"yyyy-mm-dd"))


# In[86]:


#Conferindo os dados pra ter certeza da execução da conversão
df_stage.select(df_stage.invoice_date, df_stage.newDate).show(5)


# In[129]:


#Criando um dataframe selecionando o customerkey(chave do cliente) que é sempre diferente pra usar como chave no join
#com a func date_format criei as colunas [dia, mes, ano, semana do ano e trimestre]
df_data = df_stage.select((df_stage.customerkey).alias("date_customer_key"), 
                           date_format('newDate', 'dd').alias('day_num'), 
                            date_format('newDate', 'MM').alias('month_num'), 
                             date_format('newDate', 'yyyy').alias('year'), 
                              weekofyear('newDate').alias('week_of_year'), 
                               quarter('newDate').alias('quarter')
                         )


# In[130]:


#Conferindo o dataframe data
df_data.show(5, truncate=False)


# In[131]:


#juntando df_data com a tabela df_stage para criação das chaves criptografadas para o df_stage_final
df_stage_final = df_data.join(df_stage, df_data.date_customer_key == df_stage.customerkey, "inner").drop('date_customer_key','newDate')


# In[132]:


#Testando o df_stage_final
df_stage_final.show(5, truncate=False)


# In[133]:


#Criando as chaves criptografadas para as dimensões como diferencial fiz a dimensão ITEM para se preciso fazer pesquisas
#por produtos mais vendidos para incrementar no dashboard
df_stage_final = df_stage_final.withColumn('PK_CUSTOMER', sha2(concat_ws("", df_stage_final.customerkey, df_stage_final.customer, df_stage_final.phone, df_stage_final.customer_type, df_stage_final.business_family, df_stage_final.business_unit, df_stage_final.line_of_business, df_stage_final.regional_sales_mgr, df_stage_final.search_type), 256))
df_stage_final = df_stage_final.withColumn('PK_LOCALITY', sha2(concat_ws("", df_stage_final.address_number, df_stage_final.customer_address_1, df_stage_final.customer_address_2, df_stage_final.customer_address_3, df_stage_final.customer_address_4, df_stage_final.zip_code, df_stage_final.city, df_stage_final.state, df_stage_final.country, df_stage_final.region_code, df_stage_final.region_name, df_stage_final.division, df_stage_final.division_name), 256))
df_stage_final = df_stage_final.withColumn('PK_TIME', sha2(concat_ws("", df_stage_final.invoice_date, df_stage_final.day_num, df_stage_final.month_num, df_stage_final.year, df_stage_final.week_of_year, df_stage_final.quarter), 256))


# In[134]:


df_stage_final.show(10, truncate=False)


# In[135]:


#Criando a dimensão cliente
dim_clientes = df_stage_final.select(df_stage_final.PK_CUSTOMER, df_stage_final.customerkey, df_stage_final.customer, df_stage_final.phone, df_stage_final.customer_type, df_stage_final.business_family, df_stage_final.business_unit, df_stage_final.line_of_business, df_stage_final.regional_sales_mgr, df_stage_final.search_type)


# In[136]:


#testando a dimensão cliente
dim_clientes.show(5, truncate=False)


# In[137]:


#Criando a dimensão Localidade
dim_localidade = df_stage_final.select(df_stage_final.PK_LOCALITY, df_stage_final.address_number, df_stage_final.customer_address_1, df_stage_final.customer_address_2, df_stage_final.customer_address_3, df_stage_final.customer_address_4, df_stage_final.zip_code, df_stage_final.city, df_stage_final.state, df_stage_final.country, df_stage_final.region_code, df_stage_final.region_name, df_stage_final.division, df_stage_final.division_name)


# In[138]:


#testando a dimensão Localidade
dim_localidade.show(5, truncate=False)


# In[139]:


#Criando a dimensão Tempo
dim_tempo = df_stage_final.select(df_stage_final.PK_TIME, df_stage_final.invoice_date, df_stage_final.day_num, df_stage_final.month_num, df_stage_final.year, df_stage_final.week_of_year, df_stage_final.quarter)


# In[140]:


#testando a dimensão Tempo
dim_tempo.show(5, truncate=False)


# In[141]:


df_stage_final.printSchema()


# In[142]:


#Criando um tempView para criar fato para calculos
df_stage_final.createOrReplaceTempView("stage")


# In[151]:


#Criando o fato implementei um atributo a mais de quantidade de itens vendidos (QTY_ITEM_SOLD = SOMA SALES_QUANTITY) dei um desc pra conferir os valores da tabela
#e mandar os nulls pra baixo que eu vou tratar no power bi
ft_vendas = spark.sql("""
                        SELECT PK_CUSTOMER, 
                            PK_LOCALITY,
                            PK_TIME,
                            invoice_number,
                            COUNT(invoice_number) as ORDER_QTY,
                            SUM(total) as TOTAL_SALES_AMOUNT,
                            SUM(sales_quantity) as QTY_ITEM_SOLD
                            FROM stage 
                            GROUP BY invoice_number, PK_CUSTOMER, PK_LOCALITY,PK_TIME ORDER BY TOTAL_SALES_AMOUNT DESC
""")


# In[152]:


ft_vendas.show(10, truncate=False)


# In[153]:


#testando a ft_vendas somando o total os valores bateram com o da tabela somado no excel
df = spark.sql("""select 
                            PK_CUSTOMER, 
                            PK_LOCALITY,
                            invoice_number,
                            COUNT(invoice_number) as ORDER_QTY,
                            SUM(total) as TOTAl,
                            SUM(sales_quantity) as QTY_ITEM_SOLD
                            from stage 
                            where invoice_number = 305451
                            group by
                            invoice_number,
                            PK_CUSTOMER, 
                            PK_LOCALITY
                            order by TOTAL desc """).show()

df = spark.sql("""select 
                            PK_CUSTOMER, 
                            PK_LOCALITY,
                            PK_TIME,
                            invoice_number,
                            COUNT(invoice_number) as ORDER_QTY,
                            SUM(total) as TOTAl,
                            SUM(sales_quantity) as QTY_ITEM_SOLD
                            from stage 
                            where invoice_number = 305451
                            group by
                            invoice_number,
                            PK_CUSTOMER, 
                            PK_LOCALITY,
                            PK_TIME
                            order by TOTAL desc """).show()


# In[154]:


ft_vendas.printSchema()


# In[155]:


#SALVANDO AS DIMENSÕES NO HDFS
salvar_df(dim_clientes, 'DIM_CLIENTES')
salvar_df(dim_localidade, 'DIM_LOCALIDADE')
salvar_df(dim_tempo, 'DIM_TEMPO')
salvar_df(ft_vendas, 'FT_VENDAS')

