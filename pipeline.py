from etl import pipeline_calcular_kpi_de_vendas

pasta = 'data'
formato_de_saida = ['csv', 'parquet']

pipeline_calcular_kpi_de_vendas(pasta,formato_de_saida)