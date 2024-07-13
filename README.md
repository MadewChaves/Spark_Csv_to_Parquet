# Spark_Csv_to_Parquet
This projects has the main objective of showing the difference in size between files in CSV and in PARQUET, in this project i use Spark and Python to achieve the expected result.

# Data Reading
The data is read from a CSV file stored on Google Drive. The directory and the file name are specified as shown below:

dir = '/content/drive/MyDrive/Colab Notebooks/Dados_gov'
file_name = '2024_Passagem'
df_passagem = spark.read.csv(f'{dir}/{file_name}.csv', header=True,sep=";", encoding='ISO-8859-1', inferSchema=True)

# Data Processing
The code performs several transformations on the data, including standardizing column names and converting the values of the valor_da_passagem and taxa_de_serviço columns to float:

df_passagem_tratado = df_passagem.select([F.col(x).alias(x.replace('-','').replace(' ','_').lower()) for x in df_passagem.columns])
df_passagem_tratado = df_passagem_tratado.withColumn('valor_da_passagem',F.expr("cast(replace(valor_da_passagem,',','.') as float)")) \
                                          .withColumn('taxa_de_serviço',F.expr("cast(replace(valor_da_passagem,',','.') as float)"))

# Saving Processed Data
The processed data is saved in Parquet format:

df_passagem_tratado.write.mode('overwrite').parquet(f'{dir}/{file_name}.parquet')

# File Size Comparison
The code compares the size of the original CSV file with the size of the generated Parquet file:

tamanho_csv = float(os.path.getsize(f"{dir}/{file_name}.csv"))
tamanho_parquet = float(os.path.getsize(f"{dir}/{file_name}.parquet"))
diferenca_percentual = round((tamanho_csv - tamanho_parquet) / tamanho_csv * 100, 2)
print("Size of the CSV file:",(os.path.getsize(f"{dir}/{file_name}.csv")),"bytes","\n\n","Size of the PARQUET file:",(os.path.getsize(f"{dir}/{file_name}.parquet")),"bytes","\n\n",f"Difference between the sizes in percentage: {diferenca_percentual} %")
