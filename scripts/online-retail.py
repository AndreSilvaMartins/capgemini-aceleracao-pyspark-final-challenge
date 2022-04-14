from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

# REGEX PATTERNS ---
REGEX_ALPHA     = r'[a-zA-Z]+'
REGEX_EMPTY_STR = r'[\t ]+$'
REGEX_INTEGER  = r'[0-9]+'
REGEX_INVOICENO = r'c?([0-9]{6})'
# --------------------

# Helper functions ---
def check_empty_column(col):
	return (F.col(col).isNull() | (F.col(col) == '') | F.col(col).rlike(REGEX_EMPTY_STR))

def clean_dataframe(dataframe):
	#remove PADS StockCodes
	data = dataframe.select('*').where(dataframe['StockCode'] != 'PADS')
	
	#ensures InvoiceNo
	data = data.withColumn('InvoiceNo', (
		F.when(
			(F.col('InvoiceNo').rlike(REGEX_INVOICENO) ), F.col('InvoiceNo') 
		)
	))

	#ensures StockCode
	data = data.withColumn('StockCode', (
		F.when(
			(F.col('StockCode').rlike(REGEX_INTEGER) & (F.length(F.col('StockCode')) == 5)), F.col('StockCode').cast(IntegerType())
		)
	))

	#ensures the Unit Price
	data = data.withColumn('UnitPrice', (
		F.when(
			(check_empty_column('UnitPrice') | (F.col('UnitPrice').contains('-')) ), 0.0
		).when(
			F.col('UnitPrice').contains(','), F.regexp_replace('UnitPrice', ',', '.').cast(FloatType())
		).otherwise(
			F.col('UnitPrice').cast(FloatType())
		)
	))

	#ensures the timestamp format
	data = data.withColumn('InvoiceDate', (
		F.when(
			check_empty_column('InvoiceDate'), ''
		 ).otherwise(
			 F.to_timestamp(F.col('InvoiceDate'), 'd/M/yyyy HH:mm').cast(TimestampType())
		 )
	))

	#ensures the Quantity
	data = data.withColumn('Quantity', (
		F.when(
			check_empty_column('Quantity'), 0
		).otherwise(
			F.col('Quantity').cast(IntegerType())
		)
	))

	#ensures the CustomerID
	data = data.withColumn('CustomerID', (
		F.when(
			(F.col('CustomerID').rlike(REGEX_INTEGER) & (F.length(F.col('CustomerID')) == 5)), F.col('StockCode').cast(IntegerType())
		)
	))

	return data
# --------------------

#Bussiness questions to Online Retail

#-------------------------------------

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

	schema_online_retail = StructType([
		StructField('InvoiceNo',   StringType(), True),
		StructField('StockCode',   StringType(), True),
		StructField('Description', StringType(), True),
		StructField('Quantity',    StringType(), True),
		StructField('InvoiceDate', StringType(), True),
		StructField('UnitPrice',   StringType(), True),
		StructField('CustomerID',  StringType(), True),
		StructField('Country',     StringType(), True)
	])

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_online_retail)
		          .load("data/online-retail/online-retail.csv"))
	df.show(10)
	df.printSchema()
	df.select('*').where(F.col('UnitPrice').contains('-')).show(10)
	treated_df = clean_dataframe(df)	
