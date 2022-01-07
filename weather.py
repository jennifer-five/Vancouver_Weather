#!/usr/bin/env python3

##author: Ziyue Cheng
#runuing in terminal as:spark-submit weather.py weatherstats_vancouver_daily.csv out_dir

#this file will return the average rain and temperature by month
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import sys,os,uuid,gzip,re
from pyspark.sql.functions import lit

from pyspark.sql import SparkSession, functions, types
from datetime import datetime


	
@functions.udf(returnType=types.IntegerType())
def return_month(date):
	date = datetime.strptime(date, "%Y-%m-%d")
	month = int(date.month)
	return month

@functions.udf(returnType=types.IntegerType())
def return_year(date):
	date = datetime.strptime(date, "%Y-%m-%d")
	year = int(date.year)
	return year


@functions.udf(returnType=types.FloatType())
def to_int(num):
	if num is not None:
		return float(num)
	else: 
		return 0
	
def main(inputs):
	# main logic starts here
	# calculate the future occupancy by listing Id
	weather = spark.read.option("multiline", "true")\
		.option("quote", '"')\
		.option("header", "true")\
		.option("escape", "\\")\
		.option("escape", '"').csv(inputs).cache()
	
	d1 = weather.select(weather['date'], to_int(weather['avg_temperature']).alias('tem'), to_int(weather['max_temperature']).alias('max_tem'),to_int(weather['min_temperature']).alias('min_tem'),to_int(weather['rain']).alias('rain'),to_int(weather['snow']).alias('snow')).withColumn('month', return_month('date')).withColumn('year', return_year('date')).cache()
	d2 = d1.groupBy('month').agg(functions.avg(d1['max_tem']).alias('max_tem'),functions.avg(d1['min_tem']).alias('min_tem'),functions.avg(d1['tem']).alias('tem'),functions.avg(d1['rain']).alias('rain'),functions.max(d1['rain']).alias('max_rain') ,functions.avg(d1['snow']).alias('snow'),functions.max(d1['snow']).alias('max_snow')).orderBy('month')
	
	df = d2.select('month',functions.round(d2['rain'],2).alias('rain'),functions.round(d2['max_rain'],2).alias('max_rain'),functions.round(d2['snow'],2).alias('snow'),functions.round(d2['max_snow'],2).alias('max_snow'),functions.round(d2['tem'],2).alias('temperature'),functions.round(d2['max_tem'],2).alias('max_tem'),functions.round(d2['min_tem'],2).alias('min_tem')).orderBy('month')
	df.show()
	
	#select the data of 2021
	d1 = d1.where(d1['year'] ==2021)
	d2 = d1.groupBy('month').agg(functions.avg(d1['max_tem']).alias('max_tem'),functions.avg(d1['min_tem']).alias('min_tem'),functions.avg(d1['tem']).alias('tem'),functions.avg(d1['rain']).alias('rain'),functions.max(d1['rain']).alias('max_rain') ,functions.avg(d1['snow']).alias('snow'),functions.max(d1['snow']).alias('max_snow')).orderBy('month')
	
	df = d2.select('month',functions.round(d2['rain'],2).alias('rain'),functions.round(d2['max_rain'],2).alias('max_rain'),functions.round(d2['snow'],2).alias('snow'),functions.round(d2['max_snow'],2).alias('max_snow'),functions.round(d2['tem'],2).alias('temperature'),functions.round(d2['max_tem'],2).alias('max_tem'),functions.round(d2['min_tem'],2).alias('min_tem')).orderBy('month')
	df.show()
	
if __name__ == '__main__':
	inputs = sys.argv[1]
	
	spark = SparkSession.builder.appName('example code').getOrCreate()
	assert spark.version >= '3.0' # make sure we have Spark 3.0+
	spark.sparkContext.setLogLevel('WARN')
	sc = spark.sparkContext
	main(inputs)