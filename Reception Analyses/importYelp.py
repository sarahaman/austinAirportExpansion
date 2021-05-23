################################
## EARLY DATA PRE-PROCESSING  ##
################################

##################
##    SET UP    ##
##################

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType
from pyspark.sql import Row
import pandas as pd
import pyspark.sql.functions as f
import os
import re


###################################
## METHOD FOR GETTING CLEAN RDD  ##
###################################

def get_reviews(sc):

    '''
    Gets cleaned Yelp reviews to be used in sentiment analysis.

    Arguments: sc
         pyspark spark context object
    Returns: rdd
         cleaned spark rdd of the yelp_reviews.csv
    '''

    spark = SparkSession.builder.getOrCreate()
    schema = StructType() \
             .add("Reviews", StringType(), True) \
             .add("Time", StringType(), True)

    yelpDF = spark.read.format("csv") \
             .option("header", True) \
           .option("escape", '"') \
             .schema(schema) \
             .load("yelpData/yelp_reviews.csv")

    def cleanThis(x):
      import re
      x = re.sub('[<@*&?].*[>@*&?]','',x)
      x = re.sub('[&@*&?].*[;@*&?]','',x)
      x = re.sub('[#@*&?].*[;@*&?]','',x)
      x = x.lower()
      x = re.sub(r'[^0-9a-zA-z,.!?]+', ' ', x)
      output = re.sub(r'\bt\b', 'not', x)
      return output

    cleanThis = f.udf(cleanThis)
    cleanText = yelpDF.select(cleanThis('Reviews').alias('Review'), "Time")
    return cleanText



