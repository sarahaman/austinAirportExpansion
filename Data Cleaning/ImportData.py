import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as f
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
from pyspark.rdd import RDD
#from initSession import sc, spark
import os
from os import listdir
from os.path import isfile, join
from pyspark.sql.types import StructField, StructType, IntegerType, DateType, StringType
from FlDataSchema import csv_schema
from DateSegmentation import assignQuarter, quarterUDF


class Flights:
    '''Class object for collecting and filtering flights data.
    
    Methods:
        getAUS: Subsets the austin airport
        getDates: Convert Data into 
    '''
    def __init__(self, sc, spark):
        self.Data = ''
        self.filePath = join(os.getcwd(), 'FlightsData')
        self.filesList = [join('FlightsData', f) for f in listdir(self.filePath)]
        self.sc = sc
        self.spark = spark
        self.timeframes = []

    def getData(self):
        i = 0
        for f in self.filesList:
            curDF = self.spark.read.csv(f, header = True, \
                                        schema = csv_schema)
            if i != 0:
                self.Data = self.Data.union(curDF)
            else:
                self.Data = curDF
            i += 1
        self.Data = self.Data.withColumn("QUARTER", quarterUDF(col("YEAR"), col("MONTH")))
        
    def getAUS(self):
        self.Data = self.Data.filter((self.Data.ORIGIN == "AUS") |(self.Data.DEST == "AUS"))

    def getDFW(self):
        self.Data = self.Data.filter((self.Data.ORIGIN == "DFW") |(self.Data.DEST == "DFW"))

    def getIAH(self):
        self.Data = self.Data.filter((self.Data.ORIGIN == "IAH") |(self.Data.DEST == "IAH"))

    def getDates(self):
        self.Data = self.Data.withColumn("FL_DATE", f.to_date("FL_DATE", 'yyyy-MM-dd HH:mm:ss'))
    
    def subsetData(self, columns = 'All'):
        '''Method to convert sparkDF of flights data into an RDD
        
        Args:
            columns: Takes a list of columns in the original DF for subsetting or "ALL" for all columns.
        '''
        if isinstance(columns, str):
            cols = list()
            cols.append(columns)
            columns = cols
        if isinstance(self.Data, DataFrame):
            # Check for columns in DataFrame
            if len(columns) == 1:
                if columns[0].upper() == 'ALL':
                    return self.Data
            columns = [col.upper() for col in columns]
            if all(col in self.Data.columns for col in columns):
                return self.Data.select(columns)
            else:
                return 'Columns requested cannot be retrieved from the DataFrame'
        elif isinstance(self.Data, RDD):
            return 'Data is an RDD.'
        else:
            return 'I don\'t know what you want me to do with this object: %s' % type(self.Data)
            
        
    def getTimeFrames(self, timeframe):
        '''Split Time Frames

        '''
