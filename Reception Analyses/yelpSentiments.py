# ------ Set-up ------ #

import pyspark
from pyspark.sql import SparkSession
import os
import re
from importYelp import get_reviews
import pyspark.sql.functions as f
from nltk.sentiment import SentimentIntensityAnalyzer
from pyspark.sql.types import ArrayType, StringType, FloatType

#  ------ Get the Data ------  #

yelp_df = get_reviews(sc)

# ------ HELPER FUNCTIONS ------- #

def polarity(text):
         score = sia.polarity_scores(text)
         return score

polarity = f.udf(polarity)


def sen_cleaner(str):
        li = list(str.values())
        return li

sen_cleaner = f.udf(sen_cleaner, ArrayType(FloatType()))

# ------ SENTIMENT ANALYSIS ------- #

scoredDF = yelp_df.select("Time", "Review", polarity("Review").alias("Sentiment"))
cleanedDF = scoredDF.select("Time", sen_cleaner("Sentiment").alias("Sentiment"))
sentiment = cleanedDF.select("Time", cleanedDF.Sentiment[0].alias("NEG"), cleanedDF.Sentiment[1].alias("NEU"), cleanedDF.Sentiment[2].alias("POS"), cleanedDF.Sentiment[3].alias("Compound"))
avgsentiment = sentiment.groupBy("Time").agg(f.avg("NEG"), f.avg("NEU"), f.avg("POS"), f.avg("Compound"))
avgsentiment.show(3)
