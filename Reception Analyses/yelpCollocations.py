from importYelp import get_reviews
from pyspark.ml.feature import NGram
from pyspark.sql.types import ArrayType, FloatType, StringType
import nltk

# ----- Helper Functions ----- #

stop_words = nltk.corpus.stopwords.words('english')

def stopword_remover(sentence):
	sentence = sentence.split()
	output = [word for word in sentence if word not in stop_words]
	return output

stopword_remover = f.udf(stopword_remover, ArrayType(StringType()))

# ----- Trigram Dataframe ----- #

yelp_df = get_reviews(sc)
sentenceData = yelp_df.select("Time", stopword_remover("Review").alias("Review"))
trigram_df = NGram(n=3, inputCol="Review", outputCol="Trigrams").transform(sentenceData)
trigram_df = trigram_df.select("Time", f.explode("Trigrams").alias("Trigrams"))

# ----- Getting Trigrams Containing a Word ----- #

# Pass in the trigrams dataframe and a word and the function will return a pandas dataframe of 
# trigrams in the data that match the word. 

def get_word_trigrams(trigram_df, word):
	import pandas as pd
	correct_word = '%' + str(word) + '%'
	word_df = trigram_df.filter(trigram_df['Trigrams'].like(correct_word))
	word_df = word_df.toPandas()
	pd.set_option('display.max_rows', word_df.shape[0]+1)
	return word_df


