import csv
from nltk.classify import NaiveBayesClassifier
from nltk.corpus import subjectivity
from nltk.sentiment import SentimentAnalyzer 
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk import tokenize

import pandas as pd
dataset = pd.read_csv("/home/ubuntu/project/BigDataTeam/classify/data/sentimential/senti.csv",quoting=csv.QUOTE_NONE,encoding='latin-1')
#dataset.head(n=2)

sid = SentimentIntensityAnalyzer()

dataneg=[]
datapos=[]
dataneutr=[]
datacompound=[]
i=1
for index, row in dataset.iterrows():
     #print(subset.values[index])
     ss = sid.polarity_scores(dataset.tweet_text[index])
     dataneg.append(ss['neg'])
     datapos.append(ss['pos'])
     dataneutr.append(ss['neu'])
     datacompound.append(ss['compound'])
     i=i+1
	 
dataset['compound']=datacompound
dataset['negative']=dataneg
dataset['neutral']=dataneutr
dataset['positive']=datapos

dataset.to_csv("/home/ubuntu/project/BigDataTeam/classify/data/sentimential/senti_final.csv", sep='|', encoding='utf-8')
