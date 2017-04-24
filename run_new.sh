#!/bin/bash

WORK_DIR=/home/ubuntu/project/BigDataTeam/classify
algorithm=(naivebayes classify clean)

if [ -n "$1" ]; then
  choice=$1
else
  echo "Please select a number to choose the corresponding task to run"
  echo "1. ${algorithm[0]} -- train mahout"
  echo "2. ${algorithm[1]} -- classify a set"
  echo "3. ${algorithm[2]} -- cleans up the work area in $WORK_DIR"
  read -p "Enter your choice : " choice
fi

alg=${algorithm[$choice-1]}

# Cleaning stuff
if [ "x$alg" == "xclean" ]; then
    echo "Cleaning work directory at ${WORK_DIR}"
    if hadoop fs -test -d ${WORK_DIR} ; then
        hadoop fs -rmr ${WORK_DIR}
    fi
    echo "Cleaning local filesystem"
    rm labelindex
    rm df-count
    rm dictionary.file-0
    rm -rf model
fi

set -e

# Training naive bayes
if [ "x$alg" == "xnaivebayes" ]; then
    
    set -x

    if [ ! -f data/tweets-train.tsv ]; then
        echo "No training file at data/tweets-train.tsv"
        exit 1
    fi

    echo "Creating work directory at ${WORK_DIR}"
    if hadoop fs -test -d ${WORK_DIR} ; then
        hadoop fs -rmr ${WORK_DIR}
    fi
    hadoop fs -mkdir ${WORK_DIR}

    echo "Converting tsv to sequence files..."
    java -cp mahout-tweets-classifier-1.0-jar-with-dependencies.jar \
        mahout.classifier.TweetTSVToSeq data/tweets-train.tsv tweets-seq

    echo "Uploading sequence file to HDFS..."
    if hadoop fs -test -d tweets-seq; then
        hadoop fs -rmr tweets-seq
    fi
    hadoop fs -put tweets-seq ${WORK_DIR}/tweets-seq
    rm -rf tweets-seq

    echo "Converting sequence files to vectors..."
    mahout seq2sparse \
        -i ${WORK_DIR}/tweets-seq \
        -o ${WORK_DIR}/tweets-vectors

    echo "Creating training and holdout set with a random 80-20 split of the generated vector dataset"
    mahout split \
        -i ${WORK_DIR}/tweets-vectors/tfidf-vectors \
        --trainingOutput ${WORK_DIR}/train-vectors \
        --testOutput ${WORK_DIR}/test-vectors \
        --randomSelectionPct 40 --overwrite --sequenceFiles -xm sequential

    echo "Training Naive Bayes model"
    mahout trainnb \
        -i ${WORK_DIR}/train-vectors -el \
        -li ${WORK_DIR}/labelindex \
        -o ${WORK_DIR}/model \
        -ow -c

    echo "Self testing on training set"
    mahout testnb \
        -i ${WORK_DIR}/train-vectors \
        -m ${WORK_DIR}/model \
        -l ${WORK_DIR}/labelindex \
        -ow -o ${WORK_DIR}/tweets-testing -c

    echo "Testing on holdout set"
    mahout testnb \
        -i ${WORK_DIR}/test-vectors \
        -m ${WORK_DIR}/model \
        -l ${WORK_DIR}/labelindex \
        -ow -o ${WORK_DIR}/tweets-testing -c
fi

# Classify
if [ "x$alg" == "xclassify" ]; then

    set -x

    #insert old data into history table
    hive -e 'insert into silicon.siliconlive_hst select * from silicon.siliconlive;'
	
    #dropping livetweets from hdfs
    hadoop fs -rm /user/hive_data/raw_data/live_tweets/tweets.csv
	
    #uploading new tweets on hdfs
    hadoop fs -put /home/ubuntu/pycode/tweets.csv /user/hive_data/raw_data/live_tweets/
	
    #moving data to hist folder
    mv /home/ubuntu/pycode/tweets.csv /home/ubuntu/pycode/hist_data/tweets_$(date +%Y%m%d%H%M%S) 
	
    #moving data to hist folder
    mv /home/ubuntu/project/BigDataTeam/classify/data/tweets-to-classify.tsv /home/ubuntu/project/BigDataTeam/classify/data/tweets-to-classify_hst/tweets-to-classify_$(date +%Y%m%d%H%M%S).tsv 
	
    #exporting data for classifying 
    hive -e 'select tweet_id, tweet_text from silicon.siliconlive;' > /home/ubuntu/project/BigDataTeam/classify/data/tweets-to-classify.tsv
	    

    if ! hadoop fs -test -e ${WORK_DIR}/labelindex ; then
        echo "No index on HDFS at path ${WORK_DIR}/labelindex"
        exit 1
    fi
    if ! hadoop fs -test -d ${WORK_DIR}/model ; then
        echo "No model on HDFS at path ${WORK_DIR}/model"
        exit 1
    fi
    if ! hadoop fs -test -d ${WORK_DIR}/tweets-vectors ; then
        echo "No vector on HDFS at path ${WORK_DIR}/tweets-vectors"
        exit 1
    fi
    if [ ! -f data/tweets-to-classify.tsv ]; then
        echo "No tweets to classify at path data/tweets-to-classify.tsv"
        exit 1
    fi

    echo "Retrieving index and model from HDFS"
    hadoop fs -get \
    ${WORK_DIR}/labelindex \
    labelindex
    
    hadoop fs -get \
    ${WORK_DIR}/model \
    model
    
    hadoop fs -get \
    ${WORK_DIR}/tweets-vectors/dictionary.file-0 \
    dictionary.file-0

    hadoop fs -getmerge \
    ${WORK_DIR}/tweets-vectors/df-count \
    df-count

    #python scripts/twitter_fetcher.py 1 > data/tweets-to-classify.tsv
    
     mv /home/ubuntu/project/BigDataTeam/classify/data/classify/classified.tsv /home/ubuntu/project/BigDataTeam/classify/data/classify/classified_$(date +%Y%m%d%H%M%S).tsv 
   
    echo "Classifying tweets..."
    java -cp mahout-tweets-classifier-1.0-jar-with-dependencies.jar \
        mahout.classifier.Classifier model labelindex dictionary.file-0 df-count data/tweets-to-classify.tsv > data/classify/classified.tsv

    echo "Cleaning local filesystem"
    rm labelindex
    rm df-count
    rm dictionary.file-0
    rm -rf model

    #making hist of toanalyze file
    mv /home/ubuntu/project/BigDataTeam/classify/data/analyze/toanalyze.txt /home/ubuntu/project/BigDataTeam/classify/data/analyze/toanalyze_$(date +%Y%m%d%H%M%S).txt
	
    #run java code for parsing classified.tsv
    java parse
	
    #insert data into history table
    hive -e 'insert into silicon.siliconclassified_hst select * from silicon.siliconclassified;'
	
    #dropping livetweets from hdfs
    hadoop fs -rm /home/ubuntu/project/BigDataTeam/classify/data/analyze/toanalyze.txt
	
    #uploading new tweets on hdfs
    hadoop fs -put /home/ubuntu/project/BigDataTeam/classify/data/analyze/toanalyze.txt /home/ubuntu/project/BigDataTeam/classify/data/analyze/
	
    mv /home/ubuntu/project/BigDataTeam/classify/data/sentimential/senti.tsv  /home/ubuntu/project/BigDataTeam/classify/data/sentimential/senti_$(date +%Y%m%d%H%M%S).tsv
		
    mv /home/ubuntu/project/BigDataTeam/classify/data/sentimential/senti.csv  /home/ubuntu/project/BigDataTeam/classify/data/sentimential/senti_$(date +%Y%m%d%H%M%S).csv
	
    #exporting data for sentimential analysis 
    hive -e 'set hive.cli.print.header=true;select tweet_id, tweet_text from silicon.siliconclassified where label like "%tvshow%";' > /home/ubuntu/project/BigDataTeam/classify/data/sentimential/senti.tsv
	
    #converting data from tsv to csv
    cat data/sentimential/senti.tsv | tr "\\t" "," > data/sentimential/senti.csv
    
    if [ ! -f data/sentimential/senti.csv ]; then
    echo "No recent tweets to perform sentimential Analysis"
    exit 1
	
    #moving data to hist folder
    mv /home/ubuntu/project/BigDataTeam/classify/data/sentimential/senti_final.csv  /home/ubuntu/project/BigDataTeam/classify/data/sentimential/senti_final$(date +%Y%m%d%H%M%S).csv
	
    python sent.py
	
    #insert data into history table
    hive -e 'insert into silicon.siliconsentimential_hst select * from silicon.siliconsentimential;'
	
	
    #removing senti.csv from hdfs
    hadoop fs -rm /home/ubuntu/project/BigDataTeam/classify/data/sentimential/senti_final.csv
	
    #uploading new tweets to be senti on hdfs
    hadoop fs -put /home/ubuntu/project/BigDataTeam/classify/data/sentimential/senti_final.csv /home/ubuntu/project/BigDataTeam/classify/data/sentimential/
	
    #exporting 
    hive -e 'set hive.cli.print.header=true;select a.tweet_id, a.tweet_text, b.tweet_date, b.type_of_tweet, a.compound, a.neutral, a.negative, a.positive, case when a.negative >  a.neutral and a.negative >  a.positive then 'negative' when a.neutral >  a.positive then 'neutral' else 'positive'  end as greatest_value from  silicon.siliconsentimential a join silicon.siliconlive_hst b on a.tweet_id=b.tweet_id;' > /home/ubuntu/project/BigDataTeam/classify/Final_senti2.tsv   
fi
