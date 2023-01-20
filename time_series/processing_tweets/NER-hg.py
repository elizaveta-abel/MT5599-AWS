#import numpy as np
# pip install pandas transformers torch torchvision torchaudio boto3 feather-format tqdm


import pandas as pd
from transformers import AutoTokenizer, AutoModelForTokenClassification, pipeline
import boto3
import os
from time import process_time
import io
import tqdm
import feather

import json
import re
from multiprocessing import Pool # multithreading
from time import process_time
import unicodedata
import html


tokenizer = AutoTokenizer.from_pretrained("Davlan/bert-base-multilingual-cased-ner-hrl")

model = AutoModelForTokenClassification.from_pretrained("Davlan/bert-base-multilingual-cased-ner-hrl")

nlp = pipeline("ner", model=model, tokenizer=tokenizer)



def clean_df(filepath):
#def clean_df(df):
    
    # reading in data
    print()
    print()
    print("reading in ", filepath)
    print()
    response = s3_client.get_object(Bucket="mt5599", Key=filepath)
    df = pd.read_feather(io.BytesIO(response['Body'].read()))
    
    tweets_clean = df.tweet_clean.tolist()
    
    print("doing the NER :) lol")
    ner_results = nlp(tweets_clean)
    
    spttext = pd.DataFrame([])
    spttweet = pd.DataFrame([])
    
    for k, ner_result in enumerate(ner_results):
        
        for i, result in enumerate(ner_result):
            
            combined_entity = None

            length = len(ner_result)

            if result['entity'] == 'B-LOC':

                combined_entity = result['word']

                j = i + 1
                
                if j < length:
                    print(j, length)
                    while ner_result[j]['entity'] == 'I-LOC':
                        print(j, length)
                        if "##" in ner_result[j]['word']:
                            combined_entity = combined_entity + ner_result[j]['word'].strip("#")
                        else:
                            combined_entity = combined_entity + ' ' + ner_result[j]['word']

                        j += 1

                        if j >= length:
                            break

                spttext = spttext.append(pd.DataFrame({'textloc': combined_entity}, index = [0]), ignore_index = True)
                spttweet = spttweet.append(pd.DataFrame({'TweetNumber': k}, index = [0]), ignore_index = True)

    # Third: Combining the Spacy Spanish Findings
    frames_spacy_sp = [spttweet, spttext]
    finalent_spacy_sp = pd.concat(frames_spacy_sp, axis = 1)
    
    print(finalent_spacy_sp)
    print(df)
    

    gpedf_sp = finalent_spacy_sp

    # Fifth: Merge the GPE on the Main Data Frame
    sptlocations = pd.merge(df, gpedf_sp, on = "TweetNumber", how = "left")

    return sptlocations


       

if __name__ == "__main__":
    
    
    # For getting and saving datasets directly from/to S3
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
    )
    
    
    
    
    filepaths = []
    for i in range(75, 76):
        filepath = "tweets/spanish_tweets_2016_" + str(i) + "_processed_wcontent.feather"
        #filepath = "s3://mt5599/tweets/spanish_tweets_2016_" + str(i) + ".json"
        filepaths.append(filepath)

    no_tweets = 0
    non_existent_files = []
    for filepath in filepaths:
        try:
            t1_start = process_time()
            print(clean_df(filepath))
            #no_tweets = no_tweets + clean_df(filepath)
            t1_stop = process_time()
            print()
            print("elapsed time: ", t1_stop - t1_start)
            print()
        except:
            non_existent_files.append(filepath)
            continue
            
    print("the number of tweets processed: ", no_tweets)
    print("the files that do not exist: ", non_existent_files)

