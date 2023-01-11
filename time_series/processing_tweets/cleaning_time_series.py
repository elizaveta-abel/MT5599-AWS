import pandas as pd
import json
import re
#import tqdm
#from multiprocessing import Pool # multithreading
#import tqdm
#from time import process_time
import feather
import io
import os

import boto3


"""
Functions to process data
"""


    

# instead of 
# df = pd.read_json(filepath)
    
# replace it with
response = s3_client.get_object(Bucket="mt5599", Key=filepath)
df = pd.read_json(response.get("Body"))

# instead of
#new_filepath = filepath.replace(".json", "_processed.feather")
#df.to_feather(new_filepath)

# replace it with
new_filepath = filepath.replace(".json", "_processed.feather")
with io.BytesIO() as feather_buffer:
    df.to_feather(feather_buffer)

    response = s3_client.put_object(
        Bucket="mt5599", Key = new_filepath, Body=feather_buffer.getvalue()
    )

    
    
    

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
    for i in range(266, 401):
        filepath = "s3://mt5599/tweets/spanish_tweets_2016_" + str(i) + ".json"
        filepaths.append(filepath)

    no_tweets = 0
    non_existent_files = []
    for filepath in filepaths:
        try:
            t1_start = process_time()
            no_tweets = no_tweets + clean_df(filepath)
            t1_stop = process_time()
            print()
            print("elapsed time: ", t1_stop - t1_start)
            print()
        except:
            non_existent_files.append(filepath)
            continue
            
    print("the number of geotagged tweets collected: ", no_tweets)
    print("the files that do not exist: ", non_existent_files)
