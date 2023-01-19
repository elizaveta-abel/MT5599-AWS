import pandas as pd
import json
import re
import tqdm
from multiprocessing import Pool # multithreading
from time import process_time
import feather
import io
import os
import boto3
import unicodedata
import html
#import spacy


"""
Functions to process data
"""

    
# write function to remove unnecessary columns
def keep_columns(df, # dataframe to be cleaned
                 columns): # list of columns to keep
    df = df[columns]
    return df



#write function to keep only tweets with location data
def has_loc(df):
    df = df[df.coordinates != "None"]
    df = df[df.place != "None"]
    df = df.reset_index(drop=True)
    return df



# helper function to extract place and coordinates
def safe_json_loads(string):
    try:
        string = json.loads(string)
    except:
        string = None
    return(string)




"""
EXTRACTING PLACE
"""


def extract_place_helper(row):
    if row[1]["place"] != "None":
        try:
            split_by = "Place\(fullName='|', name='|', type='|', country='|', countryCode='|'\)"
            temp = re.split(split_by, row[1]["place"])

            row[1]['place_full_name'] = temp[1]
            row[1]['place_name'] = temp[2]
            row[1]['place_type'] = temp[3]
            row[1]['place_country'] = temp[4]
            row[1]['place_country_code'] = temp[5]
        except:
            row[1]['place_full_name'] = None
            row[1]['place_name'] = None
            row[1]['place_type'] = None
            row[1]['place_country'] = None
            row[1]['place_country_code'] = None
            
    return row[1]


# extracting place components
def extract_place(df):
    
    df_coord = df
    
    df_coord['place_full_name'] = None
    df_coord['place_name'] = None
    df_coord['place_type'] = None
    df_coord['place_country'] = None
    df_coord['place_country_code'] = None

    
    pool = Pool(processes=round(len(df_coord.index)/10000))

    result_arr = []
    
    for result in tqdm.tqdm(pool.imap_unordered(extract_place_helper, df_coord.iterrows()),
                            total=len(df_coord.index)):
        result_arr.append(result)
                
    df_coord = pd.concat(result_arr, axis=1).transpose().sort_index()
                
    return df_coord





"""
EXTRACTING COORDINATES
"""


def extract_coordinates_helper(row):
    if row[1]["coordinates"] != "None":
        try:
            split_by = "Coordinates\(longitude=|, latitude=|\)"
            temp = re.split(split_by, row[1]["coordinates"])

            row[1]['coordinates_longitude'] = float(temp[1])
            row[1]['coordinates_latitude'] = float(temp[2])

        except:
            row[1]['coordinates_longitude'] = None
            row[1]['coordinates_latitude'] = None

    return row[1]


# extracting place components
def extract_coordinates(df):
    
    df_coord = df
    
    df_coord['coordinates_longitude'] = None
    df_coord['coordinates_latitude'] = None
    
    
    pool = Pool(processes=round(len(df_coord.index)/10000))

    result_arr = []
    
    for result in tqdm.tqdm(pool.imap_unordered(extract_coordinates_helper, df_coord.iterrows()),
                            total=len(df_coord.index)):
        result_arr.append(result)
                
    df_coord = pd.concat(result_arr, axis=1).transpose().sort_index()
                
    return df_coord




"""
CLEANING TWEET CONTENT
"""

def clean_tweet_helper(row):
    
    s = row[1]["tweet_content"]
    
    r = unicodedata.normalize("NFC", s)
    r = html.unescape(r)
    #r = r.encode("ascii", "ignore").decode()
    r = r.replace('\n', " ")
    r = r.replace('@', " ")
    r = r.replace("#", " ")
    r = re.sub('http://\S+|https://\S+', '', r)
    #r = r.lower()
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               "]+", flags=re.UNICODE)
    r = (emoji_pattern.sub(r'', r))  # no emoji
    r = re.sub('\s{2,}', ' ', r)
    
    row[1]["tweet_clean"] = r
    
    return row[1]

def clean_tweet(df):
    
    df['tweet_clean'] = None
    
    pool = Pool(processes=round(len(df.index)/10000))

    result_arr = []
    
    for result in tqdm.tqdm(pool.imap_unordered(clean_tweet_helper, df.iterrows()),
                            total=len(df.index)):
        result_arr.append(result)
                
    df = pd.concat(result_arr, axis=1).transpose().sort_index()
                
    return df





"""
NER PIPELINE (FOR LOCATIONS)
"""
'''

# Second: Initialise the NLP in Spacy
nlp = spacy.load("es_core_news_sm")

        
def ner_helper(row):
    sptspacy = row[1]["tweet_clean"]
    doc_sp = nlp(sptspacy)
    
    spttext = pd.DataFrame([])
    sptlabel = pd.DataFrame([])
    spttweet = pd.DataFrame([])

    for ent in doc_sp.ents:

        spttext = spttext.append(pd.DataFrame({'textloc': ent.text}, index = [0]), ignore_index = True)
        sptlabel = sptlabel.append(pd.DataFrame({'label': ent.label_}, index = [0]), ignore_index = True)
        spttweet = spttweet.append(pd.DataFrame({'TweetNumber': row[1]["TweetNumber"]}, index = [0]), ignore_index = True)

    # Third: Combining the Spacy Spanish Findings
    frames_spacy_sp = [spttweet, spttext, sptlabel]
    finalent_spacy_sp = pd.concat(frames_spacy_sp, axis = 1)
    
    # Fourth: Keep only the GPE parts
    if len(finalent_spacy_sp.index) > 0:
        gpedf_sp = finalent_spacy_sp[finalent_spacy_sp['label'] == "LOC"]

        # Fifth: Merge the GPE on the Main Data Frame
        sptlocations = pd.merge(row[1].to_frame().transpose(), gpedf_sp, on = "TweetNumber", how = "left")
        
    else:
        sptlocations = row[1].to_frame().transpose()
        sptlocations["textloc"] = None
        sptlocations["label"] = None
        sptlocations["TweetNumber"] = row[1]["TweetNumber"]

    return sptlocations



# extracting place components
def ner(df):
    
    df = df[df.lang == "es"]

    pool = Pool(processes=round(len(df.index)/10000))

    result_arr = []
    
    for result in tqdm.tqdm(pool.imap_unordered(ner_helper, df.iterrows()),
                            total=len(df.index)):
        result_arr.append(result)
                
    df = pd.concat(result_arr, axis=0).sort_index()
                
    return df

'''



"""
PUTTING TOGETHER ALL ABOVE FUNCTIONS
"""

import boto3

def clean_df(filepath):
#def clean_df(df):
    
    # reading in data
    print()
    print()
    print("reading in ", filepath)
    print()
    response = s3_client.get_object(Bucket="mt5599", Key=filepath)
    df = pd.read_json(response.get("Body"))

    #df = pd.read_json(filepath)
    
    
    # removing unnecessary columns
    print("removing unnecessary columns")
    print()
    df = keep_columns(df, ["id", "DateTime", "coordinates",
                           "place", "username", "user_id", 
                           "user_location", "tweet_content", "lang"])
    
    df.reset_index(inplace=True)
    df.rename(columns={"index": "TweetNumber"}, inplace=True)

    
    # cleaning tweet content
    print("cleaning content of tweets - removing emojis and other symbols")
    df = clean_tweet(df)
    print()
      
    # extracting mentioned locations from tweets
    #print("getting location from tweets")
    #df = ner(df)
    #print()
    
    # removing unnecessary column
    df.drop("TweetNumber", axis=1)
    
    
    # filtering out tweets that have no location data
    #print("filtering out tweets that have no location data")
    #print()
    #df = has_loc(df)

    # extracting components of place
    print("extracting components of place")
    df = extract_place(df)
    print()

    # extracting components of coordinates
    print("extracting components of coordinates")
    df = extract_coordinates(df)
    print()
    

    # saving cleaned file to S3
    new_filepath = filepath.replace(".json", "_processed_wcontent.feather")
    with io.BytesIO() as feather_buffer:
        df.to_feather(feather_buffer)

        response = s3_client.put_object(
            Bucket="mt5599", Key = new_filepath, Body=feather_buffer.getvalue()
        )

    return df.shape[0]
    


    
    
    

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
    for i in range(75, 150):
        filepath = "tweets/spanish_tweets_2016_" + str(i) + ".json"
        #filepath = "s3://mt5599/tweets/spanish_tweets_2016_" + str(i) + ".json"
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
            
    print("the number of tweets processed: ", no_tweets)
    print("the files that do not exist: ", non_existent_files)
