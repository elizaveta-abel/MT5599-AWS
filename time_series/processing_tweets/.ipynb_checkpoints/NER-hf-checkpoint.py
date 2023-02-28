#import numpy as np
# pip install pandas transformers torch torchvision torchaudio boto3 feather-format tqdm 

# Choose an instance with GPU cores (since working with HuggingFace model)

import pandas as pd
from transformers import AutoTokenizer, AutoModelForTokenClassification, pipeline
import boto3
import os
from time import process_time
import io
import tqdm
import feather
import numpy as np
import json
import re
from multiprocessing import Pool # multithreading
from time import process_time
import unicodedata
import html




tokenizer = AutoTokenizer.from_pretrained("Davlan/bert-base-multilingual-cased-ner-hrl")

model = AutoModelForTokenClassification.from_pretrained("Davlan/bert-base-multilingual-cased-ner-hrl")

nlp = pipeline("ner", model=model, tokenizer=tokenizer, #device=0,
               aggregation_strategy="simple") #group_sub_entities

pd.set_option('display.max_rows', None)

'''
# extracting place components
def ner(df):
    
    preferred_tweets = 10
    
    batch_number = round(df.shape[0]/preferred_tweets)
    tweetid_arr = np.array_split(df, batch_number)
    
    print("number of batches: ", batch_number)
    
    pool = Pool(processes=round(batch_number*1))

    result_arr = []
    
    for result in tqdm.tqdm(pool.imap_unordered(ner_helper, tweetid_arr),
                            total=len(tweetid_arr)):
        result_arr.append(result)
                
    df = pd.concat(result_arr, axis=0) #.transpose().sort_index()
                
    return df
'''


def clean_tweet_helper(row):
    
    s = row[1]["tweet_content"]
    
    r = unicodedata.normalize("NFC", s)
    r = html.unescape(r)
    r = r.encode("ascii", "ignore").decode()
    r = r.replace('\n', " ")
    #r = r.replace('@', " ")
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
    
    # new
    r = re.sub(r'(\s)?@\w+', '', r)
    
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




                            
def ner_helper(df_temp):
    
    df_temp.drop("TweetNumber", axis=1, inplace=True)
    df_temp.reset_index(inplace=True, drop=True)
    df_temp.reset_index(inplace=True)
    df_temp.rename(columns={"index": "TweetNumber"}, inplace=True)
    
        
    tweets_clean = df_temp.tweet_clean.tolist()
       
    ner_results = nlp(tweets_clean)
    
    spttext = pd.DataFrame([])
    spttype = pd.DataFrame([])
    spttweet = pd.DataFrame([])
    
    for k, ner_result in tqdm.tqdm(enumerate(ner_results)):
        
        for i, result in enumerate(ner_result):
            
            
            spttext = pd.concat([spttext, pd.DataFrame({"ner_word": result['word']}, index = [0])], ignore_index = True)
            spttype = pd.concat([spttype, pd.DataFrame({"ner_type": result['entity_group']}, index = [0])], ignore_index = True)
            spttweet = pd.concat([spttweet, pd.DataFrame({'TweetNumber': k}, index = [0])], ignore_index = True)
            '''
            combined_entity = None

            length = len(ner_result)

            if result['entity_group'] == 'LOC':

                combined_entity = result['word']

                j = i + 1
                
                if j < length:
                    
                    while ner_result[j]['entity'] == 'I-LOC':
                        
                        if "##" in ner_result[j]['word']:
                            combined_entity = combined_entity + ner_result[j]['word'].strip("#")
                        else:
                            combined_entity = combined_entity + ' ' + ner_result[j]['word']

                        j += 1

                        if j >= length:
                            break
            '''


    # Third: Combining the Spacy Spanish Findings
    frames_spacy_sp = [spttweet, spttype, spttext]
    finalent_spacy_sp = pd.concat(frames_spacy_sp, axis = 1)
    
    #print(finalent_spacy_sp)
    #print(df)
    

    gpedf_sp = finalent_spacy_sp

    # Fifth: Merge the GPE on the Main Data Frame
    sptlocations = pd.merge(df_temp, gpedf_sp, on = "TweetNumber", how = "left")

    return sptlocations


def extract_ner_locations(df):
    
    locations = df[df["ner_type"] == "LOC"].ner_word.value_counts().rename_axis('unique_values').reset_index(name='counts')
                            
    return locations
                            



def clean_df(filepath):
#def clean_df(df):
    
    # reading in data
    print()
    print()
    print("reading in ", filepath)
    print()
    response = s3_client.get_object(Bucket="mt5599", Key=filepath)
    df = pd.read_feather(io.BytesIO(response['Body'].read()))
    
    # REMOVE FOR FINAL
    #df = df[0:10000]
    
    print("cleaning tweets")
    df = clean_tweet(df)
    
    #print("splitting df")
    #preferred_tweets = 20000
    
    #batch_number = round(df.shape[0]/preferred_tweets)
    #tweetid_arr = np.array_split(df, batch_number)
    
   # print("number of batches: ", len(tweetid_arr))
                            
    print("starting ner")
    print()
    '''
    new_df = pd.DataFrame()
    for i, temp_df in enumerate(tweetid_arr): 
        print("starting batch ", i)
        new_df = pd.concat([new_df, ner_helper(temp_df)])
     ''' 
    
    new_df = ner_helper(df)
    
    #print("adding all dfs together")
    #df = pd.concat(ner_dfs, axis=0)
    #print("df.shape: ", df.shape)
   # print(df)

    df = new_df
    
    #print(df[["tweet_clean", "ner_loc"]])
        
    locations = extract_ner_locations(df)

    df.reset_index(inplace=True)
    df.drop("TweetNumber", inplace=True, axis=1)
    
    # saving cleaned file to S3
    new_filepath = filepath.replace(".feather", "_ner.feather")
    with io.BytesIO() as feather_buffer:
        df.to_feather(feather_buffer)

        response = s3_client.put_object(
            Bucket="mt5599", Key = new_filepath, Body=feather_buffer.getvalue()
        )
        
    #print("locations from ")
    #print(locations)
    locations_filepath = filepath.replace("tweets/spanish_tweets_2016_", "../../data/unique_locations_")
    locations_filepath = locations_filepath.replace(".feather", ".csv")
    #locations.to_csv(locations_filepath)   
    
    locations.to_csv(locations_filepath)
    '''
    with open(locations_filepath, "w") as f:
        for location in locations:
            f.write(f"{location}\n")
    '''
                    
#    return df
    return len(locations.index)
    

    



       

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
    
    """
    
    filepaths = []
    # for i in range(0, 4)
    # skipped: 92
    #for i in range(530, 568):
    
    missed_paths = [253,
                     266,
                     267,
                     268,
                     269,
                     270,
                     271,
                     272,
                     273,
                     274,
                     275,
                     276,
                     277,
                     278,
                     279,
                     280,
                     281,
                     282,
                     283,
                     284,
                     285,
                     286,
                     287,
                     288,
                     289,
                     290,
                     291,
                     292,
                     293,
                     294,
                     295,
                     296,
                     297,
                     298,
                     299,
                     300,
                     314,
                     354,
                     355,
                     356,
                     357,
                     358,
                     359,
                     360,
                     361,
                     362,
                     363,
                     364,
                     365,
                     396,
                     397,
                     398,
                     399,
                     400,
                     401,
                     402,
                     403,
                     404,
                     405,
                     406,
                     407,
                     408,
                     409,
                     410,
                     411,
                     412,
                     413,
                     414,
                     415,
                     416,
                     417,
                     418,
                     419,
                     420,
                     431,
                     432,
                     433,
                     434,
                     435,
                     436,
                     497,
                     498,
                     499,
                     504,
                     509,
                     510,
                     511,
                     512,
                     513,
                     514,
                     529,
                     530,
                     531,
                     532,
                     533,
                     534,
                     535,
                     536,
                     537,
                     538,
                     543,
                     544,
                     545,
                     548,
                     549,
                     550,
                     551,
                     552,
                     553,
                     554,
                     555,
                     556,
                     557,
                     558,
                     559,
                     562,
                     563,
                     564,
                     566]
    
    for i in missed_paths:
        #filepath = "tweets/spanish_tweets_2016_processed_wcontent_cleaned_" + str(i) + ".feather"
        filepath = "tweets/spanish_tweets_2016_" + str(i) + "_processed_wcontent.feather"
        filepaths.append(filepath)

    no_locations = 0
    non_existent_files = []
    for filepath in filepaths:
        try:
            t1_start = process_time()
            no_locations = clean_df(filepath)
            #no_tweets = no_tweets + clean_df(filepath)
            t1_stop = process_time()
            print()
            print("elapsed time: ", t1_stop - t1_start)
            print()
        except:
            non_existent_files.append(filepath)
            continue
            
    print("the number of unique locations: ", no_locations)
    print("the files that do not exist: ", non_existent_files)


    """
    
    text = ["Last night's party was a blast!",
            "The decorations at last night's party were so flibberdoodleicious that I couldn't stop taking photos!",
           "afsgghdshkjfdjkgfklkfjdh gahsghjdk ghs shgjkdh",
           "I love eating sushi, especially salmon nigiri!",
           "I saw her duck, it was so adorable!"]
    
    print("encoded text")
    encoded_text = tokenizer(text)
    print(encoded_text)
    print()
    print("tokens")
    input_ids = encoded_text.input_ids
    print(input_ids)
    for ids in input_ids:
        tokens = tokenizer.convert_ids_to_tokens(ids)
        print(tokens)
        #print(encoded_textx)
    
    print("vocab size")
    print(tokenizer.vocab_size)