""""Data Processing Pipeline """
import re
import pandas as pd
import spacy
import py3langid as langid
import json
import unicodedata
import html
import tqdm


def process(s):
    r = unicodedata.normalize("NFC", s)
    r = html.unescape(r)
    r = r.encode("ascii", "ignore").decode()
    r = r.replace('\n', " ")
    r = r.replace('@', " ")
    r = r.replace("#", " ")
    r = re.sub('http://\S+|https://\S+', '', r)
    r = r.lower()
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               "]+", flags=re.UNICODE)
    r = (emoji_pattern.sub(r'', r))  # no emoji
    r = re.sub('\s{2,}', ' ', r)
    return r

def format_datetime(df):
    df["DateTime"] = pd.to_datetime(df["DateTime"])
    df["DateTime"] = pd.DatetimeIndex(df["DateTime"]).floor('S').tz_localize(None)
    return df

'''
def filter(text,nlp, blacklist, keywords):
        lemmas = [f"{token.lemma_}_{token.pos_}" for token in nlp(text)]
        tags = []
        for black in blacklist:
            if black in lemmas:
                return False
        for key in keywords.keys():
            if any(item in keywords[key] for item in lemmas):
                return True and len(text) >= 50 and langid.classify(text)[0] == 'en'

        return False
'''

def get_processed_twitter_df(df):

    print("ready for processing")
    df = format_datetime(df)
    tweets = df["tweet_content"]
    df["tweet"] = [process(x) for x in tqdm.tqdm(tweets)]
    print("text formatted")

    print("twitter processing finished")
    return df

if __name__ == "__main__":
    
    paths = ["../data/df_2016.feather", "../data/df_2019.feather"]
    
    for path in paths:
    
        df = pd.read_feather(path)
        
        df = get_processed_twitter_df(df)

        print("cleaned")
        print(df.tweet)
    
        output_path = path + "_cleaned.feather"
        df.to_feather(output_path)