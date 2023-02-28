# pip install geopy feather-format pandas boto3 tqdm


import geopy.distance
import numpy as np
from multiprocessing import Pool # multithreading
import pandas as pd
import boto3
import pandas as pd
import tqdm
import feather
from time import process_time
import os
import io




def distance_helper(row):
    
    if (pd.notna(row[1]["coordinates_latitude"]) &
        pd.notna(row[1]["coordinates_longitude"]) &
        pd.notna(row[1]["gmaps_address"])):
        
        
        tweet_lat = row[1]["coordinates_latitude"]
        tweet_long = row[1]["coordinates_longitude"]
        gmaps_lat = row[1]["gmaps_lat"]
        gmaps_long = row[1]["gmaps_long"]

        tweet_coords = (tweet_lat, tweet_long)
        gmaps_coords = (gmaps_lat, gmaps_long)

        row[1]["distance"] = geopy.distance.geodesic(tweet_coords, gmaps_coords).km

    return row[1]





def distance(df):
    
    df['distance'] = np.nan
    
    pool = Pool(processes=round(len(df.index)/1000))

    result_arr = []
    
    for result in tqdm.tqdm(pool.imap_unordered(distance_helper, df.iterrows()),
                            total=len(df.index)):
        result_arr.append(result)
                
    df = pd.concat(result_arr, axis=1).transpose().sort_index()
                
    return df





if __name__ == "__main__":

    loc_lookup = pd.read_csv("../../data/loc_lookup.csv")





    #dfs = []

    missed_files = []

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

    for i in range(300, 450):

        print("starting ", i)

        filepath = "tweets/spanish_tweets_2016_" + str(i) + "_processed_wcontent_ner.feather"

        try:

            t1_start = process_time()


            response = s3_client.get_object(Bucket="mt5599", Key=filepath)
            df = pd.read_feather(io.BytesIO(response['Body'].read()))


            df.drop("index", inplace=True, axis=1)
            #dfs.append(df)

            df_loc = pd.merge(df, loc_lookup, how="left", on="ner_word")

            #df_loc = df_loc[0:1000]

            df_loc = distance(df_loc)

            # saving cleaned file to S3
            new_filepath = "final/processed_tweets_small_" + str(i) + ".feather"
            with io.BytesIO() as feather_buffer:
                df_loc.to_feather(feather_buffer)

                response = s3_client.put_object(
                    Bucket="mt5599", Key = new_filepath, Body=feather_buffer.getvalue()
                )



            t1_stop = process_time()
            print()
            print("elapsed time: ", t1_stop - t1_start)
            print()

        except:

            missed_files.append(i)
            continue