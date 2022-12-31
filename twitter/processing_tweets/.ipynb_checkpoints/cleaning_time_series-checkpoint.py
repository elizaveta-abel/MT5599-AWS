import pandas as pd
import json
import re
import tqdm
from multiprocessing import Pool # multithreading
import tqdm
from time import process_time
import feather
  


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
    
    df_coord['place_full_name'] = "None"
    df_coord['place_name'] = "None"
    df_coord['place_type'] = "None"
    df_coord['place_country'] = "None"
    df_coord['place_country_code'] = "None"

    
    pool = Pool(processes=round(len(df_coord.index)/1000))

    result_arr = []
    
    for result in tqdm.tqdm(pool.imap_unordered(extract_place_helper, df_coord.iterrows()),
                            total=len(df_coord.index)):
        result_arr.append(result)
                
    df_coord = pd.concat(result_arr, axis=1).transpose().sort_index()
                
    return df_coord






def extract_coordinates_helper(row):
    if row[1]["coordinates"] != "None":
        try:
            split_by = "Coordinates\(longitude=|, latitude=|\)"
            temp = re.split(split_by, row[1]["coordinates"])

            row[1]['coordinates_longitude'] = temp[1]
            row[1]['coordinates_latitude'] = temp[2]

        except:
            row[1]['coordinates_longitude'] = None
            row[1]['coordinates_latitude'] = None

    return row[1]


# extracting place components
def extract_coordinates(df):
    
    df_coord = df
    
    df_coord['coordinates_longitude'] = "None"
    df_coord['coordinates_latitude'] = "None"
    
    
    pool = Pool(processes=round(len(df_coord.index)/1000))

    result_arr = []
    
    for result in tqdm.tqdm(pool.imap_unordered(extract_coordinates_helper, df_coord.iterrows()),
                            total=len(df_coord.index)):
        result_arr.append(result)
                
    df_coord = pd.concat(result_arr, axis=1).transpose().sort_index()
                
    return df_coord



def clean_df(filepath):
    
    # reading in data
    print("reading in ", filepath)
    print()
    df = pd.read_json(filepath)

    # removing unnecessary columns
    print("removing unnecessary columns")
    print()
    df = keep_columns(df, ["id", "DateTime", "coordinates", "place", "username", "user_location"])

    # filtering out tweets that have no location data
    print("filtering out tweets that have no location data")
    print()
    df = has_loc(df)

    # extracting components of place
    print("extracting components of place")
    df = extract_place(df)
    print()

    # extracting components of coordinates
    print("extracting components of coordinates")
    df = extract_coordinates(df)
    print()
    
    new_filepath = filepath.replace(".json", "_processed.feather")
    df.to_feather(new_filepath)

    return df.shape[0]




if __name__ == "__main__":
    
    filepaths = []
    for i in range(0, 567):
        filepath = "../../data/spanish_tweets_2016_" + str(i) + ".json"
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
    
    
    
    