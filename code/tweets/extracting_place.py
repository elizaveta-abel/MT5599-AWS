import pandas as pd
import json
import re
import tqdm


def safe_json_loads(string):
    try:
        string = json.loads(string)
    except:
        string = None
    return(string)



def extract_place(df):
    
    df_coord = df
    
    df_coord['place_full_name'] = "None"
    df_coord['place_name'] = "None"
    df_coord['place_type'] = "None"
    df_coord['place_country'] = "None"
    df_coord['place_country_code'] = "None"

    for i in tqdm.tqdm(range(df_coord.shape[0])):

        if df_coord.place[i] != "None":
            try:
                split_by = "Place\(fullName='|', name='|', type='|', country='|', countryCode='|'\)"
                temp = re.split(split_by, df_coord.place[i])

                df_coord['place_full_name'][i] = temp[1]
                df_coord['place_name'][i] = temp[2]
                df_coord['place_type'][i] = temp[3]
                df_coord['place_country'][i] = temp[4]
                df_coord['place_country_code'][i] = temp[5]
            except:
                df_coord['place_full_name'][i] = None
                df_coord['place_name'][i] = None
                df_coord['place_type'][i] = None
                df_coord['place_country'][i] = None
                df_coord['place_country_code'][i] = None
                
            

    return df_coord

import tqdm
import re

def extract_coordinates(df):
    
    df_coord = df
    
    df_coord['coordinates_longitude'] = "None"
    df_coord['coordinates_latitude'] = "None"

    for i in tqdm.tqdm(range(df_coord.shape[0])):

        if df_coord.coordinates[i] != "None":
            try:
                split_by = "Coordinates\(longitude=|, latitude=|\)"
                temp = re.split(split_by, df_coord.coordinates[i])

                df_coord['coordinates_longitude'][i] = temp[1]
                df_coord['coordinates_latitude'][i] = temp[2]

            except:
                df_coord['coordinates_longitude'][i] = None
                df_coord['coordinates_latitude'][i] = None
                
            

    return df_coord


df_2016 = pd.read_feather("../data/df_2016.feather_cleaned.feather")
print("finished loading df 2016")
df_2016 = extract_place(df_2016)
print("2016 place extracted")
df_2016 = extract_coordinates(df_2016)
print("2016 coordinates extracted")
df_2016.to_feather("../data/df_2016_cleaned_with_place.feather")
print("2016 done")

df_2019 = pd.read_feather("../data/df_2019.feather_cleaned.feather")
print("finished loading df 2019")
df_2019 = extract_place(df_2019)
print("2019 place extracted")
df_2019 = extract_coordinates(df_2019)
print("2019 coordinates extracted")
df_2019.to_feather("../data/df_2019#_cleaned_with_place.feather")
print("2019 done")