import pandas as pd
import json

paths_2016 = ["../../data/2015-10-01-to-2015-11-01.csv",
        "../../data/2015-11-01-to-2015-12-01.csv",
        "../../data/2015-12-01-to-2016-01-01.csv",
        "../../data/2016-01-01-to-2016-02-01.csv",
        "../../data/2016-02-01-to-2016-03-01.csv",
        "../../data/2016-03-01-to-2016-04-01.csv",
        "../../data/2016-04-01-to-2016-05-01.csv",
        "../../data/2016-05-01-to-2016-06-01.csv",
        "../../data/2016-06-01-to-2016-07-01.csv",
        "../../data/2016-07-01-to-2016-08-01.csv",
        "../../data/2016-08-01-to-2016-09-01.csv",
        "../../data/2016-09-01-to-2016-10-01.csv",
        "../../data/2016-10-01-to-2016-11-01.csv"]

paths_2019 = ["../../data/2019-06-01-to-2019-07-01.csv",
             "../../data/2019-07-01-to-2019-08-01.csv",
             "../../data/2019-08-01-to-2019-09-01.csv",
             "../../data/2019-09-01-to-2019-10-01.csv",
             "../../data/2019-10-01-to-2019-11-01.csv",
             "../../data/2019-11-01-to-2019-12-01.csv",
             "../../data/2019-12-01-to-2020-01-01.csv",
             "../../data/2020-01-01-to-2020-02-01.csv",
             "../../data/2020-02-01-to-2020-03-01.csv",
             "../../data/2020-03-01-to-2020-04-01.csv",
             "../../data/2020-04-01-to-2020-05-01.csv",
             "../../data/2020-05-01-to-2020-06-01.csv",
             "../../data/2020-06-01-to-2020-07-01.csv"]

df0_2016 = pd.read_csv(paths_2016[0], on_bad_lines='skip', low_memory=False)
df1_2016 = pd.read_csv(paths_2016[1], on_bad_lines='skip', low_memory=False, lineterminator='\n')
df2_2016 = pd.read_csv(paths_2016[2], on_bad_lines='skip', low_memory=False, lineterminator='\n')
df3_2016 = pd.read_csv(paths_2016[3], on_bad_lines='skip', low_memory=False, lineterminator='\n')
df4_2016 = pd.read_csv(paths_2016[4], on_bad_lines='skip', low_memory=False, lineterminator='\n')
df5_2016 = pd.read_csv(paths_2016[5], on_bad_lines='skip', low_memory=False, lineterminator='\n')
df6_2016 = pd.read_csv(paths_2016[6], on_bad_lines='skip', low_memory=False, lineterminator='\n')
df7_2016 = pd.read_csv(paths_2016[7], on_bad_lines='skip', low_memory=False, lineterminator='\n')
df8_2016 = pd.read_csv(paths_2016[8], on_bad_lines='skip', low_memory=False, lineterminator='\n')
df9_2016 = pd.read_csv(paths_2016[9], on_bad_lines='skip', low_memory=False, lineterminator='\n')
df10_2016 = pd.read_csv(paths_2016[10], on_bad_lines='skip', low_memory=False, lineterminator='\n')
df11_2016 = pd.read_csv(paths_2016[11], on_bad_lines='skip', low_memory=False, lineterminator='\n')
df12_2016 = pd.read_csv(paths_2016[12], on_bad_lines='skip', low_memory=False, lineterminator='\n')

df0_2019 = pd.read_csv(paths_2019[0], on_bad_lines='skip', low_memory=False, lineterminator='\n')
df1_2019 = pd.read_csv(paths_2019[1], on_bad_lines='skip', low_memory=False, lineterminator='\n')
df2_2019 = pd.read_csv(paths_2019[2], on_bad_lines='skip', low_memory=False, lineterminator='\n')
df3_2019 = pd.read_csv(paths_2019[3], on_bad_lines='skip', low_memory=False, lineterminator='\n')
df4_2019 = pd.read_csv(paths_2019[4], on_bad_lines='skip', low_memory=False, lineterminator='\n')
df5_2019 = pd.read_csv(paths_2019[5], on_bad_lines='skip', low_memory=False, lineterminator='\n')
df6_2019 = pd.read_csv(paths_2019[6], on_bad_lines='skip', low_memory=False, lineterminator='\n')
df7_2019 = pd.read_csv(paths_2019[7], on_bad_lines='skip', low_memory=False, lineterminator='\n')
df8_2019 = pd.read_csv(paths_2019[8], on_bad_lines='skip', low_memory=False, lineterminator='\n')
df9_2019 = pd.read_csv(paths_2019[9], on_bad_lines='skip', low_memory=False, lineterminator='\n')
df10_2019 = pd.read_csv(paths_2019[10], on_bad_lines='skip', low_memory=False, lineterminator='\n')
df11_2019 = pd.read_csv(paths_2019[11], on_bad_lines='skip', low_memory=False, lineterminator='\n')
df12_2019 = pd.read_csv(paths_2019[12], on_bad_lines='skip', low_memory=False, lineterminator='\n')

print("finished loading dfs")



def safe_json_loads(string):
    try:
        string = json.loads(string)
    except:
        string = None
    return(string)

def extract_user(df):
    
    df_coord = df

    df_coord.user = df_coord.user.str.replace("'", "\"")
    df_coord.user = df_coord.user.str.replace("None", "\"None\"")
    df_coord.user = df_coord.user.str.replace("True", "\"True\"")
    df_coord.user = df_coord.user.str.replace("False", "\"False\"")
    df_coord.user = df_coord.user.str.replace("datetime.datetime", "\"datetime.datetime")

    df_coord = df_coord.dropna(subset=['user'])
    df_coord = df_coord.reset_index(drop=True)

    for i in range(len(df_coord.user)):
        string = df_coord.user[i]
        string = string.replace(".utc)", ".utc)\"")
        string =  safe_json_loads(string)
        df_coord.user[i] = string

    df_coord = df_coord.dropna(subset=['user'])
    df_coord = df_coord.reset_index(drop=True)    
    
    df_coord['username'] = None
    df_coord['user_location'] = None
    df_coord['user_id'] = None
    df_coord['displayname'] = None
    df_coord['user_description'] = None
    df_coord['user_description_urls'] = None
    df_coord['user_verified'] = None
    df_coord['user_created'] = None
    df_coord['user_followers_count'] = None
    df_coord['user_friends_count'] = None
    df_coord['user_statuses_count'] = None
    df_coord['user_favourites_count'] = None
    df_coord['user_listed_count'] = None
    df_coord['user_media_count'] = None
    df_coord['user_protected'] = None
    df_coord['user_link_url'] = None
    df_coord['link_tcourl'] = None
    df_coord['user_profile_image_url'] = None
    df_coord['user_profile_banner_url'] = None
    df_coord['user_label'] = None
    

    for i in range(len(df_coord['user'])):
        try:
            username = df_coord.user[i]['username']
            location = df_coord.user[i]['location']
            user_id = df_coord.user[i]['id']
            user_displayname = df_coord.user[i]['displayname']
            user_description = df_coord.user[i]['description']
            user_description_urls = df_coord.user[i]['descriptionUrls']
            user_verified = df_coord.user[i]['verified']
            user_created = df_coord.user[i]['created']
            user_followers_count = df_coord.user[i]['followersCount']
            user_friends_count = df_coord.user[i]['friendsCount']
            user_statuses_count = df_coord.user[i]['statusesCount']
            user_favourites_count = df_coord.user[i]['favouritesCount']
            user_listed_count = df_coord.user[i]['listedCount']
            user_media_count = df_coord.user[i]['mediaCount']
            user_protected = df_coord.user[i]['protected']
            user_link_url = df_coord.user[i]['linkUrl']
            user_link_tcourl = df_coord.user[i]['linkTcourl']
            user_profile_image_url = df_coord.user[i]['profileImageUrl']
            user_profile_banner_url = df_coord.user[i]['profileBannerUrl']
            user_label = df_coord.user[i]['label']
            
        except:
            username = None
            location = None
            user_displayname = None
            user_description = None
            user_description_urls = None
            user_verified = None
            user_created = None
            user_followers_count = None
            user_friends_count = None
            user_statuses_count = None
            user_favourites_count = None
            user_listed_count = None
            user_media_count = None
            user_protected = None
            user_link_url = None
            user_link_tcourl = None
            user_profile_image_url = None
            user_profile_banner_url = None
            user_label = None
            
        df_coord['username'][i] = username
        df_coord['user_location'][i] = location
        df_coord['user_id'][i] = user_id
        df_coord['displayname'][i] = user_displayname
        df_coord['user_description'][i] = user_description
        df_coord['user_description_urls'][i] = user_description_urls
        df_coord['user_verified'][i] = user_verified
        df_coord['user_created'][i] = user_created
        df_coord['user_followers_count'][i] = user_followers_count
        df_coord['user_friends_count'][i] = user_friends_count
        df_coord['user_statuses_count'][i] = user_statuses_count
        df_coord['user_favourites_count'][i] = user_favourites_count
        df_coord['user_listed_count'][i] = user_listed_count
        df_coord['user_media_count'][i] = user_media_count
        df_coord['user_protected'][i] = user_protected
        df_coord['user_link_url'][i] = user_link_url
        df_coord['link_tcourl'][i] = user_link_tcourl
        df_coord['user_profile_image_url'][i] = user_profile_image_url
        df_coord['user_profile_banner_url'][i] = user_profile_banner_url
        df_coord['user_label'][i] = user_label

    return df_coord

df0_2016 = extract_user(df0_2016)
print("df0_2016 done")
df1_2016 = extract_user(df1_2016)
print("df1_2016 done")
df2_2016 = extract_user(df2_2016)
print("df2_2016 done")
df3_2016 = extract_user(df3_2016)
print("df3_2016 done")
df4_2016 = extract_user(df4_2016)
print("df4_2016 done")
df5_2016 = extract_user(df5_2016)
print("df5_2016 done")
df6_2016 = extract_user(df6_2016)
print("df6_2016 done")
df7_2016 = extract_user(df7_2016)
print("df7_2016 done")
df8_2016 = extract_user(df8_2016)
print("df8_2016 done")
df9_2016 = extract_user(df9_2016)
print("df9_2016 done")
df10_2016 = extract_user(df10_2016)
print("df10_2016 done")
df11_2016 = extract_user(df11_2016)
print("df11_2016 done")
df12_2016 = extract_user(df12_2016)
print("df12_2016 done")

dfs_2016 = [df0_2016, df1_2016, df2_2016, df3_2016, df4_2016, df5_2016, df6_2016, df7_2016, df8_2016, df9_2016, df10_2016, df11_2016, df12_2016]
df_2016 = pd.concat(dfs_2016, ignore_index=True)

df_2016.drop(labels=["Unnamed: 0"], axis=1, inplace=True)
#df_2016.drop_duplicates(inplace=True)
df_2016.reset_index(drop=True, inplace=True)
df_2016.to_csv("../../data/df_2016.csv")

print("")
print("2016 done")
print("")

df0_2019 = extract_user(df0_2019)
print("df0_2019 done")
df1_2019 = extract_user(df1_2019)
print("df1_2019 done")
df2_2019 = extract_user(df2_2019)
print("df2_2019 done")
df3_2019 = extract_user(df3_2019)
print("df3_2019 done")
df4_2019 = extract_user(df4_2019)
print("df4_2019 done")
df5_2019 = extract_user(df5_2019)
print("df5_2019 done")
df6_2019 = extract_user(df6_2019)
print("df6_2019 done")
df7_2019 = extract_user(df7_2019)
print("df7_2019 done")
df8_2019 = extract_user(df8_2019)
print("df8_2019 done")
df9_2019 = extract_user(df9_2019)
print("df9_2019 done")
df10_2019 = extract_user(df10_2019)
print("df10_2019 done")
df11_2019 = extract_user(df11_2019)
print("df11_2019 done")
df12_2019 = extract_user(df12_2019)
print("df12_2019 done")

dfs_2019 = [df0_2019, df1_2019, df2_2019, df3_2019, df4_2019, df5_2019, df6_2019, df7_2019, df8_2019, df9_2019, df10_2019, df11_2019, df12_2019]
df_2019 = pd.concat(dfs_2019, ignore_index=True)

df_2019.drop(labels=["Unnamed: 0"], axis=1, inplace=True)
#df_2016.drop_duplicates(inplace=True)
df_2019.reset_index(drop=True, inplace=True)
df_2019.to_csv("../../data/df_2019.csv")

print("")
print("2019 done")
