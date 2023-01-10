import pandas as pd
import json





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



if __name__ == "__main__":

    years = ["2016", "2019"]


    for year in years:

        if year == "2016":
            dates = pd.date_range(start='2015-10-01',
                                   end='2016-10-01', freq='MS') # by month start
        else:
            dates = pd.date_range(start='2019-06-01',
                                   end='2020-07-01', freq='MS')


        dates_arr = []
        for i in range(len(dates)-1):
            dates_arr.append(dates[i:i+2]) 


        dfs = []





        for date in dates_arr:
            start_date = str(date[0])
            end_date = str(date[1])

            path = "../../data/" + start_date + "-to-" + end_date + ".csv"  

            df = pd.read_csv(paths_2016[1],
                             on_bad_lines='skip',
                             low_memory=False,
                             lineterminator='\n')

            df = extract_user(df)


            dfs.append(df)



        df_main = pd.concat(dfs, ignore_index=True)

        df_main.drop(labels=["Unnamed: 0"], axis=1, inplace=True)
        #df_2016.drop_duplicates(inplace=True)
        df_main.reset_index(drop=True, inplace=True)
        df_main.to_csv("../../data/df_" + year + ".csv")


