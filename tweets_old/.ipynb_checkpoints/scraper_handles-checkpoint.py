"""Twitter Scraping Tool"""
import json
import snscrape.modules.twitter as sntwitter
from _datetime import datetime, timedelta
from multiprocessing import Pool # multithreading
import tqdm # for progress bar
import pandas as pd
import itertools 
import numpy as np # to split handles
import time # to slow down scraper

scraper = sntwitter.TwitterSearchScraper

start_date = "2015-10-01_00:00:00_UTC"
end_date   = "2015-10-01_01:00:00_UTC"

#start_date = "2019-06-01"
#end_date = "2020-07-01"


def get_tweetlist(date):
    tweetlist = []
    start_date = str(date[0]).replace(" ", "_") + "_UTC"
    end_date = str(date[1]).replace(" ", "_") + "_UTC"
    for idx, tweet in enumerate(scraper(f'lang:es since:{start_date} until:{end_date}').get_items()):
        
        try:
            
            tweetlist.append({"id": str(tweet.id),
                            "DateTime": str(tweet.date),
                            "tweet_content": str(tweet.content),
                            "cashtags": str(tweet.cashtags),
                            "coordinates": str(tweet.coordinates),
                            "hashtags": str(tweet.hashtags),
                            "lang": str(tweet.lang),
                            "like_count": str(tweet.likeCount),
                            "media": str(tweet.media),
                            "mentioned_users": str(tweet.mentionedUsers),
                            "quoteCount": str(tweet.quoteCount),
                            "quotedTweet": str(tweet.quotedTweet),
                            "renderedContent": str(tweet.renderedContent),
                            "replyCount": str(tweet.replyCount),
                            "retweetCount": str(tweet.retweetCount),
                            "retweetedTweet": str(tweet.retweetedTweet),
                            "source": str(tweet.source),

                            # user information
                            "username": str(tweet.user.username),
                            "user_location": str(tweet.user.location),
                            "user_id": str(tweet.user.id),
                            "user_displayname": str(tweet.user.displayname),
                            "user_description": str(tweet.user.description),
                            "user_description_urls": str(tweet.user.descriptionUrls),
                            "user_verified": str(tweet.user.verified),
                            "user_created": str(tweet.user.created),
                            "user_followers_count": str(tweet.user.followersCount),
                            "user_friends_count": str(tweet.user.friendsCount),
                            "user_statuses_count": str(tweet.user.statusesCount),
                            "user_favourites_count": str(tweet.user.favouritesCount),
                            "user_listed_count": str(tweet.user.listedCount),
                            "user_media_count": str(tweet.user.mediaCount),
                            "user_protected": str(tweet.user.protected),
                            "user_link_url": str(tweet.user.linkUrl),
                            "user_link_tcourl": str(tweet.user.linkTcourl),
                            "user_profile_image_url": str(tweet.user.profileImageUrl),
                            "user_profile_banner_url": str(tweet.user.profileBannerUrl),
                            "user_label": str(tweet.user.label),

                            # place information
                            "place_country": str(tweet.place.country),
                            "place_country_code": str(tweet.place.countryCode),
                            "place_full_name": str(tweet.place.fullName),
                            "place_name": str(tweet.place.name),
                            "place_type": str(tweet.place.type),

                            # coordinates information
                            "coordinates_longitude": str(tweet.coordinates.longitude),
                            "coordinates_latitude": str(tweet.coordinates.latitude)

                            })
            
        except:
            pass
        
        
    output_path = "../data/tweets_spanish_"  + start_date + ".json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(tweetlist, f, indent=4, ensure_ascii=False)

    return tweetlist
 

if __name__ == "__main__":
    
    
    dates1 = pd.date_range(start='2015-12-01', end='2015-12-02', freq='H')
    dates2 = pd.date_range(start='2016-04-15', end='2016-04-16', freq='H')
    dates3 = pd.date_range(start='2016-10-30', end='2016-10-31', freq='H')


    dates_arr = []
    for i in range(len(dates1)-1):
        dates_arr.append(dates1[i:i+2]) 
    for i in range(len(dates2)-1):
        dates_arr.append(dates2[i:i+2]) 
    for i in range(len(dates3)-1):
        dates_arr.append(dates3[i:i+2]) 

    pool = Pool(processes=len(dates_arr))
    no_tweets = 0
    for result in tqdm.tqdm(pool.imap_unordered(get_tweetlist, dates_arr), total=len(dates_arr)):
        no_tweets += len(result)
    print(no_tweets)
    
    
    '''
    
    # 2016
    handles_location_2016 = "../data/argentina_residents_by_user_location_2016.txt"
    with open(handles_location_2016) as f:
        handles_location_2016 = f.read().splitlines()
        
    handles_place_2016 = "../data/simpler_argentina_residents_by_place_2016.txt"
    with open(handles_place_2016) as f:
        handles_place_2016 = f.read().splitlines()
        
    
    print("starting place")
    
    preferred_handles = 200
    year = "2016"
    
    batch_number_place = round(len(handles_place_2016)/preferred_handles)
    handles_place_arr = np.array_split(handles_place_2016, batch_number_place)
    
    print("number of batches: ", batch_number_place)
    
    
    no_tweets_place = 0
    for i in range(20, batch_number_place):
        handles = handles_place_arr[i]
        handles = np.delete(handles, np.where(handles == 'Polinesios_Cr'))
        handles = np.delete(handles, np.where(handles == ''))
        pool = Pool(processes=len(handles))
        results = []
        for result in tqdm.tqdm(pool.imap_unordered(get_tweetlist, handles), total=len(handles)):
            results.extend(result)
        no_tweets_place += len(results)
        output_path = "../data/tweets_place_" + year + "_" + str(i) + ".json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)
        print(output_path, " done")
        time.sleep(300)
        
    print("number of tweets gathered from place: ", no_tweets_place)
    

    print("starting location")
        
    batch_number_location = round(len(handles_location_2016)/preferred_handles)
    handles_location_arr = np.array_split(handles_location_2016, batch_number_location)
    
    print("number of batches: ", batch_number_location)
        
    no_tweets_location = 0
    for i in range(batch_number_location):
        handles = handles_location_arr[i]
        pool = Pool(processes=len(handles))
        results = []
        for result in tqdm.tqdm(pool.imap_unordered(get_tweetlist, handles), total=len(handles)):
            results.extend(result)
        no_tweets_location += len(results)
        output_path = "../data/tweets_location_"  + year + "_" + str(i) + ".json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)
        print(output_path, " done")
        
    print("number of tweets gathered from location: ", no_tweets_location)
    
    print("total tweets gathered: ", no_tweets_place + no_tweets_location)

    
    
    
    
    
    # 2019
    handles_location_2019 = "../data/argentina_residents_by_user_location_2019.txt"
    with open(handles_location_2019) as f:
        handles_location_2019 = f.read().splitlines()
        
    handles_place_2019 = "../data/simpler_argentina_residents_by_place_2019.txt"
    with open(handles_place_2019) as f:
        handles_place_2019 = f.read().splitlines()
        
    
        
    print("starting place")
    
    preferred_handles = 200
    year = "2019"
    
    batch_number_place = round(len(handles_place_2019)/preferred_handles)
    handles_place_arr = np.array_split(handles_place_2019, batch_number_place)
    
    print("number of batches: ", batch_number_place)
    
    
    no_tweets_place = 0
    for i in range(batch_number_place):
        handles = handles_place_arr[i]
        pool = Pool(processes=len(handles))
        results = []
        for result in tqdm.tqdm(pool.imap_unordered(get_tweetlist, handles), total=len(handles)):
            results.extend(result)
        no_tweets_place += len(results)
        output_path = "../data/tweets_place_" + year + "_" + str(i) + ".json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)
        print(output_path, " done")
        time.sleep(300)
        
    print("number of tweets gathered from place: ", no_tweets_place)
        
        

    

    print("starting location")
        
    batch_number_location = round(len(handles_location_2019)/preferred_handles)
    handles_location_arr = np.array_split(handles_location_2019, batch_number_location)
    
    print("number of batches: ", batch_number_location)
        
    no_tweets_location = 0
    for i in range(batch_number_location):
        handles = handles_location_arr[i]
        pool = Pool(processes=len(handles))
        results = []
        for result in tqdm.tqdm(pool.imap_unordered(get_tweetlist, handles), total=len(handles)):
            results.extend(result)
        no_tweets_location += len(results)
        output_path = "../data/tweets_location_"  + year + "_" + str(i) + ".json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)
        print(output_path, " done")
        
    print("number of tweets gathered from location: ", no_tweets_location)
    
    print("total tweets gathered: ", no_tweets_place + no_tweets_location)

    

    
    
    ######## 2016, user_location
    handles_path = "../data/argentina_residents_by_user_location_2016.txt"
    with open(handles_path) as f:
        handles = f.read().splitlines()

    pool = Pool(processes=len(handles))
    results = []
    for result in tqdm.tqdm(pool.imap_unordered(get_tweetlist, handles), total=len(handles)):
        results.extend(result)

    output_path = "../data/tweets_location_2016.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)
    print("")
    print("2016 location done")
    print("")
    
    
    ######## 2016, place
    handles_path = "../data/argentina_residents_by_place_2016.txt"
    with open(handles_path) as f:
        handles = f.read().splitlines()

    pool = Pool(processes=len(handles_1))
    results = []
    for result in tqdm.tqdm(pool.imap_unordered(get_tweetlist, handles_1), total=len(handles_1)):
        results.extend(result)
    output_path = "../data/tweets_place_2016_1.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)
    
    pool = Pool(processes=len(handles_2))
    results = []
    for result in tqdm.tqdm(pool.imap_unordered(get_tweetlist, handles_2), total=len(handles_2)):
        results.extend(result)
    output_path = "../data/tweets_place_2016_2.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)
        
    pool = Pool(processes=len(handles_3))
    results = []
    for result in tqdm.tqdm(pool.imap_unordered(get_tweetlist, handles_3), total=len(handles_3)):
        results.extend(result)
    output_path = "../data/tweets_place_2016_3.json"   
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)
   
    print("")
    print("2016 place 3 done")
    print("")
    
    
    print("")
    print("2016 done")
    print("")
    
    
    ######## 2019, user_location
    handles_path = "../data/argentina_residents_by_user_location_2019.txt"
    with open(handles_path) as f:
        handles = f.read().splitlines()

    pool = Pool(processes=len(handles))
    results = []
    for result in tqdm.tqdm(pool.imap_unordered(get_tweetlist, handles), total=len(handles)):
        results.extend(result)

    output_path = "../data/tweets_location_2019.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)
    print("")
    print("2019 location done")
    print("")
        
    ######## 2019, place
    handles_path = "../data/argentina_residents_by_place_2019.txt" 
    with open(handles_path) as f:
        handles = f.read().splitlines()

    pool = Pool(processes=len(handles))
    results = []
    for result in tqdm.tqdm(pool.imap_unordered(get_tweetlist, handles), total=len(handles)):
        results.extend(result)

    output_path = "../data/tweets_place_2019.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)
    print("")
    print("2019 place done")
    print("")
    
    print("")
    print("2019 done")
    print("")
    
    '''
    

############################################ Old (original) code

'''
   
"""Twitter Scraping Tool"""
import json
import snscrape.modules.twitter as sntwitter
from _datetime import datetime, timedelta
from multiprocessing import Pool
import tqdm # for progress bar
import pytz # for current time
import pandas as pd
import itertools

scraper = sntwitter.TwitterSearchScraper

################################ 2019 ############################################

start_date = "2019-06-01"
end_date = "2019-07-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")

start_date = "2019-07-01"
end_date = "2019-08-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")

start_date = "2019-08-01"
end_date = "2019-09-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")

start_date = "2019-09-01"
end_date = "2019-10-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")

start_date = "2019-10-01"
end_date = "2019-11-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")

start_date = "2019-11-01"
end_date = "2019-12-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")

start_date = "2019-12-01"
end_date = "2020-01-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")

start_date = "2020-01-01"
end_date = "2020-02-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")

start_date = "2020-02-01"
end_date = "2020-03-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")

start_date = "2020-03-01"
end_date = "2020-04-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")


start_date = "2020-04-01"
end_date = "2020-05-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")


start_date = "2020-05-01"
end_date = "2020-06-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")

start_date = "2020-06-01"
end_date = "2020-07-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")


################################ 2016 ############################################

start_date = "2015-10-01"
end_date = "2015-11-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")

start_date = "2015-11-01"
end_date = "2015-12-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")

start_date = "2015-12-01"
end_date = "2016-01-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")

start_date = "2016-01-01"
end_date = "2016-02-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")

start_date = "2016-02-01"
end_date = "2016-03-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")

start_date = "2016-03-01"
end_date = "2016-04-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")

start_date = "2016-04-01"
end_date = "2016-05-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")

start_date = "2016-05-01"
end_date = "2016-06-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")

start_date = "2016-06-01"
end_date = "2016-07-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")

start_date = "2016-07-01"
end_date = "2016-08-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")

start_date = "2016-08-01"
end_date = "2016-09-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")

start_date = "2016-09-01"
end_date = "2016-10-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")

start_date = "2016-10-01"
end_date = "2016-11-01"
df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until:{end_date}').get_items())
path = "../data/" + start_date + "-to-" + end_date + ".csv"  
df.to_csv(path)
print(start_date, "done")



if __name__ == "__main__":
    print("")
    print("done")
    
'''