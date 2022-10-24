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

start_date = "2015-10-01"
end_date = "2016-11-01"

#start_date = "2019-06-01"
#end_date = "2020-07-01"

def get_tweetlist(handle: str):
    tweetlist = []
    for idx, tweet in enumerate(scraper(f'from:{handle} since:{start_date} until:{end_date}').get_items()):
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
                        "place": str(tweet.place),
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
                        "user_label": str(tweet.user.label)})

    print(f"{handle} completed!")
    return tweetlist

'''

# function to automatically detect number of handles and run scraper
def scraper(handles_place, year, preferred_handles):
    
    print("starting place")
    
    batch_number_place = round(len(handles_place)/preferred_handles)
    handles_place_arr = np.array_split(handles_place, batch_number_place)
    
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
        
    batch_number_location = round(len(handles_location)/preferred_handles)
    handles_location_arr = np.array_split(handles_location, batch_number_location)
    
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

        
    return "done!"
'''

if __name__ == "__main__":
    
    
    
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
        
        
    '''
    

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
   