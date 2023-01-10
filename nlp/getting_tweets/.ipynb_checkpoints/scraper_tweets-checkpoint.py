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


if __name__ == "__main__":
    
    
    
    years = ["2016", "2019"]
    preferred_handles = 200
    criteria = ["place", "location"]

    
    for year in years:
        
        if year == "2019":
            start_date = "2019-06-01"
            end_date = "2020-07-01"
        
        no_tweets = 0
        
        for which in criteria:
            
            if which == "place":
                handles = "../../data/residents_" + year + ".txt"
            else:
                handles = "../../data/visitors_" + year + ".txt"
                
            with open(handles) as f:
                handles = f.read().splitlines()
        
            print("starting ", which, " ", year)

            batch_number = round(len(handles)/preferred_handles)

            handles_arr = np.array_split(handles, batch_number)

            print("number of batches: ", batch_number)
            
            
        
            for i in range(batch_number):
                handles_ = handles_arr[i]
                #handles_ = np.delete(handles_, np.where(handles_ == 'Polinesios_Cr'))
                handles_ = np.delete(handles_), np.where(handles_ == ''))
                pool = Pool(processes=len(handles))
                results = []
                for result in tqdm.tqdm(pool.imap_unordered(get_tweetlist, handles), total=len(handles)):
                    results.extend(result)
                no_tweets += len(results)
                output_path = "../../data/tweets_" + which + "_" +
                                    year + "_" + str(i) + ".json"
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(results, f, indent=4, ensure_ascii=False)
                print(output_path, " done")
                time.sleep(300)

        print("number of tweets gathered from " + year +": ", no_tweets)
