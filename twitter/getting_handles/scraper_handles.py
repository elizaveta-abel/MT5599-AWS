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
    
  