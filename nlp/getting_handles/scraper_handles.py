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


""" NEEDS PROPER COMMENTING """


def get_tweetlist(date):

    start_date = str(date[0])
    end_date = str(date[1])
    
    
    df = pd.DataFrame(scraper(f'near:Argentina within:10km since:{start_date} until {end_date}').get_items())
    
    
    path = "../../data/" + start_date + "-to-" + end_date + ".csv"  
    df.to_csv(path)
    print(path, " done!")
    
    no_tweets = df.shape[0]
    
    return no_tweets
    
    
    
if __name__ == "__main__":
    
    dates1 = pd.date_range(start='2015-10-01', end='2016-10-01', freq='MS') # by month start
    dates2 = pd.date_range(start='2019-06-01', end='2020-07-01', freq='MS')

    
    
    dates_arr = []
    for i in range(len(dates1)-1):
        dates_arr.append(dates1[i:i+2]) 
    for i in range(len(dates2)-1):
        dates_arr.append(dates2[i:i+2]) 
        
        
    print("number of batches: ", len(dates_arr))

        
        
    pool = Pool(processes=len(dates_arr))
    no_tweets = 0
    for result in tqdm.tqdm(pool.imap_unordered(get_tweetlist, dates_arr),
                            total=len(dates_arr)):
        no_tweets += len(result)
        
    print("number of tweets gathered: ", no_tweets)
    