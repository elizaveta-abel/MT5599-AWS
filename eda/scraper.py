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

    