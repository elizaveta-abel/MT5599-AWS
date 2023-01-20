import numpy as np
import pandas as pd


# Part 2: Get the NER, Location Text and Coordinates


# Get the length of the dataframe
lensptw = len(df["text"])

# First: Create new dataframes to capture the different NER parts
spttext = pd.DataFrame([])
sptlabel = pd.DataFrame([])
spttweet = pd.DataFrame([])

# Second: Initialise the NLP in Spacy
nlp = spacy.load("es_code_news_sm")
for i in np.arange(0, lensptw):
    sptspacy = df["text"][i]
    doc_sp = nlp(sptspacy)
    for ent in doc_sp.ents:
        spttext = spttext.append(pd.DataFrame({'textloc': ent.text}, index = [0]), ignore_index = True)
        sptlabel = sptlabel.append(pd.DataFrame({'label': ent.label_}, index = [0]), ignore_index = True)
        spttweet = spttweet.append(pd.DataFrame({'TweetNumber': [i]}, index = [0]), ignore_index = True)
        

# Third: Combining the Spacy Spanish Findings
frames_spacy_sp = [spttweet, spttext, sptlabel]
finalent_spacy_sp = pd.concat(frames_spacy_sp, axis = 1)

# Fourth: Keep only the GPE parts
gpedf_sp = finalent_spacy_sp[finalent_spacy_sp['label'] == "LOC"]

# Fifth: Merge the GPE on the Main Data Frame
sptlocations = pd.merge(df, gpedf_sp, on = "TweetNumber", how = "left")


'''
##########################################################################

# Make the empty dataframes for lat and long
sptlat = pd.DataFrame([])
sptlong = pd.DataFrame([])
spttweet_2 = pd.DataFrame([])

# Create  the lat and long variables to add to the data

lensptw = len(sptlocations["text"])

# Initialize the loop for the API
for i in np.arange(0, lensptw):
    sptloc = sptlocations["textloc"][i]
    geo_code_result_sp = gmaps.geocode(sptloc)
    # If there is no location in the tweet, enter (0,0) for lat and long
    if pd.isna(sptloc) == True or len(geo_code_result_sp) == 0:
        sptlat = sptlat.append(pd.DataFrame({"Lat": 0}, index = [0]), ignore_index = True)
        sptlong = sptlat.append(pd.DataFrame({"Long": 0}, index = [0]), ignore_index = True)
        spttweet_2 = spttweet_2.append(pd.DataFrame({"TweetNumber": [i]}, index = [0]), ignore_index=True)

    # Else, get the lat and long from the dictionarise and subdictionaries
    else:
        geo_code_dict_sp = geo_code_result_sp[0]
        geo_code_geometry_dict_sp = geo_code_dict_sp.get("geometry")
        geo_location_sp = geo_code_geometry_dict_sp.get("location")
        sptlat = sptlat.append(pd.DataGrame({"Lat": geo_location_sp.get("lat")}, index = [0]), ignore_index = True)
        sptlong = sptlat.append(pd.DataGrame({"Long": geo_location_sp.get("lng")}, index = [0]), ignore_index = True)
        spttweet_2 = spttweet_2.append(pd.DataFrame({"TweetNumber": [i]}, index = [0]), ignore_index = True)
        

# Concatenate dataframes
# https://pandas.pydata.org/docs/user_guide/merging.html
framesloc_sp = [splat, splong]
finalloc_sp = pd.concat(framesloc_sp, axis = 1)


'''