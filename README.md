# MT5599 Masters Project

There are two parts to this project:

- Using geoparsers to extract locations from tweets and ProMed Mail reports
- Using twitter location data to create time series model

The folder `twitter` contains the code required to obtain datasets found in `data`.

This repo is currently just a record of what was done, and is not fully or easily reproducible.
The main reason for this is the use of AWS and cloud computing resources.


## Part 1: Geoparsing Twitter and ProMed

### Data Collection

For a specified year, all the tweets from Argentina were collected using `snscrape`.

### Data Preprocessing

These tweets were cleaned to remove emojis, links, etc.

### Use of Models

#### Machine Translation

Then these tweets were translated into Spanish [insert model here, insert research paper here].

#### Geoparsing

Geoparsing is a combination of Named Entity Recognition to recognise location names, followed by disambiguating these location names by mapping them to coordinates.

*The geoparser has not been chosen or implemented yet, which is an important part of this research paper.*




## Part 2: Time Series

The aim of using a time series model is to predict whether user location obtained from tweets can be used to predict dengue spread in Argentina.

### Data Collection

A larger volume of data was required, therefore the following steps were taken:

1. Sample three days at random from chosen year, and collect all the tweets in Spanish from those days.
2. Extract the user handles from those dates.
3. Collect all the tweets from those user handles from chosen year.

### Data Preprocessing

Since the content of the tweets was not going to be used, only the location and datetime data was retained.
This resulted in [insert number here] tweets from [insert number here] users.


