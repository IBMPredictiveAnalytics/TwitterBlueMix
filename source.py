# Request Twitter Decahose Data
# Developed by Jonathan Langefeld (jonathan.langefeld@de.ibm.com)
import spss.pyspark.runtime
from pyspark.sql.types import *
import pandas as pd
import urllib
import json
import numpy as np
from datetime import datetime
cxt = spss.pyspark.runtime.getContext() 
sqlContext = cxt.getSparkSQLContext()
sc = cxt.getSparkContext()
customQuery = %%customQueryU%%
cQuery = '%%cQueryU%%'
listOfAuthors = %%listOfAuthorsU%%
authors = '%%authorsU%%'
dates = %%datesU%%
fromDate = '%%fromU%%'
toDate = '%%toU%%'
url = '%%urlU%%'
maxTweets = int('%%maxTweetsU%%')
def to_utc(dt):
    format = '%Y-%m-%d %H:%M:%S'
    delta = datetime.strptime(datetime.strftime(datetime.now(),format),format) - datetime.strptime(datetime.strftime(datetime.utcnow(),format),format)
    #dt=fromDate
    dt = dt[:19]+dt[-5:]
    dt = datetime.strptime(dt,"%a %b %d %H:%M:%S %Y")
    utctime = dt - delta
    return(datetime.strftime(utctime,'%Y-%m-%dT%H:%M:%SZ'))
if maxTweets == 0:
    maxTweets = 9223372036854775807
url = url + ':443/api/v1/messages/search?'
cols = [
    "author",
    "gender",
    "sentimentPolarity",
    "verb",
    "postedTime",
    "generatorDisplayName",
    "link",
    "body",
    "favoritesCount",
    "twitter_filter_level",
    "twitter_lang",
    "retweetCount",
    "longitude",
    "latitude",
    "country",
    "city",
    "state"
]
pddf = pd.DataFrame()
batchSize = 1000
# OutputSchema:
fieldList = [
    StructField('author',StringType(),True),
    StructField('gender',StringType(),True),
    StructField('sentimentPolarity',StringType(),True),
    StructField('verb',StringType(),True),
    StructField('postedTime',StringType(),True),
    StructField('generatorDisplayName',StringType(),True),
    StructField('link',StringType(),True),
    StructField('body',StringType(),True),
    StructField('favoritesCount',LongType(),True),
    StructField('twitter_filter_level',StringType(),True),
    StructField('twitter_lang',StringType(),True),
    StructField('retweetCount',LongType(),True),
    StructField('longitude',DoubleType(),True),
    StructField('latitude',DoubleType(),True),
    StructField('country',StringType(),True),
    StructField('city',StringType(),True),
    StructField('state',StringType(),True),
]
schema = StructType(fieldList)
if  cxt.isComputeDataModelOnly():
        cxt.setSparkOutputSchema(schema)
else:
        inputData = cxt.getSparkInputData()
        if (listOfAuthors):
            nameList = inputData.select(authors).map(lambda r: r[0]).collect()
        else:
            nameList = [0]
        for name in nameList:
            query = ''
            if (customQuery):
                query = '('+cQuery.replace('::dollar::','$')+')'
            if (dates):
                fromUTC = to_utc(fromDate)
                toUTC = to_utc(toDate)
                if (customQuery):
                    query = query + ' AND (posted:' + fromUTC + ',' + toUTC + ')'
                else:
                    query = '(posted:' + fromUTC + ',' + toUTC + ')'
            if (name != 0):
                if (customQuery | dates):
                    query = query + ' AND (from:' + name + ')'
                else:
                    query = '(from:' + name + ')'
        
            print(query)
            _from = 0
            while _from < maxTweets:
                if maxTweets < _from + batchSize:
                    batchSize = maxTweets - _from
                params = urllib.urlencode({'q': query, 'from': _from, 'size': batchSize})
                print url + params
                tweets = json.loads(unicode( urllib.urlopen(url + params).read(), "utf-8" ))
                total = tweets["search"]["results"]
                _from = _from + batchSize
                author = []
                gender = []
                sentimentPolarity = []
                verb = []
                postedTime = []
                generatorDisplayName = []
                link = []
                body = []
                favoritesCount = []
                twitter_filter_level = []
                twitter_lang = []
                retweetCount = []
                longitude = []
                latitude = []
                country = []
                city = []
                state = []
                for tweet in tweets["tweets"]:
                    try:
                        author.extend([tweet["message"]["actor"]["preferredUsername"]])
                    except:
                        author.extend([np.nan])
                    try:
                        gender.extend([tweet["cde"]["author"]["gender"]])
                    except:
                        gender.extend([np.nan])
                    try:
                        sentimentPolarity.extend([tweet["cde"]["content"]["sentiment"]["polarity"]])
                    except:
                        sentimentPolarity.extend([np.nan])
                    try:
                        verb.extend([tweet["message"]["verb"]])
                    except:
                        verb.extend([np.nan])
                    try:
                        postedTime.extend([tweet["message"]["postedTime"]])
                    except:
                        postedTime.extend([np.nan])
                    try:
                        generatorDisplayName.extend([tweet["message"]["generator"]["displayName"]])
                    except:
                        generatorDisplayName.extend([np.nan])
                    try:
                        link.extend([tweet["message"]["link"]])
                    except:
                        link.extend([np.nan])
                    try:
                        body.extend([tweet["message"]["body"]])
                    except:
                        body.extend([np.nan])
                    try:
                        favoritesCount.extend([tweet["message"]["favoritesCount"]])
                    except:
                        favoritesCount.extend([np.nan])
                    try:
                        twitter_filter_level.extend([tweet["message"]["twitter_filter_level"]])
                    except:
                        twitter_filter_level.extend([np.nan])
                    try:
                        twitter_lang.extend([tweet["message"]["twitter_lang"]])
                    except:
                        twitter_lang.extend([np.nan])
                    try:
                        retweetCount.extend([tweet["message"]["retweetCount"]])
                    except:
                        retweetCount.extend([np.nan])
                    try:
                        longitude.extend([tweet["message"]["gnip"]["profileLocations"][0]["geo"]["coordinates"][0]])
                    except:
                        longitude.extend([np.nan])
                    try:
                        latitude.extend([tweet["message"]["gnip"]["profileLocations"][0]["geo"]["coordinates"][1]])
                    except:
                        latitude.extend([np.nan])
                    try:
                        country.extend([tweet["cde"]["author"]["location"]["country"]])
                    except:
                        country.extend([np.nan])
                    try:
                        city.extend([tweet["cde"]["author"]["location"]["city"]])
                    except:
                        city.extend([np.nan])
                    try:
                        state.extend([tweet["cde"]["author"]["location"]["state"]])
                    except:
                        state.extend([np.nan])
                d = {
                    "author":author,
                    "gender":gender,
                    "sentimentPolarity":sentimentPolarity,
                    "verb":verb,
                    "postedTime":postedTime,
                    "generatorDisplayName":generatorDisplayName,
                    "link":link,
                    "body":body,
                    "favoritesCount":favoritesCount,
                    "twitter_filter_level":twitter_filter_level,
                    "twitter_lang":twitter_lang,
                    "retweetCount":retweetCount,
                    "longitude":longitude,
                    "latitude":latitude,
                    "country":country,
                    "city":city,
                    "state":state
                }
                
                pddf = pddf.append(pd.DataFrame(d), ignore_index=True)
                pddf[['favoritesCount', 'retweetCount']] = pddf[['favoritesCount', 'retweetCount']].astype(int)
                print(pddf.shape)
                if _from > total:
                    break
        df = sqlContext.createDataFrame(pddf[cols], schema)
        cxt.setSparkOutputData(df)