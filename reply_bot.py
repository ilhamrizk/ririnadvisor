import tweepy
from tweepy import OAuthHandler
import json
import datetime as dt
import time
import os
import sys
import random
import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
from pickle5 import pickle
from math import sin, cos, sqrt, atan2, radians
from statistics import mean

import warnings
warnings.filterwarnings("ignore")
pd.options.display.html.table_schema=True


'''
In order to use this script you should register a data-mining application
with Twitter.  Good instructions for doing so can be found here:
http://marcobonzanini.com/2015/03/02/mining-twitter-data-with-python-part-1/

After doing this you can copy and paste your unique consumer key,
consumer secret, access token, and access secret into the load_api()
function below.

The main() function can be run by executing the command: 
python twitter_search.py

I used Python 3 and tweepy version 3.5.0.  You will also need the other
packages imported above.
'''

def load_api(hour):
  ''' Function that loads the twitter API after authorizing the user. '''
  KEY = [{'consumer_key' : 'CONSUMER_KEY',
          'consumer_secret' : 'CONSUMER_SECRET',
          'access_token' : 'ACCESS_TOKEN',
          'access_secret' : 'ACCESS_SECRET'},
         {'consumer_key' : 'CONSUMER_KEY',
          'consumer_secret' : 'CONSUMER_SECRET',
          'access_token' : 'ACCESS_TOKEN',
          'access_secret' : 'ACCESS_SECRET'},
         {'consumer_key' : 'CONSUMER_KEY',
          'consumer_secret' : 'CONSUMER_SECRET',
          'access_token' : 'ACCESS_TOKEN',
          'access_secret' : 'ACCESS_SECRET'}]

  key = KEY[hour % 3]
  auth = OAuthHandler(key['consumer_key'], key['consumer_secret'])
  auth.set_access_token(key['access_token'], key['access_secret'])
  # load the twitter API via tweepy
  return tweepy.API(auth, wait_on_rate_limit=True)

def get_tweet_id(api, date='', days_ago=1, query='a'):
  ''' Function that gets the ID of a tweet. This ID can then be
      used as a 'starting point' from which to search. The query is
      required and has been set to a commonly used word by default.
      The variable 'days_ago' has been initialized to the maximum
      amount we are able to search back in time (9).'''

  if date:
    # return an ID from the start of the given day
    td = date + dt.timedelta(days=1)
    tweet_date = '{0}-{1:0>2}-{2:0>2}'.format(td.year, td.month, td.day)
    tweet = api.search(q=query, count=1, until=tweet_date)
  else:
    # return an ID from __ days ago
    td = dt.datetime.now() - dt.timedelta(days=days_ago)
    tweet_date = '{0}-{1:0>2}-{2:0>2}'.format(td.year, td.month, td.day)
    # get list of up to 10 tweets
    tweet = api.search(q=query, count=10, until=tweet_date)
    print('search limit (start/stop):',tweet[0].created_at)
    # return the id of the first tweet in the list
    return tweet[0].id

def tweet_search(api, query, max_tweets, max_id, since_id, geocode):
  ''' Function that takes in a search string 'query', the maximum
      number of tweets 'max_tweets', and the minimum (i.e., starting)
      tweet id. It returns a list of tweepy.models.Status objects. '''

  searched_tweets = []
  while len(searched_tweets) < max_tweets:
    remaining_tweets = max_tweets - len(searched_tweets)
    try:
      new_tweets = api.search(q=query, count=remaining_tweets,
                              since_id=str(since_id),
                              max_id=str(max_id-1))
      print('found',len(new_tweets),'tweets')
      if not new_tweets:
        print('no tweets found')
        break
      searched_tweets.extend(new_tweets)
      max_id = new_tweets[-1].id
    except tweepy.TweepError:
      print('exception raised, waiting 15 minutes')
      print('(until:', dt.datetime.now()+dt.timedelta(minutes=15), ')')
      time.sleep(15*60)
      break # stop the loop
  return searched_tweets, max_id

def write_tweets(tweets, filename):
  ''' Function that appends tweets to a file. '''

  with open(filename, 'a') as f:
    for tweet in tweets:
      json.dump(tweet._json, f)
      f.write('\n')

def get_list_wisata(df_wisata, lokasi_wisata, tempat_wisata, limit_num):
  list_wisata = []
  lokasi_wisata_array = lokasi_wisata.split(',')
  tempat_wisata_array = tempat_wisata.split(',')
  for i in range(len(df_wisata)):
    if len(list_wisata) >= limit_num:
      break
    if df_wisata.loc[i]['kota'] in lokasi_wisata_array and df_wisata.loc[i]['jenis_usaha'] in tempat_wisata_array:
      cek_link = df_wisata.loc[i]['link_to_dec']
      nama_tempat = df_wisata.loc[i]['nama_tempat']
      list_wisata.append([nama_tempat, cek_link])

  return list_wisata
  
def restoreFilePickle(df, filename):
  try:
    df_all = pd.read_pickle(filename)
    df_all.append(df)\
      .reset_index()\
      .drop('index', axis=1)\
      .to_pickle(filename)
  except:
    df.to_pickle(filename)

def main():
  hour = int((dt.datetime.today() - dt.timedelta(hours = 1)).strftime("%H"))
  api = load_api(hour)
  user_filename = '/home/genomexyz/twitter_reply/user.dat'
  verbs = ['butuh', 'mau', 'ingin', 'pengen', 'kangen']
  nouns = ['vacation', 'liburan', 'libur', 'jalan jalan', 'traveling', 'cuti', 'wisata', 'piknik']
  keterangan = ['staycation', 'makan', 'kulineran']
  tempat_cari = ['Homestay / Pondok Wisata,Hotel', 'Restoran / Rumah Makan', 'Restoran / Rumah Makan']

  search_phrases = [f'{verb} {noun}' for verb in verbs for noun in nouns]
  time_limit = 1.5                           # runtime limit in hours
  max_tweets = 100                           # number of tweets per search (will be
                                             # iterated over) - maximum is 100
  min_days_old, max_days_old = 0, 8          # search limits e.g., from 7 to 8
                                             # gives current weekday from last week,
                                             # min_days_old=0 will search from right now

  ID = '0,120,10000km'

  df_recommendation = pd.read_pickle('df_recommendation.pkl')

  #data_chse_w_lonlat = '/home/genomexyz/twitter_reply/data_chse_w_lonlat.pkl'
  #data_tempat_wisata = pd.read_pickle(data_chse_w_lonlat)
  data_chse_w_lonlat = 'data_chse_w_lonlat.csv'
  data_tempat_wisata = pd.read_csv(data_chse_w_lonlat).iloc[:,1:]
  kota_all = data_tempat_wisata['kota'].unique()

  # Scraping Tweets ================================================#
  cnt = -1
  #print('begin')
  #print(search_phrases)
  searched_tweets = []
  for search_phrase in search_phrases:
    since_id = 0
    remaining_tweets = max_tweets - len(searched_tweets)
    try:
      #print(search_phrase)
      new_tweets = api.search(q='"{}"'.format(search_phrase),
                              since_id=str(since_id),
                              max_id=str(-1))
      #print('found',len(new_tweets),'tweets')
      #if not new_tweets:
      #  print('no tweets found')
      searched_tweets.extend(new_tweets)
    except tweepy.TweepError:
        break # stop the loop

    list_searched_tweets = [dict_searched_tweets._json for dict_searched_tweets in searched_tweets]
    df_searched_tweets = pd.DataFrame(list_searched_tweets)

  list_userkey = list(df_searched_tweets.user[0].keys())

  def tryGetUser(dict_user, key):
    try:
      return dict_user[key]
    except:
      return None
    
  for userkey in list_userkey:
    df_searched_tweets['user_'+userkey] = df_searched_tweets.user.apply(lambda x : tryGetUser(x, userkey))
  df_searched_tweets['created_at'] = pd.to_datetime(df_searched_tweets.created_at, 
                                                    format = '%a %b %d %H:%M:%S +0000 %Y')
  df_searched_tweets['hours'] = df_searched_tweets['created_at'].apply(lambda x: x.strftime("%Y%m%d%H"))
  date_now = (dt.datetime.today() - dt.timedelta(hours = 1)).strftime("%Y%m%d%H")

  df_searched_tweets = df_searched_tweets[(df_searched_tweets.hours == date_now)]
  restoreFilePickle(df_searched_tweets, 'df_searched_tweets.pkl')
  print('scrapped & filtered ', df_searched_tweets.shape[0], ' tweets')
  df_filter = df_searched_tweets[(df_searched_tweets['in_reply_to_screen_name'].isnull())
                                 & (df_searched_tweets['retweeted_status'].isnull())
                                 & (df_searched_tweets['lang'] == 'in')]
  df_filter['rn'] = df_filter.sort_values('created_at')\
    .groupby(['user_screen_name', 'user_id_str'])\
    .cumcount() +1
  df_filter = df_filter[df_filter.rn == 1][['created_at',
                                            'id_str',
                                            'place',
                                            'text',
                                            'user_id_str',
                                            'user_name',
                                            'user_screen_name',
                                            'user_location',
                                            'user_followers_count']]
  try:
    list_done = df_recommendation[df_recommendation['status'] == 'success']['user_id_str'].tolist()
  except:
    list_done = []
  df_filter['done'] = df_filter.user_id_str.apply(lambda x: 1 if x in list_done else 0)

  df_filter = df_filter[df_filter.done == 0]\
    .reset_index()\
    .sort_values('user_followers_count', ascending = False)\
    .drop(['index', 'done', 'user_followers_count'], axis=1)

  ## recommending relevan location =================================#
  def getDistance(lat1, lat2, lon1, lon2):
    R = 6373.0
    lat1_rad = radians(lat1)
    lon1_rad = radians(lon1)
    lat2_rad = radians(lat2)
    lon2_rad = radians(lon2)
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

  print('recommending ', df_filter.shape[0], ' accounts')
  recommend_place_id = []
  for ll in range(len(df_filter)):
    #print(searched_tweets[ll].id)

    #ll = 20

    #difilter di atas
    ##if already get reply, pass...
    #if user_target in all_user_get_reply:
    #    print('already get reply, pass...')
    #    recommend_place_id.append(None)
    #    recommendation_status.append('replied')
    #    continue

    if df_filter.iloc[ll, 2]:
      lon_tweet = mean(lonlat[0] for lonlat in df_filter.iloc[ll, 2]['bounding_box']['coordinates'][0])
      lat_tweet = mean(lonlat[1] for lonlat in df_filter.iloc[ll, 2]['bounding_box']['coordinates'][0])
      distances = np.zeros(len(data_tempat_wisata))
      for idx in range(len(data_tempat_wisata)):
        lon_place = data_tempat_wisata.loc[idx, 'lon']
        lat_place = data_tempat_wisata.loc[idx, 'lat']
        distances[idx] = getDistance(lat_tweet, lat_place, lon_tweet, lon_place)
      idx_recommended = np.argmin(distances)
      recommend_place_id.append(idx_recommended)
    else:
      #print(df_filter.iloc[ll,3], df_filter.iloc[ll, -1])
      teks_tweet = df_filter.iloc[ll,3] + ' ' + str(df_filter.iloc[ll, -1])
      #user_target = df_filter[ll].user.screen_name
      #print(user_target)
      score_wisata = np.zeros(len(data_tempat_wisata))
      for i in range(len(data_tempat_wisata)):
        cek_word = data_tempat_wisata.loc[i, 'kota'] + ' ' + data_tempat_wisata.loc[i, 'provinsi']
        #Ratio = fuzz.ratio(cek_word.lower(),teks_tweet.lower())
        Partial_Ratio = fuzz.partial_ratio(cek_word.lower(),teks_tweet.lower())
        score_wisata[i] = Partial_Ratio * 2
        #index_highest_score = np.argmax(score_wisata)
        #kota_tepat = kota_all[index_highest_score]
        #for i in range(len(keterangan)):

        Ratio = fuzz.ratio(data_tempat_wisata.loc[i, 'jenis_usaha'].lower(),
                           teks_tweet.lower())
        Partial_Ratio = fuzz.partial_ratio(data_tempat_wisata.loc[i, 'jenis_usaha'].lower(),
                                           teks_tweet.lower())
        score_wisata[i] += (Ratio+Partial_Ratio)/4

        token_set = fuzz.token_set_ratio(data_tempat_wisata.loc[i, 'nama_tempat'].lower(),
                                         teks_tweet.lower())
        score_wisata[i] += token_set

      index_highest_score = np.argmax(score_wisata)
      #data_tempat_wisata.iloc[index_highest_score, :]
      recommend_place_id.append(index_highest_score)

  df_filter['recommend_place'] = pd.Series(recommend_place_id)
  df_recommendation = df_filter.set_index('recommend_place').join(data_tempat_wisata).reset_index()
  #df_recommendation['index_tempat'] = df_recommendation['index']
  #df_recommendation = df_recommendation.drop('index', axis=1)

  #cari semua rekomendasi
  #for i in range(len(tempat_cari_tepat)):
  #    if i == 0:
  #        rekomendasi = data_tempat_wisata[(data_tempat_wisata['kota'] == kota_tepat) & (data_tempat_wisata['jenis_usaha'] == tempat_cari_tepat[i])]
  #    else:
  #        rekomendasi_new = data_tempat_wisata[(data_tempat_wisata['kota'] == kota_tepat) & (data_tempat_wisata['jenis_usaha'] == tempat_cari_tepat[i])]
  #        rekomendasi = pd.concat([rekomendasi, rekomendasi_new], ignore_index=True)

  #print(kota_tepat, tempat_cari_tepat)
  #print(rekomendasi, len(rekomendasi))

  #pilih rekomendasi random sebanyak limit_rekomen = 4
  #limit_rekomen = 4
  #rekomendasi_show = []
  #rekomendasi_show_index = []
  #index_rekomendasi = np.array(rekomendasi.index.values.tolist())
  #for i in range(len(rekomendasi)):
  #    if len(rekomendasi_show) >= limit_rekomen:
  #        break
  #    random_choice = random.choice(index_rekomendasi)
  #    if random_choice not in rekomendasi_show_index:
  #        print(random_choice, rekomendasi_show_index)
  #        rekomendasi_show.append([rekomendasi.loc[random_choice]['nama_tempat'], rekomendasi.loc[random_choice]['place_id']])
  #        rekomendasi_show_index.append(random_choice)

  #no recomendation, continue
  #if len(rekomendasi_show) == 0:
  #    continue

  # Reply tweets ===================================================#
  limit = 3
  reply_status = []
  replied_at = []
  id_batch = []
  for i in range(df_recommendation.shape[0]):
    id_batch.append(date_now)
    id_tweet = df_recommendation.iloc[i, 2]
    username = df_recommendation.iloc[i, 7]
    title = df_recommendation.iloc[i, 12]
    kota = df_recommendation.iloc[i, 16].lower()
    place_id = df_recommendation.iloc[i, 28]
    url = 'https://www.google.com/maps/place/?q=place_id:'+place_id
    string_reply = """Hai kak @{0}, mungkin rekomendasi lokasi wisata ini bagus buat kamu. {1} yang ada di {2}. \nBantu kami agar lebih baik bit.ly/banturirin. trimakasi. \n{3} """.format(username, title, kota, url)
    #print(len(string_reply))

    #'Hai kak @%s,\n'%(df_recommen)
    #string_reply += 'mungkin rekomendasi lokasi wisata ini bagus buat kk:\n'
    #for i in range(len(rekomendasi_show)):
    #    string_reply += '%s. %s, https://www.google.com/maps/place/?q=place_id:%s\n'%(i+1, rekomendasi_show[i][0], rekomendasi_show[i][1])
    #print(string_reply)
    if (limit > 0) or (df_recommendation.iloc[i, 2]):
      try:
        api.update_status(status = string_reply, 
                          in_reply_to_status_id = id_tweet, 
                          auto_populate_reply_metadata=True)
        reply_status.append('Success')
        replied_at.append(dt.datetime.today())
        limit -= 1
        #print('membalas tweet dengan id %s, isinya: %s'%(searched_tweets[ll].id_str, teks_tweet))
      except tweepy.TweepError as e:
        #print('tweet terlalu panjang sehingga tidak bisa dikirimkan, continue...')
        print(e)
        reply_status.append('Failed')
        replied_at.append(None)
        continue
    else:
      reply_status.append('limit')
      replied_at.append(None)

  df_recommendation['status'] = pd.Series(reply_status)
  df_recommendation['replied_at'] = pd.Series(replied_at)
  df_recommendation['id_batch'] = pd.Series(id_batch)
  restoreFilePickle(df_recommendation, 'df_recommendation.pkl')
  print('replied ', 
        df_recommendation[df_recommendation.status == 'Success'].shape[0],
        ' tweets')
  #include user to list
  #all_user_get_reply.append(user_target)

  #SAVE ALL USER THAT ALREADY GET REPLY IN THIS SESSION
  #string_save = ','.join(all_user_get_reply)
  #user_file_open = open(user_filename, 'w')
  #user_file = user_file_open.write(string_save)
  #user_file_open.close()

  #print(search_phrases)

   

if __name__ == "__main__":
    main()
