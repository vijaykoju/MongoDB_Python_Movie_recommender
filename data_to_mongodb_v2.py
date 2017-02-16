# import sys
import csv
import pprint
import argparse
import numpy as np
import pandas as pd
from itertools import islice, groupby
from pymongo import MongoClient

# input argument parsing
pr = argparse.ArgumentParser(description='This scripts prepares data and dumps \
                             it into mongodb.')
pr.add_argument('-d', '--dump', help='Dump data into database', required=False)
pr.add_argument('-s', '--start_line', help='Starting line #', required=True)
pr.add_argument('-e', '--end_line', help='Ending line #', required=True)
args = pr.parse_args()

# get a connection to mongodb if the argument 'dump' is True
if args.dump == 'True':
    client = MongoClient('mongodb://localhost:27017')
    db = client.mllatest
    collection = db.movie_ratings
    # db.drop_collection('movie_ratings')

# read the 'movie.csv' file. Read 'README.txt' for more details.
movies = pd.read_csv('movies.csv')
# read the 'links.csv' file.
links = pd.read_csv('links.csv')

# read 'ratings.csv' file from line # 'start_line' to 'end_line' and store it.
# reading the whole data is not appropriate when the file size is large. This
# method of reading the data from file in chunks is memory efficient and
# scalable.
with open('ratings.csv', 'r') as f:
    reader = csv.reader(f, delimiter=',')
    dta = [row for row in islice(reader, int(args.start_line)-1,
                                 int(args.end_line))]

dta = np.array(dta)  # convert the data to numpy array

# group the distinct users in separate arrays. This helps in easy access of the
# data by users.
grouped = [list(j) for i, j in groupby(dta[:, 0])]

# get the # of lines of data (or # of rated movies) each user.
numlines = [len(row) for row in grouped]

# read the data for each user, put it in json format and dump it in the databse.
indxsum = 0
for i in range(len(numlines)):
    data = dta[indxsum:indxsum+numlines[i], :]  # data of a user
    indxsum += numlines[i]

    u_id = 0
    ddict = {}  # one dictionary for each user
    info_list = []  # list to store dictionaries of data on rated movies
    # loop over all the rated movies by a user and put each of them in a
    # separate dictionary, and store it in a list
    for item in data:
        u_id = item[0]
        info = {}
        info['id'] = int(item[1])
        info['rating'] = float(item[2])
        info['timestamp'] = int(item[3])
        info['title'] = movies[movies['movieId'] == int(item[1])]['title'].values[0]
        info['genres'] = movies[movies['movieId'] == int(item[1])]['genres'].values[0].split('|')
        info['imdb_id'] = links[links['movieId'] == int(item[1])]['imdbId'].values[0]
        info['tmdb_id'] = links[links['movieId'] == int(item[1])]['tmdbId'].values[0]
        info_list.append(info)

    ddict['user_id'] = u_id
    ddict['rated_movies'] = info_list

    # insert the data into mongodb database if the argument 'dump' is True.
    if args.dump == 'True':
        collection.insert(ddict)

pprint.pprint(ddict)

# print the # of documents in the database.
if args.dump == 'True':
    print('{} items in mllatest.movie_ratings.'.format(collection.count()))
