from pymongo import MongoClient
import pprint


# sample data document in the database
# {
# 	"_id" : ObjectId("58a36a174f9fec2488728850"),
# 	"rated_movies" : [
# 		{
# 			"rating" : 2,
# 			"genres" : [
# 				"Comedy",
# 				"Romance"
# 			],
# 			"tmdb_id" : 11066,
# 			"title" : "Boomerang (1992)",
# 			"timestamp" : 945544824,
# 			"imdb_id" : 103859,
# 			"id" : 122
# 		},
# 		{
# 			"rating" : 1,
# 			"genres" : [
# 				"Action",
# 				"Sci-Fi",
# 				"Thriller"
# 			],
# 			"tmdb_id" : 9886,
# 			"title" : "Johnny Mnemonic (1995)",
# 			"timestamp" : 945544871,
# 			"imdb_id" : 113481,
# 			"id" : 172
# 		},
# 		{
# 			...
# 		},
# 		{
# 			"rating" : 1,
# 			"genres" : [
# 				"Horror"
# 			],
# 			"tmdb_id" : 10160,
# 			"title" : "Nightmare on Elm Street 5: The Dream Child, A (1989)",
# 			"timestamp" : 945544871,
# 			"imdb_id" : 97981,
# 			"id" : 1972
# 		}
# 	],
# 	"user_id" : "1"
# }


# function to return documents from aggregate
def aggregate(db, pipeline):
    return [doc for doc in db.movie_ratings.aggregate(pipeline)]


################################################################

client = MongoClient('localhost:27017')
db = client.mllatest

# print a record
# pprint.pprint(db.movie_ratings.find_one())

# 1. Which user has rated the highest number of movies?
pipeline1 = [
    # method 1
    {'$project': {'_id': '$user_id', 'count': {'$size': '$rated_movies'}}},
    {'$sort': {'count': -1}},
    {'$limit': 10}
    # # method 2
    # {'$unwind': '$rated_movies'},
    # {'$group': {'_id': '$user_id', 'count': {'$sum': 1}}},
    # {'$sort': {'count': -1}},
    # {'$limit': 10}
]

# 2. Which movie has the highest average rating?
pipeline2 = [
    {'$unwind': '$rated_movies'},
    {'$group': {'_id': {'movie_id': '$rated_movies.id',
                        'movie_title': '$rated_movies.title'},
                'avg_rating': {'$avg': '$rated_movies.rating'}}},
    {'$project': {'_id': 0, 'avg_rating': '$avg_rating',
                  'title': '$_id.movie_title'}},
    # {'$project': {'_id': 0, 'avg_rating': '$avg_rating'}}
    {'$sort': {'avg_rating': -1}},
    {'$limit': 10}
]

# 3. What is the most popular genre of movie?
pipeline3 = [
    {'$unwind': '$rated_movies'},
    {'$unwind': '$rated_movies.genres'},
    {'$group': {'_id': '$rated_movies.genres',
                'view_count': {'$sum': 1},
                'avg_rating': {'$avg': '$rated_movies.rating'}}},
    {'$sort': {'view_count': -1}},
    {'$limit': 10}
]

#######################################################################

# set the pipeline you want to test
pipeline_to_run = pipeline3

# run the aggregate pipeline and get the result
result = aggregate(db, pipeline_to_run)
pprint.pprint(result)
