import os
import re

from flask import Flask, request, jsonify
from google.cloud import secretmanager
from flask_cors import CORS
from flask_restful import Resource, Api, reqparse
import pandas as pd
from rapidfuzz import process, fuzz
import tmdbsimple as tmdb
from google.cloud import datastore



app = Flask(__name__)
app.config['CORS_HEADERS'] = 'Content-Type'
CORS(app)

client = secretmanager.SecretManagerServiceClient()
secret_name = "projects/864523597732/secrets/tmdb_api_key/versions/1"
# Access the secret version.
response = client.access_secret_version(request={"name": secret_name})
tmdb.API_KEY = response.payload.data.decode("UTF-8")


#initalize dataframes from movie info in gcloud
df_all_frame = pd.read_csv('gs://all_frame/all_frame.csv', storage_options={"token": "cloud"})
maths = df_all_frame.iloc[:, 4:]

maths = maths.astype('float')

dist_frame = pd.DataFrame(index=df_all_frame.index, data=df_all_frame[['movieId', 'title']])


@app.route('/recommendations/<media>', methods=['POST'])
def recommendations(media):


    search_results = process.extract(media, df_all_frame['title'], scorer=fuzz.WRatio)

    base_id = search_results[0][2]

    dist = ((maths - maths.iloc[base_id]) ** 2).sum(axis=1).to_list() 

    dist_frame["Dist_1"] = dist

    recommended_results = dist_frame.sort_values('Dist_1').head(20)
    recommended_movies = recommended_results.loc[:, "title"].reset_index(drop=True).to_json()

    return ({
        "search_results": search_results,
        "recommended": recommended_movies
    },
    200)

# change from movie to media later if we want to generalize
@app.route('/movie/<movie>', methods=['GET'])
def movie_info(movie):
    datastore_client = datastore.Client()
    # look if movie already in cloud datastore
    movie_key = datastore_client.key("imdb_movie", movie)
    cached_movie = datastore_client.get(movie_key)
    if cached_movie:
        #if movie already saved
        return ({
            "result": cached_movie,
            "returned_from_cache" : True
        },
        200)

    search = tmdb.Search()
    response = search.movie(query=movie)
    if not response["results"]:
        # handle no response case
        return {"results: ": []}

    first_result = response["results"][0]
    movie_entity = datastore.Entity(key=movie_key)
    movie_entity["data"] = first_result

    # save movie in datastore
    datastore_client.put(movie_entity)

    return ({
        "result": movie_entity,
        "returned_from_cache" : False
    },
    200)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))