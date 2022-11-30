import os

from flask import Flask
from google.cloud import secretmanager
from flask_cors import CORS
import pandas as pd
from rapidfuzz import process, fuzz
import tmdbsimple as tmdb
from google.cloud import datastore
import Levenshtein




app = Flask(__name__)
app.config['CORS_HEADERS'] = 'Content-Type'
CORS(app)

client = secretmanager.SecretManagerServiceClient()
secret_name = "projects/864523597732/secrets/tmdb_api_key/versions/1"
# Access the secret version.
response = client.access_secret_version(request={"name": secret_name})
tmdb.API_KEY = response.payload.data.decode("UTF-8")


def clean_movie_name(movie):
    index_the = movie.find(", The")
    if index_the != -1:
        movie = "The " + movie[0:index_the]

    index_a = movie.find(", A")
    if index_a != -1:
        movie = "A " + movie[0:index_the]
    
    index_year = movie.find(" (")
    if index_year != -1:
        movie = movie[:index_year]
    
    return movie

@app.route('/recommendations/<media>', methods=['POST'])
def recommendations(media):

    #initalize dataframes from movie info in gcloud
    df_all_frame = pd.read_csv('gs://all_frame/all_frame.csv', storage_options={"token": "cloud"})
    maths = df_all_frame.iloc[:, 4:]

    maths = maths.astype('float')

    dist_frame = pd.DataFrame(index=df_all_frame.index, data=df_all_frame[['movieId', 'title']])


    search_results = process.extract(media, df_all_frame['title'], scorer=fuzz.WRatio)

    base_id = search_results[0][2]

    dist = ((maths - maths.iloc[base_id]) ** 2).sum(axis=1).to_list() 

    dist_frame["Dist_1"] = dist

    recommended_results = dist_frame.sort_values('Dist_1').head(20)
    recommended_movies = recommended_results.loc[:, "title"].reset_index(drop=True).to_dict()

    recommended_movies_info = []
    for movie in recommended_movies.values():
        clean_movie = clean_movie_name(movie)
        recommended_movies_info.append(movie_info(clean_movie))
    return ({
        "search_results": search_results,
        "recommended": recommended_movies_info
    },
    200)

def search_book(name, index, book_meta):
    dist = []

    for t in book_meta['title']:
        dist.append(Levenshtein.distance(t.lower(), name.lower(), weights=(100, 1, 10)))

    df = pd.DataFrame(book_meta[['title']])
    df['dist'] = dist        
    selection = df.sort_values('dist', ascending=True).head(10).reset_index()

    return selection['item_id'][index]

@app.route('/book_recommendations/<book>', methods=['POST'])
def book_recommendations(book):

    # Get data, create usable format
    book_pivot = pd.read_csv('gs://all_frame/book_pivot.csv', storage_options={"token": "cloud"}, index_col=[0])
    book_meta = pd.read_csv('gs://all_frame/book_meta.csv', storage_options={"token": "cloud"}, index_col=[0])

    # Create frame for storing distances
    dist_frame = pd.DataFrame(index=book_meta.index, data=book_meta[['title']])
    base_id = search_book(book, 1, book_meta)

    dist_frame['Dist_1'] = ((book_pivot - book_pivot.loc[base_id]) ** 2).sum(axis=1).to_list()
    recommendations = dist_frame.to_dict()
    return ({
        "recommended": recommendations
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
            "result": cached_movie
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
        "result": movie_entity
    },
    200)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))