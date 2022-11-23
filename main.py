import os

from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_restful import Resource, Api, reqparse
import pandas as pd
from rapidfuzz import process, fuzz

app = Flask(__name__)
app.config['CORS_HEADERS'] = 'Content-Type'
CORS(app)

df_all_frame = pd.read_csv('gs://all_frame/all_frame.csv', storage_options={"token": "cloud"})
maths = df_all_frame.iloc[:, 4:]

maths = maths.astype('float')

dist_frame = pd.DataFrame(index=df_all_frame.index, data=df_all_frame[['movieId', 'title']])


@app.route('/recommendations', methods=['POST'])
def recommendations():

    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'search_str' in request_json:
        search_str = request_json['search_str']
    elif request_args and 'search_str' in request_args:
        search_str = request_args['search_str']
    else:
       search_str = ''

    search_results = process.extract(search_str, df_all_frame['title'], scorer=fuzz.WRatio)

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


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))