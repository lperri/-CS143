#!/usr/bin/python3

from flask import Flask, render_template, request

import search

application = app = Flask(__name__)
app.debug = True
@app.route('/search', methods=["GET"])
def dosearch():
    query = request.args['query']
    qtype = request.args['query_type']
    #current_page = request.args['current_page']
    search_results = search.search(query, qtype)
    return render_template('results.html',
            query=query,
            results=len(search_results),
            search_results=search_results)

@app.route("/", methods=["GET"])
def index():
    if request.method == "GET":
        pass

    return render_template('index.html')

if __name__ == "__main__":
    app.run(host='0.0.0.0')


