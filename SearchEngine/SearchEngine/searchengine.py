#!/usr/bin/python3

from flask import Flask, render_template, request

import search

application = app = Flask(__name__)
app.debug = True
@app.route('/search',methods=["GET"])
def dosearch():
    query = request.args['query']
    qtype = request.args['query_type']
    page = request.args.get('page',1,type=int)
    if request.args.get('next'):
        page += 1
    elif request.args.get('prev'):
        page -= 1

    num_lines, search_results = search.search(query, qtype, page)
    num_lines = request.args.get('num_lines',num_lines,type=int)
    return render_template('results.html',
            query=query,
            query_type=qtype,
            page=page,
            num_lines=num_lines,
            results=len(search_results),
            search_results=search_results)

@app.route("/", methods=["GET"])
def index():
    if request.method == "GET":
        pass

    return render_template('index.html')

if __name__ == "__main__":
    app.run(host='0.0.0.0')


