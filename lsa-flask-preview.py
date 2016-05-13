import os
import sys

import pymysql
import sphinxapi
from flask import Flask
from flask import render_template
from flask import request

from lsa.search.machine import SearchMachine

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

print(SCRIPT_DIR)
app = Flask(__name__)

DB_HOST = 'localhost'
DB_USER = 'user'
DB_PASSWORD = 'user_big_password'
DB_NAME = 'documents'
CHARSET = 'utf8'

SPHINX_INDEX_NAME = "documents"
SPHINX_HOST = '127.0.0.1'
SPHINX_PORT = 9312

try:
    from local_settings import *
except ImportError:
    pass


def init_search_machine():
    return SearchMachine(latent_dimensions=150, index_backend='lsa.keeper.backends.JsonIndexBackend',
                         keep_index_info={'path_to_index_folder': os.path.join(SCRIPT_DIR, 'index_7500')},
                         # db_backend='lsa.db.mysql.MySQLBackend',
                         # db_credentials={'db_name': DB_NAME, 'user': DB_USER, 'password': DB_PASSWORD},
                         # tables_info={
                         #     'news_news': {'fields': ('title', 'text'), 'pk_field_name': 'id',
                         #                   'prefix': 'where id < 75001',
                         #                   'where': ''}
                         # },
                         )


CONNECTION = pymysql.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWORD, db=DB_NAME,
                             charset=CHARSET, cursorclass=pymysql.cursors.Cursor)


def get_new_by_id(id):
    query = "SELECT title, text FROM news_news WHERE id={}".format(id)

    with CONNECTION.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchone()


def search_by_lsa(query):
    results = []
    sm = init_search_machine()
    search_results = sm.search(query, with_distances=True, limit=10)

    if search_results:
        for pk, distance in search_results:
            raw_res = get_new_by_id(pk)
            results.append({"id": pk, "title": raw_res[0], "text": raw_res[1][:500], "distance": distance})
    return results


def search_by_sphinx(query):
    results = []
    client = sphinxapi.SphinxClient()
    client.SetServer(SPHINX_HOST, SPHINX_PORT)
    response = client.Query(query, index=SPHINX_INDEX_NAME)
    matches = response['matches']
    if matches:
        for r in matches:
            pk = r['id']
            raw_res = get_new_by_id(pk)
            results.append({"id": pk, "title": raw_res[0], "text": raw_res[1][:500], "distance": r['weight']})
    return results


@app.route("/news")
def news():
    query = request.args.get('q', '')
    semantic_results = list()
    sphinx_results = list()
    if query:
        semantic_results = search_by_lsa(query)
        sphinx_results = search_by_sphinx(query)
    return render_template('news.html', semantic_results=semantic_results, compare_results=sphinx_results, query=query)


@app.route('/news/<int:post_id>')
def new(post_id):
    return render_template('new.html', new=get_new_by_id(post_id))


if __name__ == "__main__":
    app.debug = True
    app.run()
