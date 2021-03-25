from flask import Flask, render_template, request
from pymongo import MongoClient

app = Flask(__name__)
client = MongoClient(
    'mongodb+srv://CS242jzhu:CS242jzhu0305@cluster0.2gehc.mongodb.net/test?retryWrites=true&w=majority')
db = client['images']
collection = db['test_collection']


@app.route('/', methods=['GET', 'POST'])
def index():
    if len(request.form) > 0:
        img_data = list(collection.find({"title": {"$regex": ".*" + request.form["query"] + ".*"}}).limit(10))
        for i in img_data:
            print(i['title'])
        return render_template('index.html', query=request.form["query"], results=img_data)
    return render_template('index.html', name="no results found")


app.run('127.0.0.1', debug=True)
