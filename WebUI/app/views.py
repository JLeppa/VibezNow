from app import app
from flask import render_template, request, jsonify
import redis
import simplejson

# Set up a connection to Redis
with open("redis_pw.json.nogit") as fh:
    redis_pw = simplejson.loads(fh.read())
red = redis.StrictRedis(host='172.31.0.231', port=6379, db=0,
                        password=redis_pw["password"])

@app.route('/')

@app.route('/index')
def index():
    return render_template('index.html', title = 'This is VibezNow!')

@app.route('/logged', methods=['POST'])
def logged():
    # User name entered is user_id
    user_id = request.form["user_id"]

    # Get song suggestion and trending words
    songs = red.get('top_songs_key')
    songs = eval(songs)
    words = red.get('top_words_key')
    words = eval(words)
    
    # Pick the first song artist and title, and the list of words
    song = eval(songs[0][0])
    artist = song[0]
    song = song[1]
    word_list = []
    for item in words:
        word_list.append(item[0])

    # Return the top words and songs
    jsonresponse = {"artist": artist, "song": song,  "words": word_list}
    #print jsonresponse
    #print type(jsonresponse)
    #jsonresponse = jsonify(jsonresponse)
    #print jsonresponse
    #print simplejson.dumps(jsonresponse)
    #print "ok"
    return render_template("user_op.html", output=[jsonresponse])

 
"""

@app.route('/index')
def index():
    #return "Hello, World!"
    user = { 'nickname': 'Miguel' } # fake user
    mylist = [1, 2, 3, 4]
    return render_template("index.html", title = 'Home', user = user, mylist = mylist)

@app.route('/email')
def email():
    return render_template("email.html")

@app.route("/email", methods=['POST'])
def email_post():
    emailid = request.form["emailid"]
    date = request.form["date"]

    # email entered is in emailid and date selected in dropdown is in date variable, respectively

    jsonresponse = [{"fname": "x.fname", "lname": "x.lname", "id": "x.id", "message": "x.message", "time": "x.time"}]
    return render_template("emailop.html", output=jsonresponse)

"""
