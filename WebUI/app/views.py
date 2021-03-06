from app import app
from flask import render_template, request, jsonify
import redis
import simplejson
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

# Set up a connection to Redis
with open("redis_pw.json.nogit") as fh:
    redis_info = simplejson.loads(fh.read())
red = redis.StrictRedis(host=redis_info["host"], port=redis_info["port"], db=0,
                        password=redis_info["password"])

@app.route('/')

@app.route('/index')
def index():
    return render_template('index.html', title = 'This is VibezNow!')

@app.route('/theme', methods=['POST'])
def theme():
    # User  chooses "theme"
    theme = request.form["theme"]

    # Get song suggestion for the given "theme word", and trending words
    if theme == "Food":
        songs = red.get('top_songs_w1_key')
        words = red.get('top_words_w1_key')
        theme = "Theme: "+theme
    elif theme == "Love":
        songs = red.get('top_songs_w2_key')
        words = red.get('top_words_w2_key')
        theme = "Theme: "+theme
    elif theme == "Shop":
        songs = red.get('top_songs_w3_key')
        words = red.get('top_words_w3_key')
        theme = "Theme: "+theme
    elif theme == "Sport":
        songs = red.get('top_songs_w4_key')
        words = red.get('top_words_w4_key')
        theme = "Theme: "+theme
    else:
        songs = red.get('top_songs_key')
        words = red.get('top_words_key')
        theme = "No Theme"
    songs = eval(songs)
    words = eval(words)
    song = []
    for item in songs:
        song.append(eval(item[0]))
    artist_list = []
    song_list = []
    for item in song:
        artist_list.append(item[0])
        song_list.append(item[1])
    word_list = []
    counter = 0
    for item in words:
        word_list.append(item[0])
        counter += 1
        if counter > 9:
            break
    
    # Return the top words and songs
    print song_list
    print artist_list
    jsonresponse = {"artist": artist_list, "song": song_list,  "words": word_list, "theme_word": theme}
    return render_template("user_op.html", output=[jsonresponse])
