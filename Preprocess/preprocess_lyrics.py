import re
import math
import simplejson as json
import redis
from stemming.porter2 import stem

""" Lyrics in musiXmatch data set are processed into bag-of-words vector
using normalized term frequency and inverse document frequency, ntf-idf, with
normalization factor 0.4. Bag-of-words vector contain 4681 words and there
are 230789 lyrics  processed. The results are saved into a Redis database.

Pre-preprocessing was conducted to make following files loaded in this script:
missing_lyrics.txt
lyrics_idf.txt
stopwords.txt

Files mxm_dataset_test.txt, mxm_dataset_train.txt, mxm_reverse_mapping.txt
can be obtained at http://labrosa.ee.columbia.edu/millionsong/musixmatch
"""

# Load a list of track_ids that are missing lyrics or song title and
#  save them into set "missing_tracks"
file_1 = open('missing_lyrics.txt', 'r')
missing_tracks = set()
for line in file_1:
    track = re.sub(r'[\n\r]', "", line)
    missing_tracks.add(track)
file_1.close()

# Load a list of stop words, stem them, and save them into set "stopwords"
file_2 = open('stopwords.txt', 'r')
stopwords = set()
for line in file_2:
    word = re.sub(r'[\n\r]', "", line)
    stopwords.add(stem(word))
file_2.close()

# Open connection to Redis
with open("redis_pw.json.nogit") as fh:
    redis_info = json.loads(fh.read())
red = redis.StrictRedis(host=redis_info["host"], port=redis_info["port"], db=0,
                        password=redis_info["password"])

# Load idf values of lyrics and save them to list in order of bag-of-words vectors
file_3 = open('lyrics_idf.txt', 'r')
idf_all = []
for line in file_3:
    idf_value = re.sub(r'[\n\r]', "", line)
    idf_all.append(float(idf_value))
file_3.close()

# Load reverse mapping for stemmed words, keep ones without characters other than a-z 
#  and store them to dictionary with stemmed word as the key and unstemmed as value
file_4 = open('mxm_reverse_mapping.txt', 'r')
words = {}
for line in file_4:
    both = re.search(r'([^<]+)<SEP>(.+)', line)
    cleaned = re.sub(r'[^a-zA-Z\'\-\s]', "", both.group(1)) # Clean stemmed
    if len(both.group(1)) == len(cleaned) and cleaned not in stopwords:
        words[cleaned] = both.group(2)
file_4.close()

# Save the stemmed-unstemmed reverse mapping to Redis
red.hmset("unstem_key", words)

# Load the list of track_id, song_id, artist_name, song_title and save them to 
#  dictionary with track_id as a key, tuple of artist_name and song_title as value
file_5 = open('unique_tracks.txt', 'r')
track_info_all = {}
song_count = 0
for line in file_5:
    match4 = re.search(r'(.+)<SEP>(.+)<SEP>(.+)<SEP>(.+)', line)
    if match4:
        track_id = match4.group(1)
        song_id = match4.group(2)
        artist_name = match4.group(3)
        song_title = re.sub(r'[\n\r]', "", match4.group(4))
        track_info_all[track_id] = (artist_name, song_title)
    song_count += 1
file_5.close()

# Load the testing set part of musiXmatch data set, calculate ntf-idf values and save
#  them into dictionary with track_id as key and value is tuple in the following format:
#  ([word indeces], [ntf-idf values], vector_norm)
file_6 = open('mxm_dataset_test.txt', 'r')
norm_fact = 0.4 # normalization factor 'a' used when calculating ntf = a+(1-a)*tf/max_tf
ntf_idf_dict = {}
word_order = [] # The 4681 words in the order of the bag-of-words vector
index_from_original_to_new = {}
word_to_index = {} # word as key and new index as value
row_count = 0 # used to keep track of progress
for line in file_6:
    if line[0] == '%':
        word_list = line[1:].split(',')
        word_list[-1] = re.sub(r'[\n\r]', "", word_list[-1])
        # Search for the words that will be discarded
        original_counter = 0 # original index of the word (start from 1)
        new_counter = 0 # new index of the word (start from 0)
        for word in word_list:
            original_counter += 1 # original index of the word
            if word in words:
                word_order.append(word) # list of words in order
                index_from_original_to_new[int(original_counter)] = int(new_counter)
                word_to_index[word] = new_counter
                new_counter += 1
        # Save word_to_index into Redis
        red.hmset('word_indeces_key2', word_to_index)
    elif line[0] == 'T':
        data = line.split(',')
        data[-1] = re.sub(r'[\n\r]', "", data[-1])
        track_id = data[0]
        if track_id not in missing_tracks:
            data.pop(0)
            data.pop(0)
            bow_ind = [] # indeces to words in bag-of-words as sparse vector
            bow_val = [] # values of the sparse vector bag-of-words
            for item in data:
                values = re.search(r'(\d+)\:(\d+)', item)
                orig_id = int(values.group(1))
                raw_count = int(values.group(2))
                orig_word = word_list[orig_id - 1] # Original indexing from 1 NOT 0
                if orig_word in words:
                    new_ind = index_from_original_to_new[orig_id]
                    new_word = word_order[new_ind]
                    bow_ind.append(new_ind)
                    bow_val.append(raw_count)
            # Calculate ntf-idf values
            max_tf = max(bow_val) # number of terms with highest frequency in the lyrics
            bow_ntf_idf = []
            for x in range(len(bow_ind)):
                idf_val = idf_all[bow_ind[x]]
                ntf_val = norm_fact + (1-norm_fact)*bow_val[x]/max_tf
                bow_ntf_idf.append(ntf_val*idf_val)
            # Calculate vector norm
            vec_norm = 0
            for val in bow_ntf_idf:
                vec_norm += val**2
            vec_norm = math.sqrt(vec_norm)
            # Store results into dictionary
            ntf_idf_dict[track_id] = (bow_ind, bow_ntf_idf, vec_norm)

        row_count += 1
        if row_count % 1000 == 0:
            print row_count
file_6.close()

# Load the training set and process lyrics as for testing set above 
file_7 = open('mxm_dataset_train.txt', 'r')
for line in file_7:
    if line[0] == 'T':
        data = line.split(',')
        if data[0] not in missing_tracks:
            data[-1] = re.sub(r'[\n\r]', "", data[-1])
            track_id = data[0]
            data.pop(0)
            data.pop(0)
            bow_ind = [] # indeces to words in bag-of-words as sparse vector
            bow_val = [] # values of the sparse vector bag-of-words
            for item in data:
                values = re.search(r'(\d+)\:(\d+)', item)
                orig_id = int(values.group(1))
                raw_count = int(values.group(2))
                orig_word = word_list[orig_id - 1] # Original indexing from 1 NOT 0
                if orig_word in words:
                    new_ind = index_from_original_to_new[orig_id]
                    new_word = word_order[new_ind]
                    bow_ind.append(new_ind)
                    bow_val.append(raw_count)
            # Calculate ntf-idf values
            max_tf = max(bow_val) # number of terms with highest frequency in the lyrics
            bow_ntf_idf = []
            for x in range(len(bow_ind)):
                idf_val = idf_all[bow_ind[x]]
                ntf_val = norm_fact + (1-norm_fact)*bow_val[x]/max_tf
                bow_ntf_idf.append(ntf_val*idf_val)
            vec_norm = 0
            for val in bow_ntf_idf:
                vec_norm += val**2
            vec_norm = math.sqrt(vec_norm)
            ntf_idf_dict[track_id] = (bow_ind, bow_ntf_idf, vec_norm)

        row_count += 1
        if row_count % 1000 == 0:
            print row_count
file_7.close()

# Save the ntf_idf_dictionary in sparse format into redis
red.hmset("ntf_idf_lyrics_key", ntf_idf_dict)

# Make a dictionary for getting artist name and song title
#  track_id as key, tuple of artist_name and song_title as value
track_info = {}
for track_id in ntf_idf_dict:
    info = track_info_all[track_id]
    track_info[track_id] = info

# Save track_info dictionary to Redis
red.hmset("track_info_key2", track_info)

