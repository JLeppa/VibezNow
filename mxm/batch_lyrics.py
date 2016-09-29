import re
import math
import simplejson as json
import redis
from stemming.porter2 import stem

# Open connection to Redis
with open("redis_pw.json.nogit") as fh:
    redis_pw = simplejson.loads(fh.read())
red_con = redis.StrictRedis(host='172.31.0.231', port=6379, db=0,
                        password=redis_pw["password"])

# Save a test dictionary to redis
#test_dict = {"tk": [1.23, 3.25, 8.12456], "tk2": [6.1324, 234.1, 34]}
#red_con.hmset("test_dict_key", test_dict)
#test_get = red_con.hgetall("test_dict_key")
#print test_get
#quit()

# These two tracks are missing a song title: 
#m1 = 'TRWEQAA128EF36407B'
#m2 = 'TRJOMUD12903CED473'
#missing_tracks = [m1, m2]


# Load the idf values calculated later in the code
file_m1 = open('idf.txt', 'r')
idf_all = []
for line in file_m1:
    idf_value = re.sub(r'[\n\r]', "", line)
    idf_all.append(float(idf_value))

file_m1.close()
#print len(idf_all)
#quit()

# Load a list of track_ids that are missing lyrics or song title
file_0 = open('missing_lyrics.txt', 'r')
missing_tracks = set()
for line in file_0:
    track = re.sub(r'[\n\r]', "", line)
    missing_tracks.add(track)
#print missing_tracks
file_0.close()
#quit()

# Load a list of stop words
file_1 = open('stopwords.txt', 'r')
# Make a set of stemmed stop words
stopwords = set()
for line in file_1:
    word = re.sub(r'[\n\r]', "", line)
    stopwords.add(stem(word))
file_1.close()


# Load the reverse mapping for stemmed words
file_2 = open('mxm_reverse_mapping.txt', 'r') 
# Read words and only keep ones without characters other than a-z and  
words = {}  # dictionary with stemmed word as the key and unstemmed as value
word_set = set()
for line in file_2:
    both = re.search(r'([^<]+)<SEP>(.+)', line)
    cleaned = re.sub(r'[^a-zA-Z\'\-\s]', "", both.group(1)) # Clean stemmed
    if len(both.group(1)) == len(cleaned) and cleaned not in stopwords:
        word_set.add(cleaned)
        words[cleaned] = both.group(2)
file_2.close()
unstem_to_redis = 0
if unstem_to_redis:
    red_con = redis.StrictRedis(host='172.31.0.231', port=6379, db=0, password='tamaonsalasanaredikselle')
    red_con.hmset("unstem_key", words)
    test_unstem = red_con.hgetall("unstem_key")
    print test_unstem
    print len(test_unstem)
    quit()

#print len(word_set)


# Load the list of track_id, song_id, artist_name, song_title
file_3 = open('unique_tracks.txt', 'r')
track_info_all = {} # track_id as a key, tuple of artist_name and song_title as value
song_count = 0
for line in file_3:
    match4 = re.search(r'(.+)<SEP>(.+)<SEP>(.+)<SEP>(.+)', line)
    if match4:
        track_id = match4.group(1)
        song_id = match4.group(2)
        artist_name = match4.group(3)
        song_title = re.sub(r'[\n\r]', "", match4.group(4))
        track_info_all[track_id] = (artist_name, song_title)
    song_count += 1
    #if song_count > 1:
    #    print track_info_all
    #    break

#print song_count
#print len(track_info_all)
file_3.close()


# Load the testing set
file_4 = open('mxm_dataset_test.txt', 'r')
#file_9 = open('lyrics_ntf_idf.txt', 'w')
norm_fact = 0.4 # normalization factor 'a' used when calculating ntf = a+(1-a)*tf/max_tf
track_dict = {} # track_id as key and mxm_track_id as value
ntf_idf_dict = {} # track_id as key, value is tuple with list of word indeces, their ntf-idf values and vector norm
track_id_list = [] # list of track_id's
word_order = [] # The 4681 words in the order of the b-o-w
index_from_original_to_new = {}
row_count = 0
dummy = []
for a in range(len(word_set)):
    dummy.append(0)
contain_word_count = list(dummy) # count for each word in how many lyrics it appears
for line in file_4:
    if line[0] == '%':
        word_list = line[1:].split(',')
        word_list[-1] = re.sub(r'[\n\r]', "", word_list[-1])
        # Search for the words that will be discarded
        rem_counter = 0 # number of removed words
        original_counter = 0 # original index of the word (start from 1)
        new_counter = 0 # new index of the word (start from 0)
        rem_index_set = set() # original indeces of removed words
        for word in word_list:
            original_counter += 1 # original index of the word
            if word in word_set:
                word_order.append(word) # new set of words in order
                index_from_original_to_new[int(original_counter)] = int(new_counter)
                new_counter += 1
            
            if word not in word_set:
                rem_counter += 1
                rem_index_set.add(original_counter)
        #file_4.close()
        #file_9.close()
        #file_10 = open('words_in_order.txt', 'w')
        #for word in word_order:
        #    file_10.write(str(word)+'\r\n')
        #file_10.close()
        #quit()

    elif line[0] == 'T':
        #print line
        data = line.split(',')
        data[-1] = re.sub(r'[\n\r]', "", data[-1])
        if data[0] not in missing_tracks:
            #print data[0]
            track_dict[data[0]] = data[1]
            track_id_list.append(data[0])
            data.pop(0)
            data.pop(0)
            #print "----------- new---------"
            bow = list(dummy)
            bow_ind = [] # indeces to words in bag-of-words as sparse vector
            bow_val = [] # values of the sparse vector bag-of-words
            for item in data:
                values = re.search(r'(\d+)\:(\d+)', item)
                orig_id = int(values.group(1))
                raw_count = int(values.group(2))
                orig_word = word_list[orig_id - 1] # Original indexing from 1 NOT 0
                if orig_word in word_set:
                    new_ind = index_from_original_to_new[orig_id]
                    new_word = word_order[new_ind]
                    bow[new_ind] = raw_count
                    bow_ind.append(new_ind)
                    bow_val.append(raw_count)
                    contain_word_count[new_ind] += 1
                    #print(new_word)
                    #print(raw_count)
            # Normalised term frequencies, ntf
            max_tf = max(bow) # number of terms with highest frequency in the lyrics
            if max_tf == 0:
                print track_id_list[-1]
                #quit()
            else:
                ntf = [norm_fact+(1-norm_fact)*x/max_tf for x in bow]
                ntf_idf = [a*b for a,b in zip(ntf,idf_all)]
                lyric_vector = {track_id_list[-1]: ntf_idf}
                bow_ntf_idf = []
                for x in range(len(bow_ind)):
                    idf_val = idf_all[bow_ind[x]]
                    ntf_val = norm_fact + (1-norm_fact)*bow_val[x]/max_tf
                    bow_ntf_idf.append(ntf_val*idf_val)
                    #print(word_order[bow_ind[x]])
                vec_norm = 0
                for val in bow_ntf_idf:
                    vec_norm += val**2
                vec_norm = math.sqrt(vec_norm)
                #vector_norm = lambda bow_ntf_idf: math.sqrt(sum(val**2 for val in bow_ntf_idf))
                ntf_idf_dict[track_id_list[-1]] = (bow_ind, bow_ntf_idf, vec_norm)

                #print(ntf_idf_dict)
                #file_4.close()
                #quit()
                
                #file_9.write(str(lyric_vector)+'\r\n')
                #if row_count > 30:
                #    file_9.close()
                #    file_4.close()
                #    quit()


                #print ntf_idf
                #print track_id_list[-1]
                #ntf_dict[track_id_list[-1]] = ntf
                #print ntf_dict
                #quit()
                
        #else:
        #    print data[0]
        
        #print len(bow), max_tf
        #print len(ntf), max(ntf), min(ntf)
        #print len(contain_word_count), sum(contain_word_count)
        #if row_count == 10:
        #    print contain_word_count
        #    quit()

        #     print orig_word, raw_count
        #print "============= split ================="
        #for xx in range(len(bow)):
        #    if bow[xx] != 0:
        #        print word_order[xx], bow[xx]
        #print len(bow)

        row_count += 1
        if row_count % 1000 == 0:
            print row_count
        #print row_count
        #if row_count > 1:
        #    file_3.close()
        #    break
        #break
file_4.close()


# Load the testing set
file_5 = open('mxm_dataset_train.txt', 'r')
for line in file_5:
    if line[0] == 'T':
        #print line
        data = line.split(',')
        if data[0] not in missing_tracks:
            data[-1] = re.sub(r'[\n\r]', "", data[-1])
            track_dict[data[0]] = data[1]
            track_id_list.append(data[0])
            data.pop(0)
            data.pop(0)
        
            bow = list(dummy)
            bow_ind = [] # indeces to words in bag-of-words as sparse vector
            bow_val = [] # values of the sparse vector bag-of-words
            for item in data:
                values = re.search(r'(\d+)\:(\d+)', item)
                orig_id = int(values.group(1))
                raw_count = int(values.group(2))
                orig_word = word_list[orig_id - 1] # Original indexing from 1 NOT 0
                if orig_word in word_set:
                    new_ind = index_from_original_to_new[orig_id]
                    new_word = word_order[new_ind]
                    bow[new_ind] = raw_count
                    bow_ind.append(new_ind)
                    bow_val.append(raw_count)
                    contain_word_count[new_ind] += 1
            # Normalised term frequencies, ntf
            max_tf = max(bow) # number of terms with highest frequency in the lyrics
            if max_tf == 0:
                print track_id_list[-1]
            else:
                ntf = [norm_fact+(1-norm_fact)*x/max_tf for x in bow]
                ntf_idf = [a*b for a,b in zip(ntf,idf_all)]
                lyric_vector = {track_id_list[-1]: ntf_idf}
                bow_ntf_idf = []
                for x in range(len(bow_ind)):
                    idf_val = idf_all[bow_ind[x]]
                    ntf_val = norm_fact + (1-norm_fact)*bow_val[x]/max_tf
                    bow_ntf_idf.append(ntf_val*idf_val)
                    #print(word_order[bow_ind[x]])
                vec_norm = 0
                for val in bow_ntf_idf:
                    vec_norm += val**2
                vec_norm = math.sqrt(vec_norm)
                #vector_norm = lambda bow_ntf_idf: math.sqrt(sum(val**2 for val in bow_ntf_idf))
                ntf_idf_dict[track_id_list[-1]] = (bow_ind, bow_ntf_idf, vec_norm)
                #file_9.write(str(lyric_vector)+'\r\n')
                #ntf_dict[track_id_list[-1]] = ntf
        #else:
        #    print data[0]

        #     print orig_word, raw_count
        #print "============= split ================="
        #for xx in range(len(bow)):
        #    if bow[xx] != 0:
        #        print word_order[xx], bow[xx]
        #print len(bow)

        row_count += 1
        if row_count % 1000 == 0:
            print row_count
        #file_4.close()
        #break
file_5.close()
#file_9.close()

# Save the ntf_idf_dictionary in sparse format into redis
red_con.hmset("ntf_idf_lyrics_key", ntf_idf_dict)
test_get = red_con.hgetall("ntf_idf_lyrics_key")
print(test_get)
quit()

# Make a dictionary for getting artist name and song title
track_count = 0
track_info = {} # track_id as key, tuple of artist_name and song_title as value
missing_tracks = []
for track_id in track_id_list:
    if track_id in track_info_all:
        info = track_info_all[track_id]
        track_info[track_id] = info
        track_count += 1
    else:
        #print track_id
        missing_tracks.append(track_id)
    #if track_count > 3:
    #    print track_info
    #    break

#print missing_tracks

# Total number of cleaned tracks (237642)
tot_cleaned = len(track_dict)

#print contain_word_count

# Calculate the idf = log(N/(1+nt)) and save it to a file
#idf = [math.log(tot_cleaned/(1+x)) for x in contain_word_count]
#print idf
#file_6 = open('idf.txt', 'w')
#for value in idf:
#    line = str(value)+'\n\r'
#    file_6.write(line)
#file_6.close()


# Calculate the ntf-idf values and put them in dictionary
#ntf_idf_lyrics = {}
#for key in track_dict:
#    ntf = ntf_dict[key]
#    ntf_idf = [a*b for a,b in zip(ntf,idf)]
#    ntf_idf_lyrics[key] = ntf_idf


# Save track_info dictionary to Redis
red_con.hmset("track_info_key", track_info)
test_track_info = red_con.hgetall("track_info_key")
print len(test_track_info)


