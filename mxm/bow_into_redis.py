import redis
import ast
import re

red = redis.StrictRedis(host='172.31.0.231', port=6379, db=0, password='tamaonsalasanaredikselle')

file0 = open('words_in_order.txt', 'r')
words = []
words_to_index = {}
word_set = set()
hist_count = {} # Dictionary of how many times the word has appeared in queries
count = 0
for line in file0:
    word = re.sub(r'[\r\n]', "", line)
    #if word[0:2] == 'an':
    #    print word
    words.append(word)
    word_set.add(word)
    words_to_index[word] = count
    hist_count[word] = 0
    count += 1
#red.set('words_key', words)
#red.hmset('word_indeces_key', words_to_index)
#red.hmset('hist_freq_key', hist_count)
#red.set('hist_count_key', 0)
red.set('word_set_key', word_set)
file0.close()

#print red.get('words_key')
test = red.get('word_set_key')
test = eval(test)

print len(test)
print 'and' in test
print type(test)
#print red.hgetall('hist_freq_key')
#print red.get('hist_count_key')

quit()
file1 = open('lyrics_ntf_idf_small.txt', 'r')
file2 = open('keys_small.txt', 'w')
keys_small = []
for line in file1:
    dictionary = ast.literal_eval(line)
    for key in dictionary:
        file2.write(str(key)+'\r\n')
        keys_small.append(key)
        #print key
        #red.set(key, dictionary[key])
red.set('get_keys_small', keys_small)
file1.close()
file2.close()
