# **VibezNow**

## **Twittering the Songs for Today**

Project for Data Engineering program at Insight Data Science, Silicon Valley, fall 2016

[Insight Data Engineering Homepage](http://www.insightdataengineering.com)

Project Website: [VibezNow.top] (http://www.vibeznow.top)

Project [Presentation] (http://www.bit.do/VibezNow) 

Most people listen to music that suits their current mood or occasion. The purpose of this project is to provide in real-time song suggestions that reflect what people are talking about, what is going on in their life. This is achieved by collecting Twitter tweets over a short period of time and making a bag-of-words vector of the words used in those messages. The lyrics of songs from [musiXmatch] (http://labrosa.ee.columbia.edu/millionsong/musixmatch) dataset are preprocessed into the bag-of-words vectors, and the songs that reflect best the tweets are chosen based on cosine similarity between the tweet-vector and the lyrics-vectors. Additionally, the user may choose a theme word, in which case only the tweets including that word are used when calculating the tweet-vector. New top 10 suggestions are provided every three minutes.

## **Preprocessing lyrics**

The lyrics in the musiXmatch dataset were obtained as two text files, mxm_dataset_train.txt and mxm_data_set_train.txt, which contained the 5000 most common words in the whole dataset, and the lyrics of 237,662 songs in the sparse bag-of-words vector format of raw counts of words matching the list of 5000 words. The conducted preprocessing of those lyrics included the following steps:

1. Stopwords, as provided in the stopwords.txt, numbers and words with non-UTF-8 characters were removed from the list of words resulting to a new list of 4681 words. The bag-of-words vectors were mapped to this new list of words.

2. Songs with no lyrics or missing either name of the artist or title of the song in the unique_tracks.txt file were omitted, resulting to a total of 237,642 songs.

3. For each word, the number of lyrics that the word appeared in was calculated, as was the maximum count of each word within a single lyrics. These values were used to calculate normalized term frequencies, tf-idf, that account for different length of the lyrics and putting more weight on words that appear less frequently in the lyrics. 

4. Second norm was calculated for each vector after the normalization.

5. Lyrics were stored into a Redis database to be accessed during the stream processing of Twitter tweets.

## **Data ingestion**

*Coming soon...*

## **Stream processing**

*Coming soon...*

