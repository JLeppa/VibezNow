# VibezNow
- Twittering the Songs for Today

Project for Data Engineering program at Insight Data Science, Silicon Valley, fall 2016

[Insight Data Engineering Homepage](http://www.insightdataengineering.com)

Most people listen to music that suits their current mood or occasion. The purpose of this project is to provide in real-time song suggestions that reflect what people are talking about, what is going on in their life. This is achieved by collecting Twitter tweets over a short period of time and making a bag-of-words vector of the words used in those messages. Lao lyrics of songs from musiXmatch dataset [musiXmatch] (http://labrosa.ee.columbia.edu/millionsong/musixmatch) are preprocessed into the bag-of-words vectors, and the songs that reflect best the tweets are chosen based on cosine similarity between the tweet-vector and the lyrics-vectors. Additionally, the user may choose a theme word, in which case only the tweets including that word are used when calculating the tweet-vector. New top 10 suggestions are provided every ten minutes.  