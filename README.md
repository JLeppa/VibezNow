# **VibezNow**

## **Twittering the Songs for Today**

Project for Data Engineering program at Insight Data Science, Silicon Valley, fall 2016

[Insight Data Engineering Homepage](http://www.insightdataengineering.com)

Project Website: [VibezNow.top] (http://www.vibeznow.top)

[Presentation] (http://www.bit.do/VibezNow) of the Project

The project was built in three weeks using open source technologies with the data pipeline running on AWS.

Data technologies used:
- Kafka
- Spark Streaming
- Redis

Most people listen to music that suits their current mood or occasion. The purpose of this project is to provide in real-time song suggestions that reflect what people are talking about, what is going on in their life. This is achieved by collecting Twitter tweets over a short period of time and making a bag-of-words vector of the words used in those messages. The lyrics of songs from [musiXmatch] (http://labrosa.ee.columbia.edu/millionsong/musixmatch) dataset are preprocessed into the bag-of-words vectors, and the songs that reflect best the tweets are chosen based on cosine similarity between the tweet-vector and the lyrics-vectors. Additionally, the user may choose a theme word, in which case only the tweets including that word are used when calculating the tweet-vector. New top 10 suggestions are provided every three minutes.

UPDATE, 10/23/2016: The program has ended and the data pipeline of the project has been taken down. The front-end is still running on a separate server, however, but the suggestions are not being updated anymore.

## **Preprocessing lyrics**

The lyrics in the musiXmatch dataset were obtained as two text files, mxm_dataset_train.txt and mxm_data_set_train.txt, which contained the 5000 most common words in the whole dataset, and the lyrics of 237,662 songs in the sparse bag-of-words vector format of raw counts of words matching the list of 5000 words. The conducted preprocessing of those lyrics included the following steps:

1. Stopwords, as provided in the *stopwords.txt*, numbers and words with non-UTF-8 characters were removed from the list of words resulting to a new list of 4681 words. The bag-of-words vectors were mapped to this new list of words.

2. Songs with no lyrics or missing either name of the artist or title of the song in the *unique_tracks.txt* file were omitted, resulting to a total of 237,642 songs.

3. For each word, the number of lyrics that the word appeared in was calculated, as was the maximum count of each word within a single lyrics. These values were used to calculate normalized term frequencies, tf-idf, that account for different length of the lyrics and putting more weight on words that appear less frequently in the lyrics. 

4. Second norm was calculated for each vector after the normalization.

5. Lyrics were stored into a Redis database to be accessed during the stream processing of Twitter tweets.

## **Data ingestion**

The Twitter tweets are ingested using Kafka with the messages being produced to a Kafka topic *twitter_stream_new* using producer *twitter_stream.py*. The default source of the tweets is the Twitter API connected to a stream that is limited to US using latitude and longitude ranges. Alternatively, the tweets can be produced from a file of pre-recorded tweets, which is achieved by providing as inline arguments the name of the file, how many times the file will be looped over and the waiting time in seconds between two produced messages. The third argument allows controlling the message production rate. 

## **Stream processing**

The tweets are continuously processed in small batches using Spark Streaming process *spark_streaming_process.py*. That process connects directly to the Kafka topic to consume the messages. First the message parts are extracted from the incoming tweets, and links and non-letter characters are removed. The the cleaned messages are filtered into five themes with general theme containing all messages and four special themes containing only messages that include a theme-word. Theme-words were chosen to be "Food", "Love", "Shop" and "Sport", and they were chosen arbitrarily to present topics that people are often interested in, and they are also common enough that there would be non-negligible number of messages filtered to those themes for each batch processed.

After being filtered into themes, the messages are divided into separate words keeping only the words included in the set of 4681 words used in the bag-of-words vectors of the lyrics. The words are then counted and normalized relative to the most frequent word in order to account for the varrying amounts of words used in a batch of tweets. The normalized word frequencies are mapped into the same sparse vector bag-of-words format as the lyrics, and the second norm of the vector is calculated. Finally, the cosine similarity is calculated between each combination of lyrics-vector and the tweet-vectors representing the five themes. For the top ten similarities, the artists and song titles are cached into a Redis database along with the top ten words that had the highest weight in the tweet vectors.

It should be noted that the inverse document frequency is not calculated for the tweet-vectors. Such calculation would be technically straightforward to add, but it was found out to produce undesired side effects, namely emphasizing foreign words with extremely low term frequencies, making the suggestions very random.

## **WebUI**

The WebUI is build with Flask micro-web framework, with Tornado server used to handle multiple users. In the first view of the web page, the user is given the option to choose either not to have any special theme, or any of the four predefined themes. After submitting the choice, the user is shown the top ten artist and song title suggestions and the top ten words on which the suggestions were based on. The user may then choose another topic and will be provided new suggestions based on that choice.

## **Known issues and future developments**

The two main issues with the current implementation of VibezNow are that new suggestions are provided only every three minutes and that there are no user-specific suggestions, only the four themes to choose from.

The update rate depends on the batch length of the streaming process, which is set to be high enough to accommodate the processing time and to ensure that enough messages are accumulated during that time period to have enough data for the tweet-vector. By allowing a higher throughput of messages, a shorter batch would have enough messages, but the bottleneck is in the time needed to calculate the cosine similarity between the tweet-vector and all the lyrics-vectors. That computational burden could be lessened by dividing the lyrics into subgroups based on language and music genre, and clustering the vectors within each subgroup. That approach would also make the system scalable for increasing number of lyrics.

Regarding the user-specific suggestions, there were two possible approaches considered. As a straightforward extension of the current version, the user could provide the theme word used for filtering the messages. This approach, however, could easily run into issues of not having enough messages after the filtering, especially if none of the messages in a batch would contain that word. A more reasonable approach would be to use user's social media presence to provide user-specific suggestions. This couls be done, for example, by adding a batch process that counts the words used in the recent Facebook posts and/or Twitter tweets from the user and her/his first-level connections, and calculates a user-specific bag-of-words vector from the word frequencies. The user-specific suggestions would then be based on the similarity of both the tweet-vector and user-specific-vector to the lyrics-vectors, with the user defining the weighting of those two similarities. In other words, the user could set the suggestions to be based only on their own vector, only on the tweet-vector, or, for example, 30 % on their vector and 70 % on the tweet-vector. 