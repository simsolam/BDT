movies = LOAD '/home/cloudera/Desktop/movies.csv' USING PigStorage(',') AS (movieId:int, title:chararray, genre:chararray);
ratings = LOAD '/home/cloudera/Desktop/rating.txt' USING PigStorage('\\t') AS (userId:chararray, movieId:int, rating:int, timestamp:int);
rating_grouped = GROUP ratings BY movieId;
rating_calc = FOREACH rating_grouped GENERATE group AS movieId,1.0f*SUM(ratings.rating)/COUNT(ratings.rating) AS average:FLOAT;
genre_token = FOREACH movies GENERATE movieId, title, TOKENIZE(genre,'\\|') AS genreBag;
genre_bag = FOREACH genre_token GENERATE movieId, title, FLATTEN(genreBag) AS genre;
adventure_movies = FILTER genre_bag BY genre=='Adventure';
movie_rating_joined = JOIN adventure_movies BY movieId, rating_calc BY movieId;
ordered_movie_by_avg = ORDER movie_rating_joined BY average DESC;
top_20 = LIMIT ordered_movie_by_avg 20;
top_20 = FOREACH top_20 GENERATE adventure_movies::movieId,adventure_movies::genre,rating_calc::average,adventure_movies::title;
top_20_ordered = ORDER top_20 BY adventure_movies::movieId;
movies_with_header = FOREACH top_20_ordered GENERATE adventure_movies::movieId AS MovieID, adventure_movies::genre AS Genre, rating_calc::average AS Rating, adventure_movies::title AS Title;
STORE movies_with_header INTO 'Desktop/Q4_movies_with_header' USING org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

