movies = LOAD '/home/cloudera/Desktop/movies.csv' USING PigStorage(',') AS (movieId:chararray, title:chararray, genre:chararray);
movies_starting_with_A = FILTER movies BY (LOWER(title)>='a' AND LOWER(title)<'b');
genre_token = FOREACH movies_starting_with_A GENERATE TOKENIZE(genre,'\\|') AS genre_bag;
genre_bag = FOREACH genre_token GENERATE FLATTEN(genre_bag) AS genre;
grouped_genre = GROUP genre_bag BY genre;
genre_count = FOREACH grouped_genre GENERATE group AS genre, COUNT(genre_bag) AS counted;
STORE genre_count INTO 'Desktop/moviesCountByGenre';
dump genre_count;