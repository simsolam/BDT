lines = LOAD '/home/cloudera/Desktop/wordcount_simple.txt' AS (line:chararray);
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line,' ')) as word;
grouped = GROUP words BY word;
wordcount = FOREACH grouped GENERATE group, COUNT(words);
DUMP wordcount;