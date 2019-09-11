users = LOAD '/home/cloudera/Desktop/users.txt' USING PigStorage('|') AS(id:int, age:int, gender:chararray, occupation:chararray, zip:chararray);
users = FILTER users BY(gender=='M');
users = ORDER users BY age DESC;
oldest = LIMIT users 1;
oldest = FOREACH oldest GENERATE 'User ID of oldest male lawyer: ', $0;
STORE oldest INTO 'Desktop/Q2_oldest_male_lawyer';
DUMP oldest;
