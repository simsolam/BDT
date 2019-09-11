users = LOAD '/home/cloudera/Desktop/users.txt' USING PigStorage('|') AS(id:int, age:int, gender:chararray, occupation:chararray, zip:chararray);
users = FILTER users BY(gender=='M');
grouped = GROUP users BY gender;
counts = FOREACH grouped GENERATE 'Number of Male lawyers: ', COUNT(users.gender);
STORE counts INTO 'Desktop/Q1_male_lawyers';
DUMP counts;
