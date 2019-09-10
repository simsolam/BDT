users = LOAD '/home/cloudera/Desktop/user.csv' USING PigStorage(',') AS (user:chararray , age:int);
users = FILTER users BY(age>=18 AND age<=25);
sites = LOAD '/home/cloudera/Desktop/page.csv' USING PigStorage(',') AS (user:chararray , site:chararray);
joined = JOIN users BY user, sites BY user;
grouped = GROUP joined BY site;
top_site_count = FOREACH grouped GENERATE group, COUNT(joined) AS counted;
top_site_count =  ORDER top_site_count BY counted DESC;
top_data = LIMIT top_site_count 5;
DUMP top_data;