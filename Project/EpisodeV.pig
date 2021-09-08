inputDialouges = LOAD 'hdfs://localhost:9000/user/root/jahnavi/inputs/EpisodeV.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
ranked = RANK inputDialouges;
OnlyDialouges = FILTER ranked BY (rank_inputDialouges > 2);
groupByName = GROUP OnlyDialouges BY name;
names = FOREACH groupByName GENERATE $0 AS name, COUNT($1) AS no_of_lines;
namesOrdered = ORDER names BY no_of_lines DESC;
STORE namesOrdered INTO 'hdfs://localhost:9000/user/root/jahnavi/output/' USING PigStorage('\t');