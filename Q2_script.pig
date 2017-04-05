SET default_parallel 4;
set job.name 'GROUP 6 : Q2';
--load the data from HDFS and define the schema
raw = LOAD '/data2/cl03.csv' USING PigStorage(',') AS  (date, type:chararray, parl:int, prov:chararray, riding:chararray, lastname:chararray, firstname:chararray, gender:chararray, occupation:chararray, party:chararray, votes:int, percent:double, elected:int);

--some data entries use the middle name as well, so this way we will catch all of them
fltrd = FILTER raw by votes < 100;
won = FILTER fltrd by elected == 1;
lost = FILTER fltrd by elected == 0;

cross_table = CROSS won, lost;

fltrd_table = FILTER cross_table by ABS(won::votes - lost::votes) < 10;

tmp = FOREACH fltrd_table GENERATE won::lastname, lost::lastname;
results = DISTINCT tmp;

dump results;
illustrate results;
STORE results INTO 'q2';
