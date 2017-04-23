set job.name 'GROUP 6 : Q4';
--load the data from HDFS and define the schema
raw = LOAD '/data2/cl03.csv' USING PigStorage(',') AS  (date, type:chararray, parl:int, prov:chararray, riding:chararray, lastname:chararray, firstname:chararray, gender:chararray, occupation:chararray, party:chararray, votes:int, percent:double, elected:int);

--some data entries use the middle name as well, so this way we will catch all of them
noheader = FILTER raw BY gender != 'Gender';
selected = FOREACH noheader GENERATE parl, lastname, firstname, party;
distincted = DISTINCT selected;
grouped = GROUP distincted BY (parl, party);
counted = foreach grouped Generate group.parl, group.party, COUNT(distincted);

results = counted;
store results into 'q4' using PigStorage('\t', '-schema');

dump results;
illustrate results;
STORE results INTO 'q4';