set job.name 'GROUP 6 : Q3';
--load the data from HDFS and define the schema
raw = LOAD '/data2/cl03.csv' USING PigStorage(',') AS  (date, type:chararray, parl:int, prov:chararray, riding:chararray, lastname:chararray, firstname:chararray, gender:chararray, occupation:chararray, party:chararray, votes:int, percent:double, elected:int);

--some data entries use the middle name as well, so this way we will catch all of them

fltrd = FILTER raw by type == 'Gen' and elected == 1;
grp = GROUP fltrd BY parl;

tupl_a = FOREACH grp GENERATE $0, COUNT(fltrd.parl);
tupl_b = FOREACH grp GENERATE $0, COUNT(fltrd.parl);
first_tpl = FILTER tupl_a BY $0 == 1;


cple = JOIN tupl_a BY $0, tupl_b BY ($0+1);
diff_data = FOREACH cple GENERATE $0, $1-$3;

results = diff_data;

dump first_tpl;
dump results;

--illustrate results;
STORE results INTO 'q3';
