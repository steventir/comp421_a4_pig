raw = LOAD '/data2/emp.csv' USING PigStorage(',') AS (empid:int, fname:chararray, lname:chararray, deptname:chararray, isManager:chararray, mgrid:int, salary:int);


fltrd_mgr = FILTER raw BY deptname == 'Finance' and isManager == 'Y';
fltrd_emp = FILTER raw BY isManager == 'N';

jnd_mgr_emp = JOIN fltrd_mgr by empid, fltrd_emp BY mgrid; 

grp = GROUP jnd_mgr_emp BY fltrd_mgr::empid;

res = FOREACH grp GENERATE $0, jnd_mgr_emp.fltrd_emp::lname, COUNT(jnd_mgr_emp.fltrd_emp::lname);

dump res;