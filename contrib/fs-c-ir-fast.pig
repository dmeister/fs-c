CHUNKS = LOAD '$RUN/chunks' using PigStorage() AS (filename: chararray, fp: chararray, chunksize: int);
FILES = LOAD '$RUN/files' USING PigStorage() AS (filename: chararray, filelength: long, filetype: chararray);

C = FOREACH CHUNKS GENERATE fp, chunksize;
D = GROUP C BY fp;
E = FOREACH D GENERATE 0 as dummy, COUNT($1) as occurs, AVG($1.chunksize) as chunksize;
F = FOREACH E GENERATE dummy, occurs * chunksize AS totalsize, chunksize AS realsize;
G = GROUP F BY dummy;
H = FOREACH G GENERATE group, SUM($1.totalsize) / (1024 * 1024) AS totalsize, SUM($1.realsize) / (1024*1024) as realsize;
I = FOREACH H GENERATE totalsize, realsize, totalsize - realsize as redundancy;
STORE I into '$OUTPUT/ir-result' USING PigStorage();