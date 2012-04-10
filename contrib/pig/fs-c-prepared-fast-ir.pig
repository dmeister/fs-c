/*
 * Reuses the prepared chunk data
 */
CL = LOAD '$CHUNKDATA/chunkdata*' USING PigStorage() AS (fp: chararray, filelength: long, filetype: chararray, usage_count:long, chunksize:int);
-- CL: {fp: chararray,filelength: long,filetype: chararray,usage_count: long,chunksize: int}

S1 = GROUP CL BY fp;
S2= FOREACH S1 GENERATE SUM($1.usage_count) as usage_count, MAX($1.chunksize) as chunksize;
S3 = FOREACH S2 GENERATE (double)usage_count * (double)chunksize as totalsize, chunksize as realsize;
W1 = GROUP S3 ALL PARALLEL 1;
W2 = FOREACH W1 GENERATE SUM($1.totalsize) AS totalsize, SUM($1.realsize) as realsize;
W3 = FOREACH W2 GENERATE totalsize, realsize, totalsize - realsize as redundancy;
W4 = ORDER W3 BY totalsize DESC;
STORE W4 into '$OUTPUT/ir-exact-all-fast-result' USING PigStorage();