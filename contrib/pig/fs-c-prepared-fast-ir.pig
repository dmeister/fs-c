/*
 * Reuses the prepared chunk data
 */
CL1 = LOAD 'hdfs://hadoop6:54310/fsc-data/dkrz2/work_b1-cdc8/chunkdata*' USING PigStorage() AS (fp: chararray, filelength: long, filetype: chararray, usage_count:long, chunksize:int);
CL2 = LOAD 'hdfs://hadoop6:54310/fsc-data/dkrz2/work_b2-cdc8/chunkdata*' USING PigStorage() AS (fp: chararray, filelength: long, filetype: chararray, usage_count:long, chunksize:int);
CL3 = LOAD 'hdfs://hadoop6:54310/fsc-data/dkrz2/work_b3-cdc8/chunkdata*' USING PigStorage() AS (fp: chararray, filelength: long, filetype: chararray, usage_count:long, chunksize:int);
CL4 = LOAD 'hdfs://hadoop6:54310/fsc-data/dkrz2/work_b4-cdc8/chunkdata*' USING PigStorage() AS (fp: chararray, filelength: long, filetype: chararray, usage_count:long, chunksize:int);
CL5 = LOAD 'hdfs://hadoop6:54310/fsc-data/dkrz2/work_b5-cdc8/chunkdata*' USING PigStorage() AS (fp: chararray, filelength: long, filetype: chararray, usage_count:long, chunksize:int);
CL6 = LOAD 'hdfs://hadoop6:54310/fsc-data/dkrz2/work_b6-cdc8/chunkdata*' USING PigStorage() AS (fp: chararray, filelength: long, filetype: chararray, usage_count:long, chunksize:int);
CL7 = LOAD 'hdfs://hadoop6:54310/fsc-data/dkrz2/work_b7-cdc8/chunkdata*' USING PigStorage() AS (fp: chararray, filelength: long, filetype: chararray, usage_count:long, chunksize:int);
CL8 = LOAD 'hdfs://hadoop6:54310/fsc-data/dkrz2/work_b8-cdc8.stopped_by_power_outage/chunkdata*' USING PigStorage() AS (fp: chararray, filelength: long, filetype: chararray, usage_count:long, chunksize:int);

CL = UNION CL1, CL2, CL3, CL4, CL5, CL6, CL7, CL8;
S1 = GROUP CL BY fp;
S2= FOREACH S1 GENERATE SUM($1.usage_count) as usage_count, MAX($1.chunksize) as chunksize;
S3 = FOREACH S2 GENERATE (double)usage_count * (double)chunksize as totalsize, chunksize as realsize;
W1 = GROUP S3 ALL PARALLEL 1;
W2 = FOREACH W1 GENERATE SUM($1.totalsize) AS totalsize, SUM($1.realsize) as realsize;
W3 = FOREACH W2 GENERATE totalsize, realsize, totalsize - realsize as redundancy;
W4 = ORDER W3 BY totalsize DESC;
STORE W4 into 'hdfs://hadoop6:54310/fsc-data/dkrz2/work_b/ir-exact-all-result' USING PigStorage();