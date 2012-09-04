REGISTER fs-c-0.3.13.jar
CHUNKS = LOAD '$RUN/chunks*' using PigStorage() AS (filename: chararray, fp: chararray, chunksize: int);
FILES = LOAD '$RUN/files*' USING PigStorage() AS (filename: chararray, filelength: long, filetype: chararray);

FILES_A = FOREACH FILES GENERATE filename, de.pc2.dedup.util.udf.FileSizeCategory(filelength) as filelength, filetype;
A = JOIN FILES_A by filename, CHUNKS by filename;

B = GROUP A BY (CHUNKS::fp, filelength, filetype);

-- B: {group: (CHUNKS::fp: chararray,FILES_A::filelength: long,FILES_A::filetype: chararray),A: {(FILES_A::filename: chararray,FILES_A::filelength: long,FILES_A::filetype: chararray,CHUNKS::filename: chararray,CHUNKS::fp: chararray,CHUNKS::chunksize: int)}}
CL = FOREACH B GENERATE $0.fp as fp, $0.filelength as filelength, $0.filetype as filetype, COUNT($1) as usage_count, MAX($1.chunksize) as chunksize;

S1 = GROUP CL BY fp;
S2= FOREACH S1 GENERATE group, SUM($1.usage_count) as usage_count, MAX($1.chunksize) as chunksize;
-- S2: {group: chararray,usage_count: long,chunksize: int}

S3 = FOREACH S2 GENERATE group as fp, (double)chunksize / (double)usage_count as realsize;
-- ratio of the physical storage accounted for each reference

T1 = JOIN S3 by fp, CL by fp;
-- T1: {S3::fp: chararray,S3::realsize: double,CL::fp: chararray,CL::filelength: long,CL::filetype: chararray,CL::usage_count: long,CL::chunksize: int}

T2 = FOREACH T1 GENERATE CL::fp as fp, CL::filelength, CL::filetype as filetype, CL::usage_count, CL::chunksize as chunksize, S3::realsize;
-- we have one row per chunk fp with usage count information and with its relative physical size
-- T2: {fp: chararray,CL::filelength: long,filetype: chararray,CL::usage_count: long,chunksize: int,S3::realsize: double}

T3 = FOREACH T2 GENERATE fp, filelength, filetype, (double)usage_count * (double)chunksize as totalsize, (double)usage_count * realsize as realsize;

U1 = GROUP T3 by filetype PARALLEL 1;
U2 = FOREACH U1 GENERATE group as filetype, SUM($1.totalsize) / 1048576.0 AS totalsize, SUM($1.realsize) / 1048576.0 as realsize;
U3 = FOREACH U2 GENERATE filetype, totalsize, realsize, totalsize - realsize as redundancy;
U4 = ORDER U3 BY totalsize DESC;
STORE U4 into '$OUTPUT/ir-exact-types-result' USING PigStorage();

V1 = GROUP T3 by filelength PARALLEL 1;
V2 = FOREACH V1 GENERATE group as filelength, SUM($1.totalsize) / 1048576.0 AS totalsize, SUM($1.realsize) / 1048576.0 as realsize;
V3 = FOREACH V2 GENERATE filelength, totalsize, realsize, totalsize - realsize as redundancy;
V4 = ORDER V3 BY totalsize DESC;
STORE V4 into '$OUTPUT/ir-exact-sizes-result' USING PigStorage();

W1 = GROUP V3 ALL PARALLEL 1;
W2 = FOREACH W1 GENERATE SUM($1.totalsize) AS totalsize, SUM($1.realsize) as realsize;
W3 = FOREACH W2 GENERATE totalsize, realsize, totalsize - realsize as redundancy;
W4 = ORDER W3 BY totalsize DESC;
STORE W4 into '$OUTPUT/ir-exact-all-result' USING PigStorage();