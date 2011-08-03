REGISTER fs-c-0.3.7.jar
CHUNKS1 = LOAD '$RUN1/chunks' using PigStorage() AS (filename: chararray, fp: chararray, chunksize: int);

CHUNKS2 = LOAD '$RUN2/chunks' using PigStorage() AS (filename: chararray, fp: chararray, chunksize: int);
FILES2 = LOAD '$RUN2/files' USING PigStorage() AS (filename: chararray, filelength: long, filetype: chararray);

FILES2_A = FOREACH FILES2 GENERATE filename, de.pc2.dedup.util.udf.FileSizeCategory(filelength) as filelength, filetype;
FC2 = JOIN FILES2_A by filename, CHUNKS2 by filename;

FC1_G = GROUP CHUNKS1 BY fp;
FC1_M = FOREACH FC1_G GENERATE group as fp, COUNT($1) as occurs;

FC2_G = GROUP FC2 BY CHUNKS2::fp;
FC2_M = FOREACH FC2_G GENERATE group as fp, de.pc2.dedup.util.udf.Most($1.FILES2_A::filetype) as filetype, de.pc2.dedup.util.udf.Most($1.FILES2_A::filelength) as filelength, COUNT($1) as occurs, AVG($1.chunksize) as chunksize;

FC_M = COGROUP FC1_M BY fp, FC2_M BY fp INNER;
FC_MR = FOREACH FC_M GENERATE group as fp, FLATTEN(FC2_M.filetype) as filetype, FLATTEN(FC2_M.filelength) as filelength, SUM(FC1_M.occurs) as old_occurs, FLATTEN(FC2_M.occurs) as new_occurs, FLATTEN(FC2_M.chunksize) as chunksize;

D = FOREACH FC_MR GENERATE fp, filetype, filelength, de.pc2.dedup.util.udf.TemporalRedundancy(old_occurs,new_occurs,chunksize) as data;
D2 = FOREACH D GENERATE fp, filetype, filelength, FLATTEN(data.totalsize) as totalsize, FLATTEN(data.realsize) as realsize;

E = GROUP D2 BY filetype;
F = FOREACH E GENERATE group as filetype, SUM($1.totalsize) / (1024 * 1024) AS totalsize, SUM($1.realsize) / (1024*1024) as realsize;
G = FOREACH F GENERATE filetype, totalsize, realsize, totalsize - realsize as redundancy;
G_SORTED = ORDER G BY totalsize DESC;
STORE G_SORTED into '$OUTPUT/tr-types-result' USING PigStorage();

E = GROUP D BY filelength;
F = FOREACH E GENERATE group as filelength, SUM($1.totalsize) / (1024 * 1024) AS totalsize, SUM($1.realsize) / (1024*1024) as realsize;
G = FOREACH F GENERATE filelength, totalsize, realsize, totalsize - realsize as redundancy;
G_SORTED = ORDER G BY totalsize DESC;
STORE G_SORTED into '$OUTPUT/tr-sizes-result' USING PigStorage();

H = FOREACH G GENERATE 'ALL' as filetype, totalsize, realsize, redundancy;
J = GROUP H by filetype;
I = FOREACH J GENERATE SUM($1.totalsize), SUM($1.realsize), SUM($1.redundancy);
STORE I into '$OUTPUT/tr-all-result' USING PigStorage();
