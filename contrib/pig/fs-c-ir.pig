/*
 * Heuristical because it may shift data between file types/file length categories if data is shared between types.
 * It is much faster then the exact approach, but it should nevertheless not really be used.
 */
REGISTER fs-c-0.3.14.jar
CHUNKS = LOAD '$RUN/chunks' using PigStorage() AS (filename: chararray, fp: chararray, chunksize: int);
FILES = LOAD '$RUN/files' USING PigStorage() AS (filename: chararray, filelength: long, filetype: chararray);

FILES_A = FOREACH FILES GENERATE filename, de.pc2.dedup.util.udf.FileSizeCategory(filelength) as filelength, filetype;
A = JOIN FILES_A by filename, CHUNKS by filename;
B = GROUP A by CHUNKS::fp;
C = FOREACH B GENERATE group, de.pc2.dedup.util.udf.Most($1.FILES_A::filetype) as filetype, de.pc2.dedup.util.udf.Most($1.FILES_A::filelength) as filelength, COUNT($1) as occurs, AVG($1.chunksize) as chunksize;
D = FOREACH C GENERATE filetype, filelength, occurs * chunksize AS totalsize, chunksize AS realsize;

E = GROUP D BY filetype;
F = FOREACH E GENERATE group as filetype, SUM($1.totalsize) / (1024 * 1024) AS totalsize, SUM($1.realsize) / (1024*1024) as realsize;
G = FOREACH F GENERATE filetype, totalsize, realsize, totalsize - realsize as redundancy;
G_SORTED = ORDER G BY totalsize DESC;
STORE G_SORTED into '$OUTPUT/ir-types-result' USING PigStorage();

E = GROUP D BY filelength;
F = FOREACH E GENERATE group as filelength, SUM($1.totalsize) / (1024 * 1024) AS totalsize, SUM($1.realsize) / (1024*1024) as realsize;
G = FOREACH F GENERATE filelength, totalsize, realsize, totalsize - realsize as redundancy;
G_SORTED = ORDER G BY totalsize DESC;
STORE G_SORTED into '$OUTPUT/ir-sizes-result' USING PigStorage();

H = FOREACH G GENERATE 'ALL' as filetype, totalsize, realsize, redundancy;
J = GROUP H by filetype;
I = FOREACH J GENERATE SUM($1.totalsize), SUM($1.realsize), SUM($1.redundancy);
STORE I into '$OUTPUT/ir-all-result' USING PigStorage();

