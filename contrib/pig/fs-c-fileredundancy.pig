REGISTER fs-c-0.3.13.jar

FILEFINGERPRINTS = LOAD '$RUN/file-fingerprint' USING PigStorage() as (filename:chararray, fingerprint:chararray);
FILES = LOAD '$RUN/files' USING PigStorage() AS (filename: chararray, filelength: long, filetype: chararray, label:chararray);

A = JOIN FILES by filename, FILEFINGERPRINTS by filename;
B = GROUP A by FILEFINGERPRINTS::fingerprint;
C = FOREACH B GENERATE group, de.pc2.dedup.util.udf.Most($1.FILES::filetype) as filetype, AVG($1.FILES::filelength) as filelength, COUNT($1) as occurs;
D = FOREACH C GENERATE filetype, filelength, occurs * filelength AS totalsize, filelength AS realsize;

E = GROUP D BY filetype;
F = FOREACH E GENERATE group as filetype, SUM($1.totalsize) / (1024 * 1024) AS totalsize, SUM($1.realsize) / (1024*1024) as realsize;
G = FOREACH F GENERATE filetype, totalsize, realsize, totalsize - realsize as redundancy;
G_SORTED = ORDER G BY totalsize DESC;
STORE G_SORTED into '$OUTPUT/fr-types-result' USING PigStorage();

DC = FOREACH D GENERATE de.pc2.dedup.util.udf.FileSizeCategory(filelength) as filelength, filetype, totalsize, realsize;
E = GROUP DC BY filelength;
F = FOREACH E GENERATE group as filelength, SUM($1.totalsize) / (1024 * 1024) AS totalsize, SUM($1.realsize) / (1024*1024) as realsize;
G = FOREACH F GENERATE filelength, totalsize, realsize, totalsize - realsize as redundancy;
G_SORTED = ORDER G BY totalsize DESC;
STORE G_SORTED into '$OUTPUT/fr-sizes-result' USING PigStorage();

H = FOREACH G GENERATE 'ALL' as filetype, totalsize, realsize, redundancy;
J = GROUP H by filetype;
I = FOREACH J GENERATE SUM($1.totalsize), SUM($1.realsize), SUM($1.redundancy);
STORE I into '$OUTPUT/fr-all-result' USING PigStorage();

