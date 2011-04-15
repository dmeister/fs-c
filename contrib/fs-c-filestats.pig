REGISTER fs-c-0.3.3.jar
FILES = LOAD '$RUN/files' USING PigStorage() AS (filename: chararray, filelength: long, filetype: chararray);

A = FOREACH FILES GENERATE filetype, filelength, 1 as filecount;
B = GROUP A BY filetype;
C = FOREACH B GENERATE group as filetype, SUM($1.filelength) as filelength, SUM($1.filecount) as filecount;
C_SORTED = ORDER C BY filelength DESC;

STORE C_SORTED into '$OUTPUT/filetype-stats' USING PigStorage();

D = FOREACH FILES GENERATE de.pc2.dedup.util.udf.FileSizeCategory(filelength) as lengthcategory, filelength, 1 as filecount;
E = GROUP D BY lengthcategory;
F = FOREACH E GENERATE group as lengthcategory, SUM($1.filelength) as filelength, SUM($1.filecount) as filecount;
F_SORTED = ORDER F BY lengthcategory DESC;

STORE F_SORTED into '$OUTPUT/filesize-stats' USING PigStorage();



