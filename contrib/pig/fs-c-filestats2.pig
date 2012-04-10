/*
 * Chunk based file stats
 */
 /*
 * a bit heuristical because it provides outdated file sizes if data is written to the file concurrently
 */
REGISTER fs-c-0.3.12.jar
FILES = LOAD '$RUN/files' USING PigStorage() AS (filename: chararray, filelength: long, filetype: chararray);
CHUNKS = LOAD '$RUN/chunks*' using PigStorage() AS (filename: chararray, fp: chararray, chunksize: int);

A1 = GROUP CHUNKS by filename;
A2 = FOREACH A1 GENERATE group as filename, SUM($1.chunksize) as chunkbased_filelength;
A3 = JOIN FILES by filename, A2 by filename;
A4 = FOREACH A3 GENERATE filename, filetype, chunkbased_filelength;
A5 = ORDER A4 BY lengthcategory DESC;
STORE A5 into '$OUTPUT/files' USING PigStorage();

B1 = GROUP A5 BY filetype;
B2 = FOREACH B1 GENERATE group as filetype, SUM($1.chunkbased_filelength) as filelength, COUNT($1) as filecount;
B3 = ORDER B2 BY filelength DESC;
STORE B3 into '$OUTPUT/filetype-stats' USING PigStorage();

C1 = FOREACH A5 GENERATE de.pc2.dedup.util.udf.FileSizeCategory(chunkbased_filelength) as chunkbased_filelength, filelength;
C2 = GROUP C1 BY lengthcategory;
C3 = FOREACH C2 GENERATE group as lengthcategory, SUM($1.filelength) as filelength, COUNT($1) as filecount;
C4 = ORDER C3 BY lengthcategory DESC;

STORE C4 into '$OUTPUT/filesize-stats' USING PigStorage();



