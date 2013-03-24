/*
 * This script does part of the heavy lifting of the initial join.
 * The output is an entry per fp, file size category, filetype combination with the number of usages and the chunk size
 * The resutl can be reused a lot
 */
REGISTER fs-c-0.3.14.jar
CHUNKS = LOAD '$RUN/chunks*' using PigStorage() AS (filename: chararray, fp: chararray, chunksize: int);
FILES = LOAD '$RUN/files*' USING PigStorage() AS (filename: chararray, filelength: long, filetype: chararray);

FILES_A = FOREACH FILES GENERATE filename, de.pc2.dedup.util.udf.FileSizeCategory(filelength) as filelength, filetype;
A = JOIN FILES_A by filename, CHUNKS by filename;

B = GROUP A BY (CHUNKS::fp, filelength, filetype);

-- B: {group: (CHUNKS::fp: chararray,FILES_A::filelength: long,FILES_A::filetype: chararray),A: {(FILES_A::filename: chararray,FILES_A::filelength: long,FILES_A::filetype: chararray,CHUNKS::filename: chararray,CHUNKS::fp: chararray,CHUNKS::chunksize: int)}}
C = FOREACH B GENERATE $0.fp as fp, $0.filelength as filelength, $0.filetype as filetype, COUNT($1) as usage_count, MAX($1.chunksize) as chunksize;
STORE C into '$OUTPUT/chunkdata' USING PigStorage();