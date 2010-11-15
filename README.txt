Introduction

The fs-c tools can be used in two modes (Trace and Parse). In the Trace mode directories of a 
file system are processed by chunking all files using the specified chunking methods. While 
a direct analysis of the chunks if possible, often the chunk data is stored in a trace file 
for later processing. These chunk files then can be analysed in detail during the Parse mode.

Trace Details

The trace mode is started with fs-c trace and accepts the following parameters:

-f	 --filename	     Name of a directories to parse. The application accepts multiple -f options
     --min-size	     Minimal chunk size (Default: 2 KB)
     --max-size	     Maximal chunk size (Default: 32 KB)
-s	 --avg-size	     Average chunk size (Default: 64 KB)
-c	 --chunker	     Chunking method (cdc, static)
-o	 --output	     Output file (optional). If no output is specified, the chunks are analyzed 
                     directly
-t	 --threads	     Number of concurrent threads (Default: 1)
     --silent	     Reduced output
-l	 --file-listing	 File contains a listing of files (-f option does not contain a directory, 
                     but a file that contains a directory to parse in each line)
-p	 --privacy	     Privacy Mode (hashes the filename to avoid storing concrete filenames in trace 
                     files for privacy reasons)
                     
                     
Parse Details

The parse mode is started with fs-c parse and accepts the following parameters:

-t	 --type	 Type of the analysis. Currently supported:
              - "simple", that displays deduplication ratios for each chunked file (similar to 
                     fs-c trace without output option),
              - "ir" calculation deduplication ratios for each file type and file size categories,
              - "tr" calculating the temporal redundancy between two traces to simulate a 
                     backup scenario
-o	 --output	 Run name
-f	 --filename	 Trace filename
