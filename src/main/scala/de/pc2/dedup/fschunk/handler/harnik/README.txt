Handler to implement Harnik's estimation method

Research Paper:
Danny Harnik, Oded Margalit, Dalit Naor, Dmitry Sotnikov, and Gil Vernik: Estimation of Deduplication Ratios in Large Data Sets, Processings of
MSST 2012

The method implemented differs in some ways from the original approach:
- We implemented it as a post processing phase handler. That means the trace file already exists and all data has already been read. 
- We use reservior sampling for the sampling phase. We don't have the number of chunks or the total file size during the first pass
- The cannot modify the sampling to sample linearly to the chunk size, but we accond for it during the final estimation calculation
