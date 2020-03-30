# stackfiledb
 
Stackfiledb stores many small files in a large file, which is a general file in the OS file system; small files can also be other binary data.  
Stackfiledb can be used as an object storage services.   
The index of the actual file is completely in memory, which can speed up the location and reading.  
When delete and update data, the actual data is not deleted immediately. This implementation can provide access to historical data for a period of time.  
