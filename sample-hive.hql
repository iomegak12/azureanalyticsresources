DROP TABLE IF EXISTS HiveSampleIn; 
 CREATE EXTERNAL TABLE HiveSampleIn 
 (
     ProfileID     string, 
     SessionStart     string, 
     Duration     int, 
     SrcIPAddress     string, 
     GameType     string
 ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '10' STORED AS TEXTFILE LOCATION '${hiveconf:Input}'; 

 DROP TABLE IF EXISTS HiveSampleOut; 
 CREATE EXTERNAL TABLE HiveSampleOut 
 (
     ProfileID     string, 
     Duration     int
 ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '10' STORED AS TEXTFILE LOCATION '${hiveconf:Output}';

 INSERT OVERWRITE TABLE HiveSampleOut
 Select 
     ProfileID,
     SUM(Duration)
 FROM HiveSampleIn Group by ProfileID