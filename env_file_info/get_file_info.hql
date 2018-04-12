drop table if exists default.file_cnt_raw;
CREATE TABLE default.file_cnt_raw(
colmn1 string ,
colmn2 string ,
colmn3 string ,
colmn4 string ,
colmn5 string ,
colmn6 string 
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
 'quoteChar'='\""', 
 'separatorChar'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/path/if/external/or/takeoff/location/';

drop table if exists default.small_file_info;
  CREATE TABLE default.small_file_info(
    source string,
    databse_name string,
    table_name string,
    bucket_name string,
    file_name_path string,
    file_size int,
    dir_path string,
    file_name string,
    partition_folder string)
STORED AS PARQUET;
    
insert into table default.small_file_info
select
colmn1 as source,
colmn2 as databse_name,
colmn3 as table_name,
colmn4 as bucket_name,
colmn5 as file_name_path,
cast(colmn6 as int) as file_size,
regexp_extract(colmn5, '^(.*)\/([^\/]+)', 1) as dir_path,
regexp_extract(colmn5, '^(.*)\/([^\/]+)', 2) as file_name,
regexp_extract(colmn5, '^(.*)\=([^\/]+)', 2) as partition_folder
from default.file_cnt_raw;
