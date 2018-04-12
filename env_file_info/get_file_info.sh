#!/bin/bash
#Developer  : Rajesh K Bathula
#script Info: Thsi Script connect to hive, hdfs and aws s3 and fetches path and size of files
#readme



# Below will list all the Databases in Hive in Environment which it runs into a file
hive -e 'show databases' | tee databases.txt
while read database ; do
echo $database
hive -e "show tables in ${database}" | tee ${database}
done < databases.txt

# Below will get all the table locations if its is S3 into a aws_path.txt file and if HDFS into a hdfs_path.txt
while read database; do
  while read table; do
  echo $database.$table
    location_table=`hive -e "describe formatted $database.$table" | grep "Location:" | cut -f3- -d' '`
    echo $location_table
    check_path=`echo $location_table | awk -F ":" '{print $1}'`
         if [ "$check_path" = "s3a" ]; then
            echo "AWS S3 Location $table"
            #to fetch the bucket name from the string
            bucket_name=`echo $location_table | awk -F'/' '{print $3F}'`
            #to fetch the path of the table
            object_path=`echo $location_table | grep -oP "$bucket_name/\K.*"`
            #recording information including bucket and object path along with database name and table name.
            echo $database $table $bucket_name $object_path >> aws_path.txt
         elif [ "$check_path" = "hdfs" ]; then
            echo "HDFS Location $table"
            #to fetch the bucket name from the string
            bucket_name=`echo $location_table | awk -F'/' '{print $3F}'`
            #to fetch the path of the table
            object_path=`echo $location_table | grep -oP "$bucket_name\K.*"`
             #recording information including bucket and object path along with database name and table name.
            echo $database $table $bucket_name $object_path >> hdfs_path.txt
        else
        #other than hdfs and S3 traeting as VIEW and recording them into a file
          echo $database $table $location_table  >> view.txt
          echo "VIEW $table"
        fi
  done < ${database}
  rm -f ${database}
done < databases.txt
rm -f databases.txt

#Below function will get all the folders and subfolders in that loacation according to the line and save info into the file individually
while read -r database table awsbucket path d; do
  echo $database.$table
  echo $awsbucket
mkdir -p $database
  aws s3api list-objects  --bucket $awsbucket --prefix $path/ --query 'Contents[].{Key: Key, Size: Size}' --output text | awk '{ print "\""$1"\""",""\""$2"\"" }' | sed "s/^/\"awss3\",\"${database}\",\"${table}\",\"${awsbucket}\",/" >> aws_all_table.txt
  RET_VAL=$?
  echo $RET_VAL
  if [ $RET_VAL -ne 0 ]; then
    echo "Job Failed on $database.$table!!"
  else
    #sed -i '1,1 d' aws_path.txt
    echo "Success to fetch files info and sizes for $database.$table !!"
  fi
done < aws_path.txt

while read -r database table hdfs_namespace path d; do
  echo $table
  hadoop fs -ls -R $path | sed 's/  */ /g' | awk '{ print "\""$8"\""",""\""$5"\"" }' | sed "s/^/\"hdfs\",\"${database}\",\"${table}\",\"${hdfs_namespace}\",/" >> hdfs_all_table.txt
  RET_VAL=$?
  echo $RET_VAL
  if [ $RET_VAL -ne 0 ]; then
    echo "Job Failed on $database$table!!"
  else
    #sed -i '1,1 d' hdfs_path.txt
    echo "Success to fetch files info and sizes for $database.$table !!"
  fi
done < hdfs_path.txt
#this copies comma seperated and double quote
cat hdfs_all_table.txt  aws_all_table.txt > all_prod_files.csv
#rm -f hdfs_path.txt aws_path.txt
