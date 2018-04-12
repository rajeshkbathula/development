import os
import datetime
import boto3
import pandas as pd
from io import BytesIO
import csv
import sys



time_tag='{:%Y%m%d_%H%M%S}'.format(datetime.datetime.now())

#s3 resource boto3 variables
data_bucket=sys.argv[1]
encryption="AES256" #encryption of files
file_ext='.xlsx' #to get only excel files
csv_ext='.csv' #to get only excel files
colmns_limit="D"

#data_bucket='input_bucket'
input_file_path='input/'
ready_directory='ready/'
arc_directory='processed/'
fail_directory='fail/'
s3 = boto3.resource('s3')
client = boto3.client('s3')
src_bucket=s3.Bucket(data_bucket)

print(time_tag+" Job Called!")

#excel check to go ahead if only Excel exist
check_excel=[]
for obj in src_bucket.objects.filter(Prefix=input_file_path) :
    key = obj.key
    if key.endswith(file_ext):
        check_excel.append(key)


#function that converts excel to CSV
#considering muller only 3 columns and sheet name "Unpivoted"
#ignoring medina first line as not required
#all excel's are converted considering 4 columns
def excel_to_csv(file_path):
    path, filename = os.path.split(obj.key)
    print(filename)
    csv_file_name = os.path.splitext(os.path.basename(filename))[0]  # extracting file name with out extension
    csv_file_path = os.path.join(ready_directory, csv_file_name) + ".csv"
    arc_file_name = time_tag + '_' + filename
    fail_file_name=filename
    fail_file_path= os.path.join(fail_directory, filename)
    arc_file_path = os.path.join(arc_directory, arc_file_name)
    src_file_full_path = os.path.join(data_bucket, file_path)
    csv_obj = client.get_object(Bucket=data_bucket, Key=file_path)
    excel_file = csv_obj['Body'].read()
    df = pd.read_excel(BytesIO(excel_file), options={'encoding': 'utf-8'}, index_col=False,
                           parse_cols="A:"+colmns_limit, dtype=object)
    dh = df.dropna(axis='rows', how='all')  # Delete if any empty rows
    filtered_data = dh.where(pd.notnull(df), None)  # replace Nan with NONE
    filtered_data.to_csv(filename, index=False, quoting=csv.QUOTE_ALL) # quating all
    data_load = open(filename, 'rb')
    s3.Bucket(data_bucket).put_object(Key=csv_file_path, Body=data_load, ServerSideEncryption=encryption)
    s3.Object(data_bucket, arc_file_path).copy_from(CopySource=src_file_full_path,
                                                    ServerSideEncryption=encryption)
    s3.Object(data_bucket, file_path).delete()
    os.remove(filename)

#checks function if excel are exist and then go ahead with conversion
if len(check_excel)>0:
    print("converting " + str(len(check_excel))  + " Excels into CSV!")
    for obj in src_bucket.objects.filter(Prefix=input_file_path):
        file_path = obj.key
        if file_path.endswith(file_ext):
            try:
                excel_to_csv(file_path)
            except Exception as e:
                print("Converting Excel to CSV Failed!")
                print(e)
                path, filename = os.path.split(obj.key)
                print(filename)
                fail_file_name = filename
                fail_file_path = os.path.join(fail_directory, filename)
                src_file_full_path = os.path.join(data_bucket, file_path)
                s3.Object(data_bucket, fail_file_path).copy_from(CopySource=src_file_full_path, ServerSideEncryption=encryption)
                s3.Object(data_bucket, file_path).delete()

else:
    print("No excel Exist!")


check_csv = []
for obj in src_bucket.objects.filter(Prefix=ready_directory):
    key = obj.key
    if key.endswith(csv_ext):
        check_csv.append(key)

num = len(check_csv)
if len(check_csv) > 0:
    print(num,"CSV's Available ")
else:
    print("No valid sheets in Excel")
