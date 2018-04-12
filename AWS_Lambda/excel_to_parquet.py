
import boto3
import os
import datetime
import pandas as pd
import csv
import sys
import re
import pyarrow.parquet as pq


aws_temp_file_path="/tmp/" #temporary aws folder allocated to work with files 500MB is the limit
input_file_path = 'input/'
ready_directory = 'ready/'
arc_directory = 'processed/'
fail_directory = 'fail/'
tbls_directory='forecast/'
tbl_bucket = 'input_bucket'
data_bucket = 'output_bucket'
file_ext = '.xlsx' #extension of file you want to convert
encryption = "AES256" #encrytption of file you saving in S3
excel_column_alphabet="D" #How many column you want to convert or limit.
parquet_columns=["A", "B", "C", "D"] #paquet  column names

inset_column = '{:%Y-%m-%d}'.format(datetime.datetime.now())



xl_sheet_name = object() #this is default  and will help when checking numnber of sheets for looping and processing.
s3 = boto3.resource('s3')
client = boto3.client('s3')
src_bucket = s3.Bucket(data_bucket)

#this is just the entry point for lambda which simply checks for xlsx files and loop them into the function sheet_check_name
def main(event, context):
    print("Job Called!" )
    for obj in src_bucket.objects.filter(Prefix=input_file_path):
        file_path = obj.key
        path, filename = os.path.split(obj.key) #takes the path and splits object and key i.e folder and file
        if file_path.endswith(file_ext):
            try:
                sheet_check_name(file_path, filename)#calling function by passing arguments file path and name
                print(filename + " Converting Success!")
            except Exception as e:
                print(e)
                print(filename + " Converting Failed on function lambda_handler!")
                sys.exit(1)


#function that sets all the fiolder names from the inputs for copying files into s3 and deleting them. Calls actual function which converts excel to CSV and Parquet excel_to_csv
def sheet_check_name(file_path,filename):
    time_tag = '{:%Y%m%d_%H%M%S}'.format(datetime.datetime.now())
    print(time_tag + " Job Called!")
    arc_file_name = time_tag + '_' + filename
    arc_file_path = os.path.join(arc_directory, arc_file_name)
    src_file_full_path = os.path.join(data_bucket, file_path)
    temp_exl_file = os.path.join(aws_temp_file_path, filename)

    fail_file_name = filename
    fail_file_path = os.path.join(fail_directory, filename)

    client.download_file(data_bucket, file_path, temp_exl_file)
    # data_load = open(temp_exl_file, 'rb')
    xl = pd.ExcelFile(temp_exl_file)
    sheets = xl.book.sheets()
    if len(sheets) > 1: #will check if their are multiple sheets in work book if so will get them processes accordingly.
        for sheet in sheets:
            sheet_name = sheet.name
            data_check = pd.read_excel(xl, sheet_name=sheet_name)
            if sheet.visibility != 1 and data_check.empty is False:# checks for hided sheets and empty sheets
                sheet = sheet.name
                print(sheet)
                try:#this is looping and converting each sheet into CSV
                    excel_to_csv(xl,filename,sheet) #calling function to convert in this case if it has multiple sheets.
                except Exception as e:
                    print(e)
                    print(filename + " Converting Failed on Function excel_to_csv!")
                    sys.exit(1)
            else:
                print("No data in sheet " +sheet_name , filename )
                pass
    else:#here this is calling one sheet excel
        data_check = pd.read_excel(xl)
        if data_check.empty is False:
            try:
                excel_to_csv(xl, filename)
            except Exception as e:
                print(filename + " Converting Failed on Function excel_to_csv!")
                print(e)
                sys.exit(1)
        else:
            print("No data in Excel " + filename)
    s3.Object(data_bucket, arc_file_path).copy_from(CopySource=src_file_full_path, ServerSideEncryption=encryption)#Copies Source excel into archive directory.
    s3.Object(data_bucket, file_path).delete() #deletes file from source folder
    # os.remove(filename)


#this function will convert excel into CSV and into parquet asnd place them in folders requested.
def excel_to_csv(xl,filename,sheet=xl_sheet_name):
    try:
        if sheet is not xl_sheet_name :#above looped sheets names will passed in this and converted
            sheet_name = re.sub(r'[^a-zA-Z0-9]', '', filename).lower()
            #sheet_name = re.sub(r'[^a-zA-Z0-9]', '', sheet).lower()
            df = pd.read_excel(xl, sheet_name=sheet, options={'encoding': 'utf-8'}, index_col=False, usecols='A:'+excel_column_alphabet , skiprows=[0],header=None, names=parquet_columns, dtype=object)
            df['insert_date'] = (inset_column)
            csv_file_name = os.path.splitext(os.path.basename(sheet_name))[0]
        else:#above called single sheet excel converted
            df = pd.read_excel(xl, options={'encoding': 'utf-8'}, index_col=False, usecols='A:'+excel_column_alphabet , skiprows=[0],header=None, names=parquet_columns, dtype=object)
            df['insert_date'] = (inset_column)
            file_name = os.path.splitext(os.path.basename(filename))[0]
            csv_file_name = re.sub(r'[^a-zA-Z0-9]', '', file_name).lower()
        dh = df.dropna(axis='rows', how='all')  # Delete if any empty rows
        filtered_data = dh.where(pd.notnull(df), None)  # replace Nan with NONE
        #generates folder names and copies csv into the folder
        temp_csv_file = os.path.join(aws_temp_file_path, csv_file_name)
        csv_file_path = os.path.join(arc_directory, csv_file_name) + ".csv"
        filtered_data.to_csv(temp_csv_file, index=False, quoting=csv.QUOTE_ALL)
        csv_data_load = open(temp_csv_file, 'rb')
        s3.Bucket(data_bucket).put_object(Key=csv_file_path, Body=csv_data_load, ServerSideEncryption=encryption)

        # generates folder names and copies parquet into the folder
        temp_par_file = os.path.join(aws_temp_file_path, csv_file_name) + ".snappy.parquet"
        par_file_path = os.path.join(tbls_directory, csv_file_name) + ".snappy.parquet"
        csv_file = pd.read_csv(temp_csv_file, quoting=csv.QUOTE_ALL, dtype=object)
        csv_file.to_parquet(temp_par_file, engine='pyarrow', compression='snappy')
        par_data_load = open(temp_par_file, 'rb')
        s3.Bucket(tbl_bucket).put_object(Key=par_file_path, Body=par_data_load, ServerSideEncryption=encryption)
        os.remove(temp_par_file)#removes parquet file in tmp folder
        os.remove(temp_csv_file)#removes csv file in tmp folder
    except exception as e:
            print(filename + " Converting Failed in function excel_to_csv!")
            print(e)
            sys.exit(1)
