from modules.nemreader import output_as_data_frames
import os
import os.path
import csv
import pandas as pd
import json
import numpy as np
import datetime as dt
import boto3
from pathlib import Path
import shutil
import time
import logging
import sys
import traceback
from modules.nonNemParserFuncs import *
import requests
import random
from modules.common import CloudWatchLogger, BUCKET_NAME
import uuid
from botocore.exceptions import ClientError
import modules.common as common
import tempfile
from urllib.parse import unquote


execution_log = CloudWatchLogger(common.EXECUTION_LOG_GROUP)
error_log = CloudWatchLogger(common.ERROR_LOG_GROUP)
runtime_error_log = CloudWatchLogger(common.RUNTIME_ERROR_LOG_GROUP)
metrics_log = CloudWatchLogger(common.METRICS_LOG_GROUP)

s3_resource = boto3.resource("s3")



def read_nem12_mappings(bucket_name: str, object_key: str = "nem12_mappings.json") -> dict:
    try:
        obj = s3_resource.Object(bucket_name, object_key)
        content = obj.get()["Body"].read().decode("utf-8")
        return json.loads(content)
    except Exception as e:
        error_log.log(f"Failed to read NEM12 mappings from {bucket_name}/{object_key}: {e}")
        return None

def download_files_to_tmp(file_list, tmp_files_folder_path):
    local_paths = []

    for f in file_list:
        bucket = f["bucket"]

        # Always decode key before using with boto3
        key = unquote(f["file_name"].replace("+", "%20"))
        

        file_name = os.path.basename(key)
        local_path = os.path.join(tmp_files_folder_path, file_name)

        execution_log.log(f"Downloading s3://{bucket}/{key} -> {local_path}")

        try:
            s3_resource.Bucket(bucket).download_file(key, local_path)
            local_paths.append(local_path)

        except Exception as e:
            error_log.log(
                f"Downloading {key} Failed. File Potentially already processed. Error: {e}"
            )
            continue
    return local_paths


def move_s3_file(bucket_name: str, source_key: str, dest_prefix: str):
    source_key = unquote(source_key.replace("+", "%20"))
    file_name = source_key.split("/")[-1]

    source_key = f"newTBP/{file_name}"
    dest_key = f"{dest_prefix.rstrip('/')}/{file_name}"

    try:
        bucket = s3_resource.Bucket(bucket_name)

        copy_source = {"Bucket": bucket_name, "Key": source_key}
        bucket.Object(dest_key).copy(copy_source)

        bucket.Object(source_key).delete()

        return dest_key

    except Exception as e:
        error_log.log(
                f"Moving {source_key} -> {dest_key} Failed. File Potentially already processed. Error: {e}"
            )

def createIsoTimestamp(year,month,day,hour,minute,second):
    return "'" + str(year).zfill(4)+ "-" + str(month).zfill(2) + "-" + str(day).zfill(2) + "T" + str(hour).zfill(2) + ":" + str(minute).zfill(2) + ":" + str(second).zfill(2)+"'"

def dailyInitializeMetricsDict(metricsDict, key):
    if(not key in metricsDict):
        metricsDict[key] = {"calculatedTotalFilesCount":0, "ftpFilesCount":0, "calculatedEmailFilesCount":0, "validProcessedFilesCount":0, "parseErrFilesCount":0, "irrevFilesCount":0, "totalMonitorPointsCount":0, "processedMonitorPointsCount":0, "errorExecutionCount":0}

def metricsDictPopulateValues(metricsDict, key, ftpFilesCount, validProcessedFilesCount, parseErrFilesCount, irrevFilesCount, totalMonitorPointsCount, processedMonitorPointsCount, errorExecutionCount):
    dailyInitializeMetricsDict(metricsDict, key)    
    metricsDict[key]["validProcessedFilesCount"] = metricsDict[key]["validProcessedFilesCount"] + validProcessedFilesCount
    metricsDict[key]["ftpFilesCount"] = metricsDict[key]["ftpFilesCount"] + ftpFilesCount
    metricsDict[key]["parseErrFilesCount"] = metricsDict[key]["parseErrFilesCount"] + parseErrFilesCount
    metricsDict[key]["irrevFilesCount"] = metricsDict[key]["irrevFilesCount"] + irrevFilesCount
    metricsDict[key]["totalMonitorPointsCount"] = max(metricsDict[key]["totalMonitorPointsCount"], totalMonitorPointsCount)
    metricsDict[key]["processedMonitorPointsCount"] = metricsDict[key]["processedMonitorPointsCount"] + processedMonitorPointsCount
    metricsDict[key]["calculatedTotalFilesCount"] = metricsDict[key]["parseErrFilesCount"] + metricsDict[key]["irrevFilesCount"] + metricsDict[key]["validProcessedFilesCount"]
    metricsDict[key]["calculatedEmailFilesCount"] = metricsDict[key]["calculatedTotalFilesCount"] - metricsDict[key]["ftpFilesCount"]
    metricsDict[key]["errorExecutionCount"] = metricsDict[key]["errorExecutionCount"] + errorExecutionCount
                                          
def getNem12Unit(df):
    cols = []
    for col in df.columns:
        if(col !=df.index.name):
            cols.append(col)
        
    if(len(cols) != 1):
        print(cols)
        return -1
        
    else:
        return str.lower(cols[0].split("_")[1])
                                                
def parseAndWriteData(tbp_files=None):
    tmp_dir = tempfile.gettempdir()
    tmp_files_folder_name = str(uuid.uuid4())
    tmp_files_folder_path = os.path.join(tmp_dir, tmp_files_folder_name)
    os.makedirs(tmp_files_folder_path, exist_ok=True)
    try:
        
        execution_log.log("Script Started Running at: " + pd.Timestamp.now().tz_localize('UTC').tz_convert('Australia/Sydney').isoformat())
        processingStartTime = pd.Timestamp.now().tz_localize('UTC').tz_convert('Australia/Sydney').isoformat()
        execution_log
        timestampNow = pd.Timestamp.now().tz_localize('UTC').tz_convert('Australia/Sydney').isoformat()
        metricsFileKey = timestampNow.split("T")[0] + "D"

        logsDict = {}
        metricsDict = {}

        nem12_mappings = read_nem12_mappings(BUCKET_NAME)

        if nem12_mappings is None:
            raise Exception("Failed to read NEM12 mappings from S3.")

        download_files_to_tmp(tbp_files, tmp_files_folder_path)
        
        validProcessedFilesCount = 0
        irrevFilesCount = 0
        parseErrFilesCount = 0
        processedMonitorPointsCount = 0
        totalMonitorPointsCount = 0
        ftpFilesCount = 0
        
        nmiDataStreamSuffix = ["A","B","C","D","E","F","J","K","L","P","Q","R","S","T","U","G","H","Y","M","W","V","Z"]
        nmiDataStreamChannel = ["1","2","3","4","5","6","7","8","9","A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"]

        nmiDataStreamCombinedSuffix = []
        
        c = 0
        fileSize = 1
        
        neptuneIds = []

        for i in nmiDataStreamSuffix:
            for j in nmiDataStreamChannel:
                nmiDataStreamCombinedSuffix.append(i+j)
        toBeProcessedFiles = os.listdir(tmp_files_folder_path)
        for fileName in toBeProcessedFiles:
            fileName = tmp_files_folder_path + "/" + fileName
            c = c + 1
            processingDict = []            
            dfs = None
            try:
                dfs = output_as_data_frames(fileName, common.PARSE_ERROR_LOG_GROUP)
            except:
                try:
                    dfs = nonNemParsersGetDf(fileName, common.PARSE_ERROR_LOG_GROUP)
                except:
                    logsDict["Bad File: " + fileName] = "[" + timestampNow + "]"
                    move_s3_file(BUCKET_NAME, fileName, common.PARSE_ERR_DIR)
                    parseErrFilesCount = parseErrFilesCount + 1
                    continue
                    
            for df in dfs:
                bufferNMI, bufferDF = df
                for reqCol in filter(lambda x: x.split("_")[0] in nmiDataStreamCombinedSuffix, bufferDF):
                    monitorPointName = bufferNMI + "-" + reqCol.split("_")[0]
                    nemDataType = reqCol.split("_")[0][0]

                    fieldNames = ['t_start', reqCol]
                    if 't_start' not in bufferDF.columns and bufferDF.index.name == 't_start':
                        bufferDF = bufferDF.reset_index()
                    eachBufferDF = bufferDF[fieldNames].copy()

                    # Ensure proper datetime index
                    eachBufferDF = eachBufferDF.set_index('t_start')
                    eachBufferDF.index.name = 't_start'

                    nem12UnitName = getNem12Unit(eachBufferDF)

                    neptuneId = nem12_mappings.get(monitorPointName,None)
                    neptuneIds.append(neptuneId)
                    if neptuneId is not None:
                        gems2BufferDF = bufferDF[fieldNames].copy()
                        gems2BufferDF['sensorId'] = neptuneId
                        gems2BufferDF['unit'] = nem12UnitName
                        gems2BufferDF = gems2BufferDF.rename(columns={"t_start": "ts", reqCol: "val"})
                        gems2BufferDF['its'] = gems2BufferDF['ts']
                        gems2BufferDF = gems2BufferDF[['sensorId', 'ts', 'val', 'unit', 'its']]

                        # Format timestamps properly
                        gems2BufferDF['ts'] = gems2BufferDF['ts'].dt.strftime('%Y-%m-%d %H:%M:%S')
                        gems2BufferDF['its'] = gems2BufferDF['its'].dt.strftime('%Y-%m-%d %H:%M:%S')
                        s3_resource.Object(
                            "hudibucketsrc",
                            "sensorDataFiles/"
                            + monitorPointName
                            + pd.Timestamp.now().strftime('%Y_%b_%dT%H_%M_%S_%f')
                            + str(random.randint(1, 1000000))
                            + ".csv"
                        ).put(Body=gems2BufferDF.to_csv(index=False))

                        processedMonitorPointsCount += 1
            neptuneIds = [x for x in neptuneIds if x is not None]
            if(len(neptuneIds)!=0):
                totalMonitorPointsCount = totalMonitorPointsCount + len(neptuneIds)
                move_s3_file(BUCKET_NAME, fileName, common.PROCESSED_DIR)
                validProcessedFilesCount = validProcessedFilesCount + 1
            
            else:
                move_s3_file(BUCKET_NAME, fileName, common.IRREVFILES_DIR)
                irrevFilesCount = irrevFilesCount + 1
        
        for key,value in logsDict.items():
            runtime_error_log.log(key + " at " + value)
        metricsDictPopulateValues(metricsDict, metricsFileKey, ftpFilesCount, validProcessedFilesCount, parseErrFilesCount, irrevFilesCount, totalMonitorPointsCount, processedMonitorPointsCount, 0)
        metrics_log.log(json.dumps(metricsDict[metricsFileKey]))        
        processingEndTime = pd.Timestamp.now().tz_localize('UTC').tz_convert('Australia/Sydney').isoformat()
        execution_log.log("Script Finished Running at: " + processingEndTime)
        shutil.rmtree(tmp_files_folder_path, ignore_errors=True)
        
        
        return 1
        
    except Exception as e:
        err = traceback.format_exc()
        error_log.log("Script Failed with Error: " + str(e) + "\n" + err)
        metricsDictPopulateValues(metricsDict, metricsFileKey, 0, 0, 0, 0, 0, 0, 1)
        metrics_log.log(json.dumps(metricsDict[metricsFileKey]))
        shutil.rmtree(tmp_files_folder_path, ignore_errors=True)
    
def lambda_handler(event, context):
    tbp_files = []
    for record in event["Records"]:
        try:
            message_body = json.loads(record["body"])
            s3_event = message_body["Records"][0]

            bucket_name = s3_event["s3"]["bucket"]["name"]
            file_name   = s3_event["s3"]["object"]["key"]
            tbp_files.append({
                "bucket": bucket_name,
                "file_name": file_name,
            })

            parseAndWriteData(tbp_files)

        except Exception as e:
            error_log.log(f"Error processing record: {e}")
            continue

    return {
        "statusCode": 200,
        "body": "Successfully processed files."
    }

    return {
        "statusCode": 200,
        "body": "Successfully processed files."
    }
