#!/usr/local/bin/python3

# this file will zip my Logic projects so I can upload to S3

# import modules
import os
import shutil
from zipfile import ZipFile
from os import path

from datetime import datetime, timedelta
from pytz import timezone

# import AWS modules
import logging
import boto3
from botocore.exceptions import ClientError

# We are already connected to S3 so the below should retrieve an object's last mod time
# some params you will need are the dir name, etc

# CREATE CLIENT AND RESOURCE FOR S3
s3Client = boto3.client('s3')
s3Resource = boto3.resource('s3')

# object for all s3 buckets
bucket_name = 'python-bucket-n' # NAME OF BUCKET GOES HERE, HARD CODED FOR NOW

# CREATE BUCKET OBJECT FOR THE BUCKET OF CHOICE
bucket = s3Resource.Bucket(bucket_name)

# Convert GMT to US/Eastern time zone
# also converts from datetime datatype to string datatype for use in dictionary
def utc_to_est(utc_dt):
    fmt = "%Y-%m-%d %H:%M:%S"
    est_time = utc_dt.astimezone(timezone('US/Eastern'))
    return est_time.strftime(fmt)

def stamp_to_epoch(timestamp):
    year = int(timestamp[0:4])
    month = int(timestamp[5:7])
    day = int(timestamp[8:10])
    hours = int(timestamp[11:13])
    mins = int(timestamp[14:16])
    sec = int(timestamp[17:19])
    epoch_seconds = int(datetime(year, month, day, hours, mins).strftime('%s'))
    return epoch_seconds

def getS3LastModEpoch(fileName):
    for item in bucket.objects.all():
        if "Logic_Projects" in item.key:
            if ".zip" in item.key:
                if fileName in item.key:
                    utc_dt = s3Resource.Object(bucket_name, item.key).last_modified
                    est_time = utc_to_est(utc_dt)
                    s3_obj_lastmod_time_epoch = stamp_to_epoch(est_time)
                    return (s3_obj_lastmod_time_epoch)



def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        with open(file_name, "rb") as f:
            s3_client.upload_fileobj(f, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def zip_file(fileName):

        # Check if file exists
        if path.exists("/" + fileName):
            # get the path to the file in the current directory
            src = path.realpath("/" + fileName)
            # rename the original file
            os.rename(fileName,fileName)
            # now put things into a ZIP archive
            root_dir,tail = path.split(src)
            shutil.make_archive(fileName,"zip",root_dir)
        # more fine-grained control over ZIP files
        with ZipFile(fileName + ".zip","w") as newzip:
            newzip.write(fileName)

def delete_zips():
    for filename in os.listdir():
        if filename.endswith('.zip'):
            os.unlink(filename)


rootDir = '/Users/Nakul/Music/Logic/Drafts/python_test/Logic_Projects'
os.chdir(rootDir)

rootDirContents = os.listdir()

def traverse_subdir(folderName):

    # change directory using absolute path
    # get current working directory because there could be many sub directories
    os.chdir(os.getcwd() + '/' + folderName)



    subdirContents = os.listdir()
    for f in subdirContents:
        # ignore .DS_Store
        if f != '.DS_Store':
            if '.logicx' in f:
                filePath = os.getcwd() + '/' + f
                local_obj_lastmod_time_epoch = os.path.getmtime(filePath)

                # compare time of local mod with that of s3 mod for same file
                # retrieve certain value from dictionary if the file name matches
                # local file name will simply be .logicx
                # s3 key will be with the folder name and the zip
                s3_obj_lastmod_time_epoch = getS3LastModEpoch(f)


                if getS3LastModEpoch(f) is None:
                    # upload the file
                    zip_file(f)
                    upload_file(
                        os.getcwd() + '/' + f + '.zip',
                        'python-bucket-n', 'Logic_Projects/' + f + '.zip')
                    delete_zips()
                else:
                    if local_obj_lastmod_time_epoch > s3_obj_lastmod_time_epoch:

                        zip_file(f)

                        upload_file(
                            os.getcwd() + '/' + f + '.zip',
                            'python-bucket-n', 'Logic_Projects/' + f + '.zip')
                        delete_zips()

            elif '.logicx'not in f:
                traverse_subdir(f)

def traverse_rootdir():
    for file in rootDirContents:

        # navigate to root dir to crawl through
        os.chdir(rootDir)

        # Do not do anything with the .DS_Store file
        if file != '.DS_Store':


            if '.logicx' not in file:

                # assign variable to clarify that file is actually a folder
                folderName = file

                # call subdirectory crawler function
                traverse_subdir(folderName)

            # if file is actually a file, then do time comparisons
            else:
                # define filepath so we can get mod time using os module
                filePath = rootDir + '/' + file
                local_obj_lastmod_time_epoch = os.path.getmtime(filePath)

                # compare time of local mod with that of s3 mod for same file
                # retrieve certain value from dictionary if the file name matches
                # local file name will simply be .logicx
                # s3 key will be with the folder name and the zip
                s3_obj_lastmod_time_epoch = getS3LastModEpoch(file)

                # if file does not exist on S3
                if getS3LastModEpoch(file) is None:
                    # upload the file
                    zip_file(file)
                    upload_file(
                        os.getcwd() + '/' + file + '.zip',
                        'python-bucket-n', 'Logic_Projects/' + file + '.zip')
                    delete_zips()

                else:
                    if local_obj_lastmod_time_epoch > s3_obj_lastmod_time_epoch:

                        zip_file(file)
                        upload_file(
                            os.getcwd() + '/' + file + '.zip',
                            'python-bucket-n', 'Logic_Projects/' + file + '.zip')
                        delete_zips()


def main():
    delete_zips()
    traverse_rootdir()

if __name__ == '__main__':
    main()
