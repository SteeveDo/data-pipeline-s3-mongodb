from conf import conf
import boto3
from pymongo import MongoClient
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import os
import requests

load_dotenv()

uri=os.getenv("URI") #uri to mongodb database
mongodb_bdd=os.getenv("MONGO_BDD") #Name of the mongodb database

accessKeyId=os.getenv("ACCESSKEYID") #access key AWS
secretAccessKey=os.getenv("SECRETACCESSKEY") #Secret access key AWS
bucket_name=os.getenv("BUCKET_NAME") #bucket name in s3

def connect_to_s3(accessKeyId=accessKeyId,secretAccessKey=secretAccessKey):

    s3Client= boto3.client('s3',
                aws_access_key_id=accessKeyId,
                aws_secret_access_key= secretAccessKey)
    return s3Client

def connect_to_mongodb(uri=uri,mongodb_bdd=mongodb_bdd):

    #Connexion à la base Mongodb
    client = MongoClient(uri)
    #Selection de la base
    mydb=client[mongodb_bdd]

    return mydb

def create_spark_session(accessKeyId=accessKeyId,secretAccessKey=secretAccessKey):
    spark = SparkSession.builder.appName('PySpark_app').getOrCreate()

    # Replace Key with your AWS account key (You can find this on IAM 
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", accessKeyId)
    
    # Replace Key with your AWS secret key (You can find this on IAM 
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secretAccessKey)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

    return spark
def check_connection():
    """
    Check if the computer have internet access
    """
    
    try:
        requests.get("http://www.google.com")
        return {
            "message":"Online",
            "status":True
        }
    except:
        return {
            "message":"Offline",
            "status":False
        }



#s3Client.put_object(Body=open(path_export, 'rb'), Bucket=bucket_name, Key=key_file)