
import json
from conf import conf
import pandas as pd
import os
#from schema import mongo_schema
from pyspark import *
from connexion_functions import *
from datetime import datetime as dt
from pyspark.sql import SparkSession


spark=create_spark_session()
client=connect_to_s3()
mydb=connect_to_mongodb()

def Provisioning_mongo(mydb=mydb):
    
    data_path= os.getenv("LOCATION_DATA_MONGO")
    
    files= os.listdir(data_path)

    for file in files:
        file_name=file.split(".")[0]
        mycollection=mydb[file_name]
        #schema=mongo_schema[file_name]

        f=data_path+f"/{file}"
        
        df=pd.read_csv(f,sep=",",header=0,encoding = "utf-8")

        data = df.to_dict('records')
        #print()
        #Count docs before inserting
        n_docs_before=mycollection.count_documents({})

        #Ecriture dans la base Mongodb
        mycollection.insert_many(data)

        #Count docs after inserting
        n_docs_after=mycollection.count_documents({})

    #Fermeture de la connexion
    client.close()

    if(n_docs_after==n_docs_before):
        return(
            {
                "Message":"No document added in Mongodb"
            })
    else:
        return(
            {
                "Message":f"{(n_docs_after-n_docs_before)} documents added in {mycollection}'s collection"
            })

def merge_db_to_s3(mydb=mydb,s3Client=client,n=100):

    print("Database export processing...")
    if len(list(mydb.list_collection_names()))<1:

        return {
            "message":"No data to upload",
            "status":False
        }

    else:

        for collection in mydb.list_collection_names():
            query={}
            filter={'_id':0}
            data=mydb[collection].find(query,filter).limit(n)
            #df_rdd=sc.parallelize(list(data))

            df=pd.DataFrame(list(data))
            
            filename="Temp Data/Temp_file_" +str(dt.now().day)+".csv"

            df.to_csv(filename)
            #df_rdd.saveAsTextFile(filename)
            
            key_file="Mongodb/"+collection.capitalize()+"/content.csv"
#Add a Checkpoint to see if data are already in S3
            s3Client.put_object(Body=open(filename, 'rb'), Bucket=bucket_name, Key=key_file)

            print(f" {collection} collection uploaded ")

            os.remove(filename)

        #La Récupération des données se fait sans problème. Il faut compléter le script avec l'insertion du rdd dans s3
        return {
            "message":"Successfully uploaded database data to s3",
            "status":True
        }

def merge_file_to_s3(source,destination,s3Client=client,bucket_name=bucket_name):
    """
    Upload a file from local storage to s3
    source: path to the file to upload
    destination: path of where to store the file in s3
    s3Client: connection object to s3
    bucket_name: name of the bucket targeted

    return a dictionnary with a status message
    """
    try:
        f=open(source,'rb')

        s3Client.put_object(Body=f, Bucket=bucket_name, Key=destination)

        try:
            s3Client.get_object(Bucket=bucket_name, Key=destination)

            return {
                    "message":"Your file has been successfully uploaded",
                    "status":True
            }
        except:

            return{
                    "message":"An error occured while uploading the file",
                    "status":False
            }
    except:
        return{
                "message":"An error occured while reading the file",
                "status":False
        }

def fetch_from_s3(filepath,destination,s3Client=client,bucket_name=bucket_name):
    
    try:

        s3Client.download_file(Bucket=bucket_name, Key=filepath, Filename=destination)

        return {
            "message":"Downloaded file",
            "status":True
        }
    except:

        return{
            "message":"File or path not found",
            "status":False
        }

def top_ten_most_viewed_movies(spark=spark,mydb=mydb,s3Client=client,bucket_name=bucket_name):
    
    if(check_connection()["status"]):
        #f=s3Client.get_object(Bucket=bucket_name, Key="Mongodb/Movies/content.csv")
        Key="Mongodb/Movies/content.csv"
        df_rdd=spark.read.options(header=True,delimiter=";",inferSchema="true").csv("10K_Lending_Club_Loans_pres.csv")
        #f"s3://{bucket_name}/{Key}"
        print(df_rdd.printSchema())
        #ERREUR DE LECTURE AVEC RDD PYSPARK
        pass

    else:

        pass