import json
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import boto3
import urllib
import io
from botocore.exceptions import ClientError

s3 = boto3.client('s3')
url = "https://collect.tealiumiq.com/event"


tealium_datasource = ""

processed_bucket = ""
processed_key = ""

#copy object that is being processed
def copy_object(src_bucket_name, src_object_name,
                dest_bucket_name, dest_object_name=None):

    # Construct source bucket/object parameter
    copy_source = {'Bucket': src_bucket_name, 'Key': src_object_name}
    if dest_object_name is None:
        dest_object_name = src_object_name

    # Copy the object
    s3 = boto3.client('s3')
    try:
        s3.copy_object(CopySource=copy_source, Bucket=dest_bucket_name,
                       Key=dest_object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True
    
#get the object from the bucket    
def get_object(bucket_name, object_name):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_name)
    except:
        print("ERROR: ")
        return None
    return response['Body']

#sends data to tealium. obj=data object, a=account, p=profile
def send_to_tealium(obj, a, p):
    obj['tealium_account'] = a
    obj['tealium_profile'] = p
    data = urllib.parse.urlencode(obj).encode()
    
    req = urllib.request.Request(url, data=data)
    #req.add_header('Content-Type', 'application/json')
    
    resp = urllib.request.urlopen(req)
    print("RESPONSE: " + str(resp))
    
    return "";


#lambda event handler
def lambda_handler(event, context):
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote(event['Records'][0]['s3']['object']['key'])
    splitStr = source_bucket.split(".")
    account = splitStr[0]
    profile = splitStr[1]
 
    processed_bucket = source_bucket + "-processed"
    processed_key = key
    
    stream = get_object(source_bucket, key)
    success = copy_object(source_bucket, key, processed_bucket, processed_key)
    
    if success:
        s3.delete_object(Bucket=source_bucket, Key=key)

    if stream is not None:
        
        raw_bytes = stream.read()
        avro_bytes = io.BytesIO(raw_bytes)

        reader = DataFileReader(avro_bytes, DatumReader())
        for line in reader:
            send_to_tealium(line, account, profile)
        
    return ""


