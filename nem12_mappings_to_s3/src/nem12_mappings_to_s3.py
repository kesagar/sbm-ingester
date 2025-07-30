import os, sys, math, backoff
import aiohttp
from random import randint
from gremlin_python import statics
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.driver.protocol import GremlinServerError
from gremlin_python.driver import serializer
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.process.traversal import T
from tornado.websocket import WebSocketClosedError
from tornado import httpclient 
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import ReadOnlyCredentials
from types import SimpleNamespace
import json
import boto3

s3 = boto3.client('s3')

reconnectable_err_msgs = [ 
  'ReadOnlyViolationException',
  'Server disconnected',
  'Connection refused'
]

retriable_err_msgs = ['ConcurrentModificationException'] + reconnectable_err_msgs

network_errors = [WebSocketClosedError, OSError]

retriable_errors = [GremlinServerError] + network_errors    

    
def is_retriable_error(e):

  is_retriable = False
  err_msg = str(e)
  
  if isinstance(e, tuple(network_errors)):
    is_retriable = True
  else:
    is_retriable = any(retriable_err_msg in err_msg for retriable_err_msg in retriable_err_msgs)
  
  print('error: [{}] {}'.format(type(e), err_msg))
  print('is_retriable: {}'.format(is_retriable))
  
  return is_retriable

def is_non_retriable_error(e):    
  return not is_retriable_error(e)
    
def reset_connection_if_connection_issue(params):
  
  is_reconnectable = False

  e = sys.exc_info()[1]
  err_msg = str(e)
  
  if isinstance(e, tuple(network_errors)):
    is_reconnectable = True
  else:
    is_reconnectable = any(reconnectable_err_msg in err_msg for reconnectable_err_msg in reconnectable_err_msgs)
    
  print('is_reconnectable: {}'.format(is_reconnectable))
    
  if is_reconnectable:
    global conn
    global g
    conn.close()
    conn = create_remote_connection()
    g = create_graph_traversal_source(conn)
    
def create_graph_traversal_source(conn):
  return traversal().withRemote(conn)
  
def create_remote_connection():
  print('Creating remote connection')
  
  return DriverRemoteConnection(
    connection_string(),
    'g',
    pool_size=1,
    message_serializer=serializer.GraphSONSerializersV2d0())
  
def connection_string():
  
  database_url = 'wss://{}:{}/gremlin'.format(os.environ['neptuneEndpoint'], os.environ['neptunePort'])
  return database_url
  
conn = create_remote_connection()
g = create_graph_traversal_source(conn)
  

def lambda_handler(event, context):
  try:
    nem12Ids = {d['nem12Id'][0]: d['id'] for d in g.V().has('nem12Id').valueMap(True, 'nem12Id').toList()}
    nem12_json = json.dumps(nem12Ids, indent=2)
    bucket_name = "sbm-file-ingester"
    object_key = "nem12_mappings.json"
    s3.put_object(Bucket=bucket_name,Key=object_key,Body=nem12_json,ContentType="application/json")
    return {"statusCode": 200, "body": "Nem12 mappings successfully written to S3"}
  except Exception as e:
    return {"error":True,"errorMessage":str(e)}