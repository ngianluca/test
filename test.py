"""
* Copyright (C) Alpina Analytics, GmbH - All Rights Reserved
* Unauthorized copying of this file, via any medium is strictly prohibited
* Proprietary and confidential
"""

# pytype: skip-file

from __future__ import absolute_import

import os
import argparse
import logging
import json

from past.builtins import unicode

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.examples.wordcount_with_metrics import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

from google.cloud import pubsub_v1
from google.cloud import pubsub

from google.cloud import bigquery

# Globals
BUCKET="gs://swiftmessage-bucket"
INPUT_FILE="dummy_input4.json"
PROJECT="swiftflow-pipeline-poc"
DATASET="test_dataset"
TABLE="test_tablse"
TOPIC="SwiftMessage"
INPUT="{BUCKET}/{INPUT_FILE}"
SUBSCRIPTION="SwiftMessage-sub" # You can create a subscription and refer to it here
OUTPUT_TOPIC="projects/{PROJECT}/topics/{TOPIC}"

# Schema: mandatory for streaming

SCHEMA = {'fields': [{'name': 'foo', 'type': 'STRING', 'mode': 'NULLABLE'}]}

# Authentication

# PubSub utils

def create_topic():
    
    publisher = pubsub_v1.PublisherClient()
    topic_name = f"projects/{PROJECT}/topics/{TOPIC}"
    publisher.create_topic(topic_name)

def create_subscriber():
    
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = publisher.topic_path(PROJECT, TOPIC)
    sub_path = subscriber.subscription_path(PROJECT, SUBSCRIPTION)
    subscriber.create_subscription(name=sub_path,topic=topic_path)

def publish(msg=b'My first message!'):

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT, TOPIC)
    publisher.publish(topic_path,msg,spam='eggs')

# Beam utils

def create_random_record(line):
    
    print("The message flowing through beam :")
    print(line)
        
    try:
        
        return {"foo":line}
  
    except:
    
        return {"foo":"Could not take line"}
  

# Run

def run(argv=None, save_main_session=True):
  """Build and run the pipeline."""
  
  pipeline_options = PipelineOptions(save_main_session=True, streaming=True, runner='DataflowRunner')
  
  with beam.Pipeline(options=pipeline_options) as p:
      
    input_subscription=f"projects/{PROJECT}/subscriptions/{SUBSCRIPTION}"
    output_table=f"{PROJECT}:{DATASET}.{TABLE}"
    
    _ = (
        
        p
        | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(subscription=input_subscription).with_output_types(bytes)
        | 'UTF-8 bytes to string' >> beam.Map(lambda msg: msg.decode('utf-8'))
        #| beam.Map(print)
        | beam.Map(create_random_record)
        | 'Write to Table' >> beam.io.WriteToBigQuery(
                        output_table,
                        schema = SCHEMA,
                        custom_gcs_temp_location=BUCKET,
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                        )
        
        )

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()

