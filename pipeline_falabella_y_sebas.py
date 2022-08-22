import argparse
from datetime import datetime
import logging
import random
import os
import json
import time
from typing import Any, Dict, List

from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window
from apache_beam.io.gcp.internal.clients import bigquery

import base64
import hashlib
from Crypto import Random
from Crypto.Cipher import AES


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"x-oxygen-360101-a0fc362da30c.json"
key='test_1'

#la key debe buscarse en el key vault (secret manager)

BIGQUERY_TABLE = "x-oxygen-360101:medium.medium_test"
BIGQUERY_SCHEMA = "timestamp:TIMESTAMP,attr1:FLOAT,msg:STRING"

#information encryption
class GroupMessagesByFixedWindows(PTransform):
    """A composite transform that groups Pub/Sub messages based on publish time
    and outputs a list of tuples, each containing a message and its publish time.
    """
    def __init__(self, window_size, num_shards=5):
        # Set window size to 60 seconds.
        self.window_size = int(window_size * 60)
        self.num_shards = num_shards
    def expand(self, pcoll):
        return (
            pcoll
            # Bind window info to each element using element timestamp (or publish time).
            | "Window into fixed intervals"
            >> WindowInto(FixedWindows(self.window_size))
            | "Add timestamp to windowed elements" >> ParDo(AddTimestamp())
            # Assign a random key to each windowed element based on the number of shards.
            | "Add key" >> WithKeys(lambda _: random.randint(0, self.num_shards - 1))
            # Group windowed elements by key. All the elements in the same window must fit
            # memory for this. If not, you need to use `beam.util.BatchElements`.
            | "Group by key" >> GroupByKey()
        )
class AddTimestamp(DoFn):
    def process(self, element, publish_time=DoFn.TimestampParam):
        """Processes each windowed element by extracting the message body and its
        publish time into a tuple.
        """
        yield (
            element.decode("utf-8"),
            datetime.utcfromtimestamp(float(publish_time)).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            ),
        )
class WriteToGCS(DoFn):
    def __init__(self, output_path):
        self.output_path = output_path
    def process(self, key_value, window=DoFn.WindowParam):
        """Write messages in a batch to Google Cloud Storage."""
        ts_format = "%H:%M"
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        shard_id, batch = key_value
        filename = "-".join([self.output_path, window_start, window_end, str(shard_id)])
        with io.gcsio.GcsIO().open(filename=filename, mode="w") as f:
            for message_body, publish_time in batch:
                f.write(f"{message_body},{publish_time}\n".encode("utf-8"))

def run(input_subscription, output_path, output_table, window_interval_sec, window_size=1.0, num_shards=5, pipeline_args=None):
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    options1 = PipelineOptions(
    pipeline_args,
    runner='DataflowRunner',
    project='x-oxygen-360101',
    job_name='omega-1',
    temp_location='gs://temp-medium1/temp1',
    region='us-east1',
    service_account_email='684034867805-compute@developer.gserviceaccount.com',
    streaming = True,
    save_main_session= True,
    )
    with Pipeline(options=options1) as pipeline:
        (
            pipeline
            # Because `timestamp_attribute` is unspecified in `ReadFromPubSub`, Beam
            # binds the publish time returned by the Pub/Sub server for each message
            # to the element's timestamp parameter, accessible via `DoFn.TimestampParam`.
            # https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.pubsub.html#apache_beam.io.gcp.pubsub.ReadFromPubSub
            | "Read from Pub/Sub" >> io.ReadFromPubSub(subscription=input_subscription)
            | "Window into" >> GroupMessagesByFixedWindows(window_size, num_shards)
            | "Write to GCS" >> ParDo(WriteToGCS(output_path))

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_subscription",
        help="The Cloud Pub/Sub topic to read from."
        '"projects/<PROJECT_ID>/subscriptions/<TOPIC_ID>".',
    )
    parser.add_argument(
        "--window_size",
        type=float,
        default=1.0,
        help="Output file's window size in minutes.",
    )
    parser.add_argument(
        "--output_path",
        help="Path of the output GCS file including the prefix.",
    )
    parser.add_argument(
        "--num_shards",
        type=int,
        default=5,
        help="Number of shards to use when writing windowed elements to GCS.",
    )

    parser.add_argument(
        "--window_interval_sec",
        default=1,
        type=int,
        help="Window interval in seconds for grouping incoming messages.",
    )

    parser.add_argument(
        "--output_table",
        #default="x-oxygen-360101:falabella.Test1", 
        help="Output BigQuery table for results specified as: "
        "PROJECT:DATASET.TABLE or DATASET.TABLE.",
        default=BIGQUERY_TABLE
    )

    known_args, pipeline_args = parser.parse_known_args()

    run(
        known_args.input_subscription,
        known_args.output_path,
        known_args.output_table,
        known_args.window_interval_sec,
        known_args.window_size,
        known_args.num_shards,
        pipeline_args,
    )