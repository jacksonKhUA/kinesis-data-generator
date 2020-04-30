from time import sleep

import boto3

from logger import logger
from generator.kinesis_record_generator import ItemViewsGenerator, ItemReviewsGenerator, RecordGeneratorInterface
from api.kinesis_stream import KinesisStreamManager

kinesis_client = boto3.client('kinesis')


def send_data_to_kinesis(stream_name: str, generator: RecordGeneratorInterface, batch: int):
    stream_manager = KinesisStreamManager(kinesis_client)
    # safely create stream
    stream_manager.create_stream_and_wait(stream_name)

    # put batch of records to given kinesis stream using record python.generator
    put_records(stream_name, generator.get_records(batch))

    # need to delete stream_name to save money
    # stream_manager.delete_stream(stream_name)


def put_records(stream_name: str, records: list):
    logger.info(records)
    kinesis_client.put_records(StreamName=stream_name,
                               Records=records)


if __name__ == '__main__':
    while True:
        send_data_to_kinesis('AwsViewsEsoboliev', ItemViewsGenerator(), 200)
        # send_data_to_kinesis('AwsReviewsEsoboliev', ItemReviewsGenerator(), 500)
        sleep(5)
