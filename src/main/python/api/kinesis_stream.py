from time import sleep

from logger import logger


class KinesisStreamManager(object):

    def __init__(self, client):
        self.client = client

    def create_stream_and_wait(self,
                               stream_name: str,
                               time_for_wait: int = 6,
                               retries: int = 15,
                               to_be_created: bool = True,
                               shard_count: int = 1):
        if not self.is_stream_exists(stream_name, ['ACTIVE'],
                                     to_be_created=to_be_created,
                                     shard_count=shard_count):
            if retries == 0:
                raise TimeoutError(f"Stream wasn't created after {time_for_wait * retries} seconds.")
            sleep(time_for_wait)
            self.create_stream_and_wait(stream_name, time_for_wait, retries - 1, to_be_created, shard_count)

    def is_stream_exists(self,
                         stream_name: str,
                         statuses: list,
                         to_be_created=True,
                         shard_count: int = 1) -> bool:
        curr_status = self.get_stream_status(stream_name=stream_name)
        logger.info(f"{stream_name} stream is in {curr_status} status...")
        if not curr_status and to_be_created:
            if not to_be_created:
                raise ValueError(f"{stream_name} stream doesn't not exist. "
                                 "Please enable 'to_be_created' attribute to create stream ")
            self.client.create_stream(StreamName=stream_name,
                                      ShardCount=shard_count)
            logger.info(f"{stream_name} stream is created")
        return len(list(filter(lambda status: status == curr_status, statuses))) != 0

    def get_stream_status(self,
                          stream_name: str) -> str or None:
        if stream_name in self.client.list_streams(Limit=100).get('StreamNames'):
            response = self.client.describe_stream_summary(StreamName=stream_name)
            if response.get('StreamDescriptionSummary'):
                return response.get('StreamDescriptionSummary').get('StreamStatus')
        return None

    def delete_stream(self,
                      stream_name: str,
                      enforce_deletion: bool = True):
        if self.get_stream_status(stream_name) in ['CREATING', 'ACTIVE', 'UPDATING']:
            self.client.delete_stream(StreamName=stream_name,
                                      EnforceConsumerDeletion=enforce_deletion)
            logger.info(f"{stream_name} stream is deleted")
