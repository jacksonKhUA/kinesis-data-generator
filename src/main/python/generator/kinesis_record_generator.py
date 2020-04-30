import json
import time
from random import randint


class RecordGeneratorInterface:
    TOTAL_ITEMS_NUMBER: int = 50

    def __init__(self, partition_key: str):
        self.item_number: int = 0
        self.partition_key: str = partition_key

    def generate_record(self) -> dict:
        self.item_number = randint(1, self.TOTAL_ITEMS_NUMBER)
        return {
            'item_id': f"ItemId-{self.item_number}",
            'timestamp': round(time.time()),
            'device_type': f"DeviceType-{self.item_number}",
            'device_id': f"DeviceId-{self.item_number}",
            # we need to have low range due to availability to have repeated IP's
            'user_ip': f'192.168.01.{randint(0, 255)}'
        }

    def get_record(self) -> dict:
        record = self.generate_record()
        data = json.dumps(record) + ',\n'
        return {
            'Data': bytes(data, 'utf-8'),
            'PartitionKey': self.partition_key
        }

    def get_records(self, batch) -> list:
        return [self.get_record() for _ in range(batch)]


class ItemViewsGenerator(RecordGeneratorInterface):

    def __init__(self):
        super().__init__('ItemViewPartitionKey')


class ItemReviewsGenerator(RecordGeneratorInterface):

    def __init__(self):
        super().__init__('ItemReviewPartitionKey')

    def generate_record(self) -> dict:
        items = super().generate_record()
        items.update({
            'review_title': f"Random title for device number {self.item_number}",
            'review_text': f"Random text for device number {self.item_number}",
            'review_stars': randint(1, 5)
        })
        return items
