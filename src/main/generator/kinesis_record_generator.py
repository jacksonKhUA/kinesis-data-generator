import json
import time
from random import randint


class RecordGeneratorInterface:
    TOTAL_ITEMS_NUMBER: int = 50

    def __init__(self, partition_key: str):
        self.item_number: int = randint(1, self.TOTAL_ITEMS_NUMBER + 1)
        self.partition_key: str = partition_key

    def generate_record(self) -> dict:
        return {
            'item_id': f"ItemId-{self.item_number}",
            'timestamp': round(time.time()),
            'device_type': f"DeviceType-{self.item_number}",
            'device_id': f"DeviceId-{self.item_number}",
            'user_ip': '.'.join([str(randint(0, 255)) for _ in range(4)])
        }

    def get_record(self) -> dict:
        record = self.generate_record()
        data = json.dumps(record)
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
            'review_stars': randint(1, 5 + 1)
        })
        return items
