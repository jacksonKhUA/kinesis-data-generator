import os
from random import randint

from python.logger import logger


def write_query_to_file(batch: int):
    query: str = """
                    insert into items
                    values ('ItemId-{item_id}',
                            'title for item {item_id}',
                            'description for item {item_id}',
                            'Category{category_id}');
                """.rstrip()
    logger.info(f"Query template is: {query}")
    with open(os.path.join(os.path.dirname(__file__), '../../resources/export_items.sql'), "w") as myfile:
        for _ in range(1, batch):
            myfile.write(query.format(item_id=randint(1, 100), category_id=randint(1, 30)))
        logger.info(f"Queries were generated for {batch} rows.")


if __name__ == '__main__':
    write_query_to_file(50000)
