import time

import boto3
from botocore.exceptions import ClientError

from python.logger import logger

rds = boto3.client('rds')


def main():
    db_identifier = 'db-aws-items-esoboliev'
    try:
        rds.create_db_instance(DBInstanceIdentifier=db_identifier,
                               AllocatedStorage=20,
                               DBName='AwsCapstoneItems',
                               Engine='postgres',
                               MasterUsername='root',
                               MasterUserPassword='root1234',
                               DBInstanceClass='db.t2.micro')
        logger('Starting RDS instance with ID: %s' % db_identifier)
    except ClientError as e:
        if 'DBInstanceAlreadyExists' in e.response:
            logger('DB instance %s exists already, continuing to poll ...' % db_identifier)
        else:
            raise

    running = True
    while running:
        response = rds.describe_db_instances(DBInstanceIdentifier=db_identifier)
        db_instance = response['DBInstances'][0]
        status = db_instance['DBInstanceStatus']
        logger('Last DB status: %s' % status)

        if status == 'available':
            logger('DB instance ready with host: %s' % db_instance['Endpoint']['Address'])
            running = False
        time.sleep(5)


if __name__ == '__main__':
    main()
