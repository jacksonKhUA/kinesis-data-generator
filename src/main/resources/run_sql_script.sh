psql \
   -f export_items.sql
   # shellcheck disable=SC2215
   --host=db-aws-items-esoboliev.cejqpaiasore.us-east-1.rds.amazonaws.com \
   --port=5432 \
   --username=root \
   --password \
   --dbname=AwsCapstoneItems