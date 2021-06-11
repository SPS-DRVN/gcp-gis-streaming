from google.cloud import bigquery
import base64, json, sys, os
import pandas as pd

def pubsub_to_bigq(event, context):
   message_list = []

   pubsub_message = base64.b64decode(event['data']).decode('utf-8')
   data = json.loads(pubsub_message)

   df = pd.DataFrame.from_records(data)

   print(data)
   print(df)
   
   to_bigquery(os.environ['dataset'], os.environ['table'], df)


def to_bigquery(dataset, table, dataframe):
   bigquery_client = bigquery.Client()
   dataset_ref = bigquery_client.dataset(dataset)
   table_ref = dataset_ref.table(table)
   table = bigquery_client.get_table(table_ref)
   job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
   errors = bigquery_client.load_table_from_dataframe(dataframe, table, job_config=job_config)
   if errors != [] :
      print(errors, file=sys.stderr)