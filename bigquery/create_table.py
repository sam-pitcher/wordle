from google.cloud import bigquery
from google.cloud.bigquery import table

SCHEMA = {
    "fields": [
      {
        "name": "ts",
        "type": "TIMESTAMP"
      },
      {
        "name": "word",
        "type": "STRING"
      },
      {
        "fields": [
          {
            "name": "letter",
            "type": "STRING"
          },
          {
            "name": "times",
            "type": "INTEGER"
          }
        ],
        "mode": "REPEATED",
        "name": "word_info",
        "type": "RECORD"
      }
    ]
  }
DATASET_NAME = 'wordle'
TABLE_NAME = 'wordle_from_dataflow'

def create_bigquery_table(SCHEMA, DATASET_NAME, TABLE_NAME):
    client = bigquery.Client()
    dataset_ref = client.dataset(DATASET_NAME)
    table_ref = dataset_ref.table(TABLE_NAME)
    table = bigquery.table.Table(table_ref, schema=SCHEMA)
    table = client.create_table(table, exists_ok=True)
    return table

# create_bigquery_table(
#     SCHEMA,
#     DATASET_NAME,
#     TABLE_NAME
# )