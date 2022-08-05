import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options import pipeline_options
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery, ReadFromBigQuery
from apache_beam.runners import DataflowRunner

import google.auth

from google.cloud import bigquery
import datetime

project = google.auth.default()[1]

options = pipeline_options.PipelineOptions(
    streaming=True,
    project=project
)

dataset='wordle'
read_table = "lookerplus.wordle.letters_5"
write_table = "lookerplus.wordle.wordle_from_dataflow"

topic = "projects/lookerplus/topics/wordle_topic"
subscription = "projects/lookerplus/subscriptions/wordle_topic-sub"

class GetWord(beam.DoFn):
    def process(self, element):
        print(element)
        random_number = element['random_number']
        
        client = bigquery.Client()
        query_job = client.query(f"SELECT string_field_0 as word FROM (SELECT string_field_0, row_number() over() as rn FROM `{read_table}`) WHERE rn = {random_number}")
        results = query_job.result()
        
        print(results)
        
        for row in results:
            word = row.word

        yield {'word':word, 'ts':element['time']}

class GetWordInfo(beam.DoFn):
    def process(self, element):
        print(element)
        word = element['word']
        word_info = []

        for i in word:
            if {'letter': i, 'times': word.count(i)} not in word_info:
                word_info.append({'letter': i, 'times': word.count(i)})
        
        ts = datetime.datetime.fromtimestamp(element['ts']).strftime('%Y-%m-%d %H:%M:%S')
        
        print({'ts':ts, 'word':word, 'word_info':word_info})

        yield {'ts':ts, 'word':word, 'word_info':word_info}

def streaming_pipeline(project, region="europe-west6"):    
    schema = {
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
    
    options = PipelineOptions(
        streaming=True,
        project=project,
        region=region
    )

    p = beam.Pipeline(DataflowRunner(), options=options)

    word_index = (p | "Read Topic" >> ReadFromPubSub(subscription=subscription)
                    | "To Dict" >> beam.Map(json.loads)
                    | "Get Word" >> beam.ParDo(GetWord())
                    | "Get Word Info" >> beam.ParDo(GetWordInfo())
                    | 'Write to BigQuery' >> beam.io.Write(beam.io.WriteToBigQuery(
                        write_table,
                        dataset=dataset,
                        schema=schema,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                    ))
                 )

    return p.run()

# try:
#     pipeline = streaming_pipeline(project)
#     print("\n PIPELINE RUNNING \n")
# except (KeyboardInterrupt, SystemExit):
#     raise
# except:
#     print("\n PIPELINE FAILED")
#     traceback.print_exc()