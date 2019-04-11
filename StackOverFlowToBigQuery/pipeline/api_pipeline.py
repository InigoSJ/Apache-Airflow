import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.core import ParDo
from apache_beam.io.textio import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

#TODO: 
default_input = 
default_table =
default_dataset = 
project = 


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default=default_input,
                        help='Input file to process.')
    parser.add_argument('--table',
                        dest='table',
                        default=default_table,
                        help='Table to upload.')
    parser.add_argument('--dataset',
                        dest='dataset',
                        default=default_dataset,
                        help='Dataset where the table is store. Needs to exists beforehand')

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend(['--project={}'.format(project)])

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:

        def QuestionAPI(tags):

            import datetime
            import pandas as pd
            import requests

            #python2 to calculate today and yesterday
            today = datetime.date.today() - datetime.date(1970, 1, 1)
            yesterday = today - datetime.timedelta(1)

            from_date = int(yesterday.total_seconds())
            to_date = int(today.total_seconds())

            logging.info('Calling API for tag: "{}"'.format(tags))

            api_url = "http://api.stackexchange.com/2.2/search/advanced?fromdate={0}&todate={1}&order=desc&sort=activity&tagged={2}&site=stackoverflow".format(
                from_date, to_date, tags)

            api_call = requests.get(api_url)
            api_call_dict = eval(api_call.content.replace(b"true", b"True").replace(b"false", b"False"))

            # Create a DataFrame to simplify data processing
            try:
                so_api_call_DF = pd.DataFrame(api_call_dict['items'])

                if so_api_call_DF.empty:
                    logging.info('Tag "{}" does not have questions '.format(tags))
                    return []
                else:
                    so_api_call_DF = so_api_call_DF[
                        ['creation_date', 'question_id', 'title', 'link', 'tags', 'is_answered']]
                    # Fixing tags and creation_date fields:
                    so_api_call_DF['tags'] = so_api_call_DF.tags.apply(
                        lambda x: ', '.join(x).replace('[', '').replace(']', ''))
                    so_api_call_dict = so_api_call_DF.to_dict('records')

                    return so_api_call_dict

            except:
                logging.warning('Unexpected API request output: \n {}'.format(api_call_dict))
                return []

        schema = 'creation_date:timestamp,question_id:integer,is_answered:boolean,title:string,tags:string,link:string'

        api_call = (p | 'read' >> ReadFromText(known_args.input)
                    | "API call for each Tag" >> ParDo(fn=QuestionAPI)
                    | "Writing to BQ" >> WriteToBigQuery(table=known_args.table,
                                                         dataset=known_args.dataset,
                                                         project=project, schema=schema,
                                                         create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                                                         write_disposition=BigQueryDisposition.WRITE_APPEND))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
