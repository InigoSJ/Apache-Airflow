from airflow import models
from airflow.operators import python_operator
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.models import Variable
from google.cloud import bigquery

import datetime
import requests
import pandas as pd

default_dag_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2019, 04, 7, 10, 00),
    'schedule_interval': datetime.timedelta(days=1)
}

source_bucket = Variable.get("source_bucket")
destination_bucket = Variable.get("destination_bucket")

pipeline_api_tags = Variable.get("pipeline_api_tags")
temp_bucket = Variable.get("temp_bucket")
project = Variable.get("project")
tags_path = Variable.get("tags_path")

schema_question = Variable.get("schema_question")
schema_comment = Variable.get("schema_comment")
schema_answer = Variable.get("schema_answer")

dataset = Variable.get("dataset")
table_question = Variable.get("table_question")
table_answer = Variable.get("table_answer")
table_comment = Variable.get("table_comment")

question_id_path = Variable.get("question_id_path")
comment_export = Variable.get("comment_export")
answer_export = Variable.get("answer_export")

with models.DAG(
        'StackOverflow',
        default_args=default_dag_args) as dag:

    today = datetime.datetime.today()
    yesterday = today - datetime.timedelta(1)

    yesterday_string = yesterday.strftime('%d%m%Y')
    yesterday_dash_string = yesterday.strftime("%Y-%m-%d")


    def QueryToGCS(sql):

        client = bigquery.Client()
        query_job = client.query(
            sql,
            location="EU")

        question_ids = ''
        for row in query_job:
            question_ids += str(row['question_id']) + '\n'
        questions_file = open('/home/airflow/gcs/data/{}_{}.txt'.format(question_id_path, yesterday_string), "w+")
        questions_file.write(question_ids)
        questions_file.close()


    def CommentsToGCS(separation=100):

        file_path = '/home/airflow/gcs/data/{}_{}.txt'.format(question_id_path, yesterday_string)
        f = open(file_path)
        id_list = f.readlines()
        f.close()

        id_chunks = [id_list[x:x + separation] for x in range(0, len(id_list), separation)]

        path = '/home/airflow/gcs/data/{}_{}.json'.format(comment_export, yesterday_string)

        for chunk in id_chunks:
            CommentsApi(chunk, path)


    def AnswersToGCS(separation=100):

        file_path = '/home/airflow/gcs/data/{}_{}.txt'.format(question_id_path, yesterday_string)
        f = open(file_path)
        id_list = f.readlines()
        f.close()

        id_chunks = [id_list[x:x + separation] for x in range(0, len(id_list), separation)]

        path = '/home/airflow/gcs/data/{}_{}.json'.format(answer_export, yesterday_string)

        for chunk in id_chunks:
            AnswersApi(chunk, path)


    def AnswersApi(ids, path):
        # answer_id,creation_date,is_accepted,last_activity_date,last_edit_date,owner.accept_rate,owner.display_name,owner.link,owner.profile_image,owner.reputation,owner.user_id,owner.user_type,question_id,score
        id_str = ';'.join(x.replace('\n', '') for x in ids)

        api_url = "http://api.stackexchange.com/2.2/questions/{}/answers?order=desc&sort=activity&site=stackoverflow".format(
            id_str)

        api_call = requests.get(api_url)
        api_call_dict = eval(api_call.content.replace(b"true", b"True").replace(b"false", b"False"))
        api_df = pd.io.json.json_normalize(api_call_dict['items'])
        api_df = api_df[['answer_id', 'creation_date', 'is_accepted', 'score', 'owner.user_id',
                         'owner.reputation', 'question_id']]
        api_df = api_df.rename(columns=lambda x: x.replace('owner.', 'user_'))
        api_df = api_df.rename(columns={'user_user_id': 'user_id'})

        api_dict = api_df.to_dict('records')

        final_json = ''

        for question in api_dict:
            final_json += str(question) + '\n'

        final_json = final_json.replace("True", "true").replace("False", "false")

        json_answers = open(path, "a")
        json_answers.write(final_json)
        json_answers.close()


    def CommentsApi(ids, path):
        # answer_id,creation_date,is_accepted,last_activity_date,last_edit_date,owner.accept_rate,owner.display_name,owner.link,owner.profile_image,owner.reputation,owner.user_id,owner.user_type,question_id,score
        id_str = ';'.join(x.replace('\n', '') for x in ids)

        api_url = "http://api.stackexchange.com/2.2/questions/{}/comments?order=desc&sort=creation&site=stackoverflow".format(
            id_str)

        api_call = requests.get(api_url)
        api_call_dict = eval(api_call.content.replace(b"true", b"True").replace(b"false", b"False"))
        api_df = pd.io.json.json_normalize(api_call_dict['items'])
        api_df = api_df[['comment_id', 'creation_date', 'score', 'owner.user_id', 'owner.reputation',
                         'post_id']]
        api_df = api_df.rename(columns=lambda x: x.replace('owner.', 'user_'))
        api_df = api_df.rename(columns={'user_user_id': 'user_id', 'post_id': 'question_id'})

        api_dict = api_df.to_dict('records')

        final_json = ''

        for question in api_dict:
            final_json += str(question) + '\n'

        final_json = final_json.replace("True", "true").replace("False", "false")

        json_comments = open(path, "a")
        json_comments.write(final_json)
        json_comments.close()


    # Questions metadata

    APITags = DataFlowPythonOperator(py_file=pipeline_api_tags,
                                     options={'input': tags_path,
                                              'temp_location': temp_bucket,
                                              'project': project},
                                     task_id='apicallpipeline')

    # Comments and Answers reports

    sql = 'SELECT question_id FROM `{0}.{1}` WHERE creation_date >= TIMESTAMP("{2}")'.format(dataset, table_question,
                                                                                             yesterday_dash_string)

    Query = python_operator.PythonOperator(task_id='Query', python_callable=QueryToGCS, op_kwargs={'sql': sql})

    CommentsExport = python_operator.PythonOperator(task_id='CommentsExport', python_callable=CommentsToGCS)
    AnswerExport = python_operator.PythonOperator(task_id='AnswerExport', python_callable=AnswersToGCS)

    comment_file = '{}_{}.json'.format(comment_export, yesterday_string)
    answer_file = '{}_{}.json'.format(answer_export, yesterday_string)

    CommentToGCS = GoogleCloudStorageToGoogleCloudStorageOperator(
        task_id="Comment_to_GSC",
        source_bucket=source_bucket,
        source_object="data/{}".format(comment_file),
        destination_bucket=destination_bucket,
        destination_object=comment_file)

    AnswerToGCS = GoogleCloudStorageToGoogleCloudStorageOperator(
        task_id="Answer_to_GSC",
        source_bucket=source_bucket,
        source_object="data/{}".format(answer_file),
        destination_bucket=destination_bucket,
        destination_object=answer_file)

    CommentToBQ = GoogleCloudStorageToBigQueryOperator(
        task_id="Comments_to_BigQuery",
        bucket=destination_bucket,
        source_objects=[comment_file],  # This needs to be a a list of sources
        schema_object=schema_comment,
        source_format="NEWLINE_DELIMITED_JSON",
        destination_project_dataset_table="{}.{}".format(dataset, table_comment),
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND")

    AnswerToBQ = GoogleCloudStorageToBigQueryOperator(
        task_id="Answer_to_BigQuery",
        bucket=destination_bucket,
        source_objects=[answer_file],  # This needs to be a a list of sources
        schema_object=schema_answer,
        source_format="NEWLINE_DELIMITED_JSON",
        destination_project_dataset_table="{}.{}".format(dataset, table_answer),
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND")

    APITags >> Query >> CommentsExport >> CommentToGCS >> CommentToBQ
    Query >> AnswerExport >> AnswerToGCS >> AnswerToBQ
