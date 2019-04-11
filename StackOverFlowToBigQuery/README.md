# StackOverflow to BigQuery #
## Inserting metadata for questions, comments and answers that contains certain tags

### Description ####

A DAG that inserts all new questions for certain StackOverflow tags for the previous day. It also fills all the metadate for the comments and answers those questions have. The DAG runs daily, starting on the 7th of April (`start_date`).

DAG consists in:

* Dataflow Operator: Starts a Apache Beam pipeline in Google Cloud Dataflow. The pipeline reads a file from a GCS bucket which contains the tags that we are interested in. It makes an API call to the StackExchange API to read all post generated for the last day and inserts it on BigQuery.
* Python Operator 'Query': Makes a SQL query to a BQ table to get the question IDs and writes a file with them in Airflow's 'data' folder.
* Python Operator 'CommentsExport' and 'AnswerExport': Reads the file that contains the question IDs and, after splitting in lists of 100, makes a API call to get the metadata of comments/answers and writes a (NEWLINE DELIMITED) JSON after cleaning data.
* Python Operator 'AnswersToGCS' and 'CommentToGCS: Copies previous JSON files to other bucket
* Python Operator 'CommentToBQ' and 'AnswerToBQ': Reads the JSON files and insert the date to the appropiate tables.

### Setting the DAG up ###

This was done on Google Cloud Composer:

* Add DAG (`StackOverflow_DAG.py`) to the DAG folder in Composer's bucket
* Add Apache Beam pipeline (`pipeline/api_pipeline.py`) to a Composer's bucket (preferably something like `data/pipelines`)
* (Optional) Add default values to the pipeline.
* Add Airflow Variables for (sugestions in parenthesis):
	* answer_export: path+suffix to where the answers reports are going to be stored (`answers/answer_report`)
	* comment_export: path+suffix to where the comment reports are going to be stored (`comment/comment_report`)
	* dataset
	* destination_bucket: name of the secondary bucket
	* pipeline_api_tags: path to the pipeline (`/home/airflow/gcs/data/pipelines/api_pipeline.py`)
	* project
	* schema_answer: path to schema for answers table
	* schema_comment: path to schema for comments table
	* schema_question: path to schema for questions table
	* source_bucket: name of Composer's bucket
	* table_answer
	* table_comment
	* table_question
	* tags_path: full GCS path to file that contains the desired tags
	* temp_bucket: Dataflow's temp bucket folder. Full GCS path
	
## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.


