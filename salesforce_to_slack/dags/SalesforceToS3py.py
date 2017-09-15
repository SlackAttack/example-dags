from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.salesforce_plugin import SalesforceToS3Operator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime, timedelta

default_args = {
			'owner': '<OWNER',
			'depends_on_past': False,
			'start_date': datetime(2017, 8, 1),
			'email': ['<OWNER_EMAIL>'],
			'email_on_failure': False,
			'email_on_retry': False,
			'retries': 3,
			'retry_delay': timedelta(minutes=15),
}

dag = DAG('SalesforceToRedshift',
				default_args=default_args,
				schedule_interval='@once',
				catchup=False)

sf_objects = ['Account',
			  'Campaign',
			  'CampaignMember',
			  'Contact',
			  'Lead',
			  'Opportunity',
			  'OpportunityContactRole',
			  'OpportunityHistory',
			  'User']

task0 = DummyOperator(
		task_id='Start',
		dag=dag)

for sf_object in sf_objects:
	task1 = SalesforceToS3Operator(
				 task_id='{}_To_S3'.format(sf_object),
				 sf_conn_id='<SALESFORCE_CONN_ID>',
				 obj=sf_object,
				 output='salesforce/{}.json'.format(sf_object.lower()),
				 fmt='ndjson',
				 s3_conn_id='<S3_CONN_ID>',
				 s3_bucket=s3_bucket,
				 record_time_added=True,
				 coerce_to_timestamp=True,
				 dag=dag)
	task1.set_upstream(task0)