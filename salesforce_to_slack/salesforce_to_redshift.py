from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from tempfile import NamedTemporaryFile
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Connection
from airflow.utils.db import provide_session
from simple_salesforce import Salesforce
import boto3
import json
import boa
from flatten_json import flatten

names = ['Account',
         'Campaign',
         'CampaignMember',
         'Contact',
         'Lead',
         'Opportunity',
         'OpportunityContactRole',
         'OpportunityHistory',
         'Task',
         'User']

SLACK_CHANNEL='@general'
SLACK_USER='Airflow'
SLACK_ICON='DESIGNATED_PHOTO_URL_HERE'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 8, 29),
    'email': ['email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('salesforce_to_redshift',
          default_args=default_args,
          schedule_interval='0 0 * * *',
          catchup=False)


@provide_session
def get_conn(conn_id, session=None):
    conn = (
        session.query(Connection)
        .filter(Connection.conn_id == conn_id)
        .first()
    )
    return conn

def getS3Conn():
    s3_conn = get_conn('INSERT_S3_CONN_ID_HERE')
    aws_key = s3_conn.extra_dejson.get('aws_access_key_id')
    aws_secret = s3_conn.extra_dejson.get('aws_secret_access_key')
    return "aws_access_key_id={0};aws_secret_access_key={1}".format(aws_key, aws_secret)

## Requires a slack connection stored with a token under
## the extra section with the format of {"token":"TOKEN_HERE"}
## The Conn Type can be left blank.

def getSlackConn():
    slack_conn = get_conn('INSERT_SLACK_CONN_ID_HERE')
    return slack_conn.extra_dejson.get('token')

def triggerRun(context, dag_run_obj):
    if context['params']['condition_param']:
        return dag_run_obj

## Requires a salesforce connection stored with username,
## security token, and instance_url. Security token needs
## be passed in under the extra section with the format
## of {"sfdc_security_token":"SECURITY_TOKEN_HERE"}
## The Conn Type can be left blank.

def get_salesforce_conn():
    sfdc = get_conn('SFDC_CONN_ID_HERE')
    return Salesforce(username=sfdc.login,
                      password=sfdc.password,
                      security_token=sfdc.extra_dejson.get('sfdc_security_token'),
                      instance_url=sfdc.host)

def getSalesforceFields(name):
    sf = get_salesforce_conn()
    fields = []
    attr = getattr(sf, name)
    response = attr.describe()['fields']
    for field in response:
        fields.append(field.get('name'))
    return json.dumps(fields)

def getSalesforceRecords(name, **kwargs):
    sf = get_salesforce_conn()
    formatted_name = "{}.json".format(name.lower())
    templates_dict = kwargs.get('templates_dict', {})
    fields = json.loads(templates_dict.get('fields', '[]'))
    query_string = "SELECT {0} FROM {1}".format(','.join(fields), name)
    print(query_string)
    response = sf.query_all(query_string)
    output = response['records']
    output = '\n'.join([json.dumps(flatten({boa.constrict(k): v\
                        for k, v in i.items()})) for i in output])

    with  NamedTemporaryFile("w") as f:
        f.write(output)
        s3_key = 'salesforce/{}'.format(formatted_name)
        s3 = S3Hook(s3_conn_id='INSERT_S3_CONN_ID_HERE')
        s3.load_file(
            filename=f.name,
            key=s3_key,
            bucket_name='INSERT_S3_BUCKET_NAME_HERE',
            replace=True
        )
        s3.connection.close()
        return s3_key

def remove_files_py():
    s3_conn = get_conn('INSERT_S3_CONN_ID_HERE')
    access_key_id = s3_conn.extra_dejson.get('aws_access_key_id')
    secret_access_key = s3_conn.extra_dejson.get('aws_secret_access_key')

    s3 = boto3.client('s3',
                      aws_access_key_id=access_key_id,
                      aws_secret_access_key=secret_access_key)

    s3.delete_objects(Bucket='example-bucket', Delete={
                      'Objects': [
                          {'Key': 'salesforce-files/account.json'},
                          {'Key': 'salesforce-files/campaign.json'},
                          {'Key': 'salesforce-files/campaignmember.json'},
                          {'Key': 'salesforce-files/contact.json'},
                          {'Key': 'salesforce-files/lead.json'},
                          {'Key': 'salesforce-files/opportunity.json'},
                          {'Key': 'salesforce-files/opportunitycontactrole.json'},
                          {'Key': 'salesforce-files/opportunityhistory.json'},
                          {'Key': 'salesforce-files/task.json'},
                          {'Key': 'salesforce-files/user.json'}
                      ]})

kick_off_dag = DummyOperator(
    task_id='kick_off_dag',
    dag=dag
)

trigger_processing = TriggerDagRunOperator(
    task_id='salesforce_data_processing',
    trigger_dag_id='salesforce_models',
    python_callable=triggerRun,
    params={'condition_param': True},
    dag=dag
)

finish_record_pull = DummyOperator(
    task_id='finish_record_pull',
    dag=dag
)

finish_import = DummyOperator(
    task_id='finish_import',
    dag=dag
)

slack_push_sfdc_records = SlackAPIPostOperator(
    task_id='slack_sfdc_records',
    channel=SLACK_CHANNEL,
    token=getSlackConn(),
    username=SLACK_USER,
    text='New SFDC records have been imported into S3.',
    icon_url=SLACK_ICON,
    dag=dag
)

slack_push_raw_import = SlackAPIPostOperator(
    task_id='slack_raw_import',
    channel=SLACK_CHANNEL,
    provide_context=True,
    token=getSlackConn(),
    username=SLACK_USER,
    text='New SFDC records have been imported into Redshift.',
    icon_url=SLACK_ICON,
    dag=dag
)

slack_push_transforms_started = SlackAPIPostOperator(
    task_id='slack_transforms_started',
    channel=SLACK_CHANNEL,
    provide_context=True,
    token=getSlackConn(),
    username=SLACK_USER,
    text='The SFDC workflow is complete.',
    icon_url=SLACK_ICON,
    dag=dag
)

remove_files = PythonOperator(
    task_id='remove_files',
    python_callable=remove_files_py,
    dag=dag)

for name in names:
    TASK_ID_SFDC_FIELDS='get_{}_fields'.format(name)
    TASK_ID_SFDC_RECORDS='get_{}_records'.format(name)
    TASK_ID_INSERT_RECORDS='{}_row_count'.format(name)

    sfdc_fields = PythonOperator(
        task_id=TASK_ID_SFDC_FIELDS,
        op_args=[name],
        python_callable=getSalesforceFields,
        dag=dag
    )

    sfdc_records_template_dict = {'fields': '{{ \
    task_instance.xcom_pull(task_ids=params.previous_task_id) }}'}

    sfdc_records = PythonOperator(
        task_id=TASK_ID_SFDC_RECORDS,
        op_args=[name],
        provide_context=True,
        templates_dict=sfdc_records_template_dict,
        params={'previous_task_id': 'get_{}_fields'.format(name)},
        python_callable=getSalesforceRecords,
        dag=dag
    )

    raw_import_query = \
        """
        TRUNCATE \"{0}\".\"{1}\";
        COPY \"{0}\".\"{1}\"
        FROM 's3://example-bucket/salesforce-files/{1}.json'
        CREDENTIALS '{2}'
        JSON 'auto'
        TRUNCATECOLUMNS;
        """.format('salesforce_raw', name.lower(), getS3Conn())

    raw_import = PostgresOperator(
        task_id='{}_raw_import'.format(name),
        op_args=[name],
        sql=raw_import_query,
        postgres_conn_id='INSERT_PG_CONN_ID_HERE',
        dag=dag)

    insert_records_query = \
        """
        INSERT INTO "salesforce_stats"."insertion_records"
        (SELECT '{0}' AS "table_name",
        count(1) AS "row_count",
        trunc(cast(\'{1}\' as date)) AS "Date"
        FROM "salesforce_raw".\"{0}\")
        """.format(name.lower(), '{{ ts }}')

    insert_records = PostgresOperator(
        task_id=TASK_ID_INSERT_RECORDS,
        op_args=[name],
        sql=insert_records_query,
        # params={'previous_task_id': sfdc_records}
        postgres_conn_id='INSERT_PG_CONN_ID_HERE',
        dag=dag)

    kick_off_dag >> \
    sfdc_fields >> \
    sfdc_records >> \
    finish_record_pull >> \
    raw_import >> \
    finish_import >> \
    insert_records >> trigger_processing

finish_record_pull >> slack_push_sfdc_records
finish_import >> slack_push_raw_import
slack_push_sfdc_records >> slack_push_raw_import
slack_push_raw_import >> trigger_processing
