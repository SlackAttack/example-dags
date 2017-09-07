from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from tempfile import NamedTemporaryFile
from airflow.models import Connection
from airflow.utils.db import provide_session
import boto3
import pandas as pd
import io
import time
from weasyprint import HTML, CSS

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 8, 17),
    'email': ['example@email.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

timestamp = """{{ ts }}"""

dag = DAG('salesforce_data_processing',
          default_args=default_args,
          schedule_interval=None,
          catchup=False)

@provide_session
def get_conn(conn_id, session=None):
    conn = (
        session.query(Connection)
        .filter(Connection.conn_id == conn_id)
        .first()
    )
    return conn


## Requires a slack connection stored with a token under
## the extra section with the format of {"token":"TOKEN_HERE"}
## The Conn Type can be left blank.

def getSlackConn():
    slack_conn = get_conn('INSERT_SLACK_CONN_ID_HERE')
    return slack_conn.extra_dejson.get('token')

def get_s3_conn():
    s3_conn = get_conn('INSERT_S3_CONN_ID_HERE')
    key_id = s3_conn.extra_dejson.get('aws_access_key_id')
    secret = s3_conn.extra_dejson.get('aws_secret_access_key')
    return "aws_access_key_id={};aws_secret_access_key={}".format(key_id,secret)

def none_to_zero(value):
    if value is None:
        return 0
    return float(value)

def won_deal(value):
    if value == 'Closed Won':
        return 1
    return 0
def not_won(value):
    if value != 'Closed Won':
        return 1
    return 0

def triggerRun(context, dag_run_obj):
    if context['params']['condition_param']:
        return dag_run_obj


t0 = DummyOperator(
    task_id='kick_off_dag',
    dag=dag
)

create_field_reports_query = \
    """
    TRUNCATE salesforce_staging.field_reports;
    INSERT INTO salesforce_staging.field_reports
    (SELECT
        u.id
        ,u.name
    	,u.email
    	,t.subject
    	,t.description
        ,cast(t.created_date as timestamptz)
    	,cast('{}' as timestamptz) as "insertion_date"
    FROM salesforce_raw.task t
    JOIN salesforce_raw.user u
    ON t.created_by_id = u.id
    WHERE subject ilike '%Field Report%')
    """.format(timestamp)

create_field_reports = PostgresOperator(
    task_id='create_field_reports',
    sql=create_field_reports_query,
    postgres_conn_id='INSERT_PG_CONN_ID_HERE',
    dag=dag
)

create_open_opp_query = \
    """
    TRUNCATE salesforce_staging.open_opps;
    INSERT INTO salesforce_staging.open_opps
    (SELECT
    	name,
    	stage_name,
    	cast(amount as int),
    	cast(expected_revenue as int),
    	lead_source,
    	owner_id,
    	cast(created_date as timestamptz),
    	cast(last_modified_date as timestamptz)
    FROM salesforce_raw.opportunity
    WHERE is_closed = 'f');
    """

create_open_opp = PostgresOperator(
    task_id='create_open_opp',
    sql=create_open_opp_query,
    postgres_conn_id='INSERT_PG_CONN_ID_HERE',
    dag=dag)

count_opps_by_lead_source_query = \
    """
    TRUNCATE "salesforce_models"."opps_by_lead_source";
    INSERT INTO "salesforce_models"."opps_by_lead_source"
    (SELECT
        "stage_name"
        ,"lead_source"
        ,sum("amount") as "amount"
        ,sum("expected_revenue") as "expected_revenue"
        ,trunc(cast("created_date" as date)) as created_date
        ,cast('{}' as date) AS "insertion_date"
        FROM "salesforce_staging"."open_opps"
        GROUP BY "stage_name", "lead_source",trunc(cast("created_date" as date)));
    """.format(timestamp)

count_opps_by_lead_source = PostgresOperator(
    task_id='count_opps_by_lead_source',
    sql=count_opps_by_lead_source_query,
    postgres_conn_id='INSERT_PG_CONN_ID_HERE',
    dag=dag)

count_opps_by_rep_query = \
    """
    TRUNCATE "salesforce_models"."opps_by_rep";
    INSERT INTO salesforce_models.opps_by_rep
    (SELECT
        "u"."name" as "name"
        ,"stage_name" as "stage_name"
        ,sum("amount") as "amount"
        ,sum("expected_revenue") as "expected_revenue"
        ,trunc(cast("o"."created_date" as date)) as "created_date"
        ,cast('{}' as timestamptz) AS "insertion_date"
    FROM "salesforce_staging"."open_opps" "o"
    JOIN "salesforce_raw"."user" "u"
    ON "o"."owner_id" = "u"."id"
    GROUP BY
    "u"."name"
    ,"o"."stage_name"
    ,trunc(cast("o"."created_date" as date)));
    """.format(timestamp)

count_opps_by_rep = PostgresOperator(
    task_id='count_opps_by_rep',
    sql=count_opps_by_rep_query,
    postgres_conn_id='INSERT_PG_CONN_ID_HERE',
    dag=dag)


create_won_opp_query = \
"""
    TRUNCATE salesforce_staging.won_opp_amounts;
    INSERT INTO salesforce_staging.won_opp_amounts
    (SELECT
        o.name,
        u.name as "owner_name",
        o.lead_source,
        CAST(o.amount as INT4),
        CAST(o.created_date as timestamptz),
        CAST(o.last_modified_date as timestamptz)
    FROM salesforce_raw.opportunity o
    JOIN salesforce_raw.user u
        ON o.owner_id = u.id
    WHERE o.stage_name = 'Closed Won'
    ORDER BY o.name);
"""

create_won_opp = PostgresOperator(
    task_id='create_won_opp',
    sql=create_won_opp_query,
    postgres_conn_id='INSERT_PG_CONN_ID_HERE',
    dag=dag)


field_reports_count_query = \
    """
    TRUNCATE salesforce_models.field_report_count;
    INSERT INTO salesforce_models.field_report_count
    (SELECT name,count(1), trunc(cast(created_date as date)) as created_date
    FROM salesforce_staging.field_reports
    GROUP BY name,trunc(cast(created_date as date)));
    """



field_report_count = PostgresOperator(
    task_id='count_field_reports',
    sql=field_reports_count_query,
    postgres_conn_id='INSERT_PG_CONN_ID_HERE',
    dag=dag
)

won_opp_count_query = \
    """
    TRUNCATE salesforce_models.won_opp_count;
    INSERT INTO salesforce_models.won_opp_count
    SELECT
        owner_name
        ,lead_source
        ,count(1)
        ,sum(amount) as amount
        ,trunc(cast(last_modified_date as date)) AS last_modified_date
    FROM salesforce_staging.won_opp_amounts
    GROUP BY
        trunc(cast(last_modified_date as date))
        ,owner_name
        ,lead_source
    """

won_opp_count = PostgresOperator(
    task_id='won_opp_count',
    sql=won_opp_count_query,
    postgres_conn_id='INSERT_PG_CONN_ID_HERE',
    dag=dag
)

leads_by_lead_source_query = \
"""
TRUNCATE salesforce_models.lead_by_lead_source;
INSERT INTO salesforce_models.lead_by_lead_source(
    SELECT lead_source,count(1),trunc(cast(created_date as date)) created_date
    FROM salesforce_raw.lead
    WHERE lead_source IS NOT NULL
    GROUP BY lead_source,created_date);
"""

count_lead_by_lead_source = PostgresOperator(
    task_id='count_lead_by_lead_source',
    sql=leads_by_lead_source_query,
    postgres_conn_id='INSERT_PG_CONN_ID_HERE',
    dag=dag
)

def createSolutionDirectorOutput(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='INSERT_PG_CONN_ID_HERE')

    current_field_report_count_query = \
    """
    SELECT name,count as field_reports
    FROM salesforce_models.field_report_count
    WHERE date_part(month,created_date) = date_part(month,GETDATE());
    """

    opps_by_rep_query = \
    """
    SELECT name,sum(expected_revenue)
    FROM salesforce_models.opps_by_rep
    GROUP BY name;
    """

    wins_query = \
    """
    SELECT owner_name as name,count(1) as wins
    FROM salesforce_staging.won_opp_amounts
    WHERE date_part(month,cast(last_modified_date as date)) = date_part(month,GETDATE())
    GROUP BY trunc(cast(last_modified_date as date)),owner_name;
    """

    bookings_query = \
    """
    SELECT owner_name as name,sum(amount) as bookings
    FROM salesforce_staging.won_opp_amounts
    WHERE date_part(month,cast(last_modified_date as date)) = date_part(month,GETDATE())
    GROUP BY trunc(cast(last_modified_date as date)),owner_name;
    """

    field_reports = pg_hook.get_pandas_df(current_field_report_count_query)
    expected_revenue = pg_hook.get_pandas_df(opps_by_rep_query)
    wins = pg_hook.get_pandas_df(wins_query)
    bookings = pg_hook.get_pandas_df(bookings_query)

    final_output = expected_revenue.set_index('name')\
                                   .join(field_reports.set_index('name'))\
                                   .join(wins.set_index('name'))\
                                   .join(bookings.set_index('name'))\
                                   .fillna(0.0)\
                                   .astype(int)\
                                   .reset_index()\
                                   .sort_values('name')\
                                   .to_html('solutionDirector.html', index=False)

    HTML('solutionDirector.html')\
    .write_png('solutionDirector.png',
               stylesheets=[CSS(string=
               '''
               @page {
                  size: 650px 375px;
                  margin:0;
               }

               table {
                    border-collapse: collapse;
                    width: 100%;
                    font-family: avenir;
               }

                th, td {
                    padding: 15px;
                    text-align: left;
                }

                tr:nth-child(even) {background-color: #c1b19a}

                th {
                    background-color: #342f54;
                    color: #f4f2ec;
                }
               ''')])

    current_time = datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M%S')
    s3key = "salesforce-files/{0}.png".format(current_time)
    s3 = S3Hook(s3_conn_id='INSERT_S3_CONN_ID_HERE')
    s3.load_file(
        filename='solutionDirector.png',
        key=s3key,
        bucket_name='example-bucket',
        replace=True
    )
    s3.connection.close()

    path = 'https://s3.amazonaws.com/example-bucket/{0}'.format(s3key)

    return path

SLACK_CHANNEL='@general'
SLACK_USER='Airflow'
SLACK_ICON='DESIGNATED_PHOTO_URL_HERE'

sd_slack_attachments = [
    {
        "fallback": "New Solution Director data available.",
        "color": "#342f54",
        "author_name": SLACK_USER,
        "author_icon": SLACK_ICON,
        "title": "Salesforce Solution Director Stats",
        "image_url": "{{ task_instance.xcom_pull(task_ids='create_solution_director_output') }}",
        "thumb_url": SLACK_ICON,
        "footer": "Thanks!",
    }
]

sd_slack = SlackAPIPostOperator(
    task_id = 'solution_director_slack',
    channel=SLACK_CHANNEL,
    token=getSlackConn(),
    username=SLACK_USER,
    text='',
    attachments=sd_slack_attachments,
    icon_url=SLACK_ICON,
    dag=dag
)

ls_slack_attachments = [
    {
        "fallback": "New Lead Source data available.",
        "color": "#342f54",
        "author_name": SLACK_USER,
        "author_icon": SLACK_ICON,
        "title": "Salesforce Lead Source Stats",
        "image_url": "{{ task_instance.xcom_pull(task_ids='create_lead_source_output') }}",
        "thumb_url": SLACK_ICON,
        "footer": "Thanks!",
    }
]

ls_slack = SlackAPIPostOperator(
    task_id = 'lead_source_slack',
    channel=SLACK_CHANNEL,
    token=getSlackConn(),
    username=SLACK_USER,
    text='',
    attachments=ls_slack_attachments,
    icon_url=SLACK_ICON,
    dag=dag
)

def formatOutput(query):
    pg_hook = PostgresHook(postgres_conn_id='INSERT_PG_CONN_ID_HERE')
    results = pg_hook.get_records(query)
    new_results = []
    for i in results:
        if len(i) > 2:
            new_results.append("*_{0}_*: {1} won, ${2} total bookings, {3}"\
            .format(i[0],i[1],i[2],i[3]))
        else:
            new_results.append("*_{0}_*: {1}".format(i[0],i[1]))
        new_results.append('\n')
    new_results = '\n'.join(new_results)
    return new_results

prepare_field_reports_query =\
"""
SELECT name, description
FROM salesforce_staging.field_reports
ORDER BY name;
"""

prepare_field_reports = PythonOperator(
    task_id='prepare_field_reports',
    op_args=[prepare_field_reports_query],
    python_callable=formatOutput,
    dag=dag)

field_reports_slack = SlackAPIPostOperator(
    task_id = 'field_reports_slack',
    channel=SLACK_CHANNEL,
    token=getSlackConn(),
    username=SLACK_USER,
    text="*---------------------------Field Notes:---------------------------*\
     \n {{ task_instance.xcom_pull(task_ids='prepare_field_reports') }}",
    icon_url=SLACK_ICON,
    dag=dag)

create_solution_director_output = PythonOperator(
    task_id='create_solution_director_output',
    python_callable=createSolutionDirectorOutput,
    provide_context=True,
    dag=dag)

def createLeadSourceOutput(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='INSERT_PG_CONN_ID_HERE')

    mql_query = \
    """
    SELECT lead_source,count(1) as "mql"
    FROM salesforce_models.lead_by_lead_source
    WHERE date_part(month,created_date) = date_part(month,GETDATE())
    GROUP BY lead_source
    """

    new_opp_query = \
    """
    SELECT
        lead_source,count(1) as "new_opps"
    FROM salesforce_models.opps_by_lead_source
    WHERE date_part(month,cast(created_date as date)) = date_part(month,GETDATE())
    GROUP BY lead_source;
    """

    opps_by_lead_source_rev_query = \
    """
    SELECT lead_source,sum(expected_revenue) as "expected_revenue"
    FROM salesforce_models.opps_by_lead_source
    GROUP BY lead_source;
    """

    wins_query = \
    """
    SELECT
        lead_source,
        count(1) as wins
    FROM salesforce_models.won_opp_count
    WHERE date_part(month,cast(last_modified_date as date)) = date_part(month,GETDATE())
    GROUP BY lead_source;
    """

    bookings_query = \
    """
    SELECT
        lead_source,
        sum(amount) as bookings
    FROM salesforce_models.won_opp_count
    WHERE date_part(month,cast(last_modified_date as date)) = date_part(month,GETDATE())
    GROUP BY lead_source;
    """

    mql = pg_hook.get_pandas_df(mql_query)
    new_opps = pg_hook.get_pandas_df(new_opp_query)
    expected_revenue = pg_hook.get_pandas_df(opps_by_lead_source_rev_query)
    wins = pg_hook.get_pandas_df(wins_query)
    bookings = pg_hook.get_pandas_df(bookings_query)

    final_output = expected_revenue.set_index('lead_source')\
                                   .join(mql.set_index('lead_source'))\
                                   .join(new_opps.set_index('lead_source'))\
                                   .join(wins.set_index('lead_source'))\
                                   .join(bookings.set_index('lead_source'))\
                                   .fillna(0.0)\
                                   .astype(int)\
                                   .reset_index()\
                                   .sort_values('lead_source')\
                                   .to_html('leadSource.html', index=False)

    HTML('leadSource.html')\
    .write_png('leadSource.png',
               stylesheets=[CSS(string=
               '''
               @page {
                  size: 700px 375px;
                  margin:0;
               }

               table {
                    border-collapse: collapse;
                    width: 100%;
                    font-family: avenir;
               }

                th, td {
                    padding: 15px;
                    text-align: left;
                }

                tr:nth-child(even) {background-color: #c1b19a}

                th {
                    background-color: #ff9a09;
                    color: #f4f2ec;
                }
               ''')])

    current_time = datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M%S')
    s3key = "salesforce-files/{0}.png".format(current_time)
    s3 = S3Hook(s3_conn_id='INSERT_S3_CONN_ID_HERE')
    s3.load_file(
        filename='leadSource.png',
        key=s3key,
        bucket_name='example-bucket',
        replace=True
    )
    s3.connection.close()

    path = 'https://s3.amazonaws.com/example-bucket/{0}'.format(s3key)

    return path

create_lead_source_output = PythonOperator(
    task_id='create_lead_source_output',
    python_callable=createLeadSourceOutput,
    provide_context=True,
    dag=dag
)

t0 >> create_field_reports >> field_report_count >> create_solution_director_output
t0 >> create_open_opp
create_open_opp >> count_opps_by_lead_source >> create_lead_source_output
create_open_opp >> count_opps_by_rep >> create_solution_director_output
count_opps_by_lead_source >> create_solution_director_output
t0 >> create_won_opp >> won_opp_count >> create_lead_source_output
won_opp_count >> create_solution_director_output
t0 >> count_lead_by_lead_source >> create_lead_source_output
create_solution_director_output >> sd_slack
create_lead_source_output >> ls_slack
create_field_reports >> prepare_field_reports >> field_reports_slack
