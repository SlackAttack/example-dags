"""Example DAG Detailing Examples of Dynamic Dag Creation

This DAG File creates DAGs in Airflow Dynamically for the following scenario.

We have an ever-growing list of customers that each have a similar workflow need.

Assumptions
    - Each Customer Will Have a Unique Source Connection
    - Each Customer Source Will have a Single Destination Connection
    - Actual Workflow will be represented by DummyOperators 
    - We will never need to delete a customer dag file 
"""

import json
import os

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.models import Connection
from airflow.utils.db import provide_session

## Create JSON Var if it doesn't exist
## Get JSON Var
CUSTOMERS = [
    {
        'customer_name': 'Faux Customer',
        'customer_id': 'faux_customer',
        'email': ['admin@fauxcustomer.com', 'admin@astronomer.io'],
        'enabled': True
    },
    {
        'customer_name': 'Bogus Customer',
        'customer_id': 'bogus_customer',
        'email': ['admin@boguscustomer.com', 'admin@astronomer.io'],
        'enabled': False
    }
]

CUSTOMERS = Variable.get("customer_list", default_var=CUSTOMERS, deserialize_json=True)

def create_dag(customer):
    """
    Takes a cust parameters dict, uses that to override default args creates DAG object
    
    Returns: DAG() Object
    """
    default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'email': 'xyz@xyz.com',
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            'start_date': datetime(2017, 1, 1, 0, 0),
            'end_date': None,
            'schedule_interval': '@once'
    }

    # This allows DAG parameters to be passed in from the Variable if a customer needs something specific overridden in their DAG
    # See email example
    replaced_args = {k: default_args[k] if customer.get(k, None) is None else customer[k] for k in default_args}

    dag_id = '{base_name}_{id}'.format(base_name='load_clickstream_data',id=customer['customer_id'])
    return DAG(dag_id=dag_id, default_args=replaced_args)

for cust in CUSTOMERS: # Loop customers array of containing customer objects
    dag = create_dag(cust)    
    globals()[dag.dag_id] = dag

    extract = DummyOperator(
        task_id='extract_data',
        dag=dag
    )

    transform = DummyOperator(
        task_id='transform_data',
        dag=dag
    )

    load = DummyOperator(
        task_id='load_data',
        dag=dag
    )

    extract.set_downstream(transform)
    transform.set_downstream(load)






