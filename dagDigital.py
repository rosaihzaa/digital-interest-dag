from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from digitalDag.etlDigital import extract, transform, load, confidence_interval
from datetime import timedelta, datetime



default_args = {
    'owner' : 'Rosa',
    'retries' : 1,
    'retry_delay' : timedelta(minutes=2)
}

with DAG(
    dag_id='digital_interest_customer',
    default_args=default_args,
    description='ETL to Looker and BigQuery',
    schedule='@daily',
    start_date=datetime(2025, 4, 24),
    catchup=False
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract
    )
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform
    )
    load_task = PythonOperator(
        task_id='load',
        python_callable=load
    )
    ci_task = BigQueryInsertJobOperator(
            task_id='confidence_interval',
            configuration={
                    'query':{
                        'query':confidence_interval(),
                        'useLegacySql':False,
                        'destinationTable': {
                            'projectId':'bank-marketing-project-446413',
                            'datasetId': 'digitalInterest',
                            'tableId': 'ci-output'
                        },
                    'write_disposition':'WRITE_TRUNCATE',
                    }
            },
            location='US',
            project_id='bank-marketing-project-446413'
            )
        
    
    extract_task >> transform_task >> load_task >> ci_task



