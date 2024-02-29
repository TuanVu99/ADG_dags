from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import smbclient
import pandas as pd

default_args = {
    'owner': 'ducnn',
    'depends_on_past': False,
    'start_date': '2021-04-02',
    'email_on_failure': False,
    'email_on_retry': False,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    'Test',
    default_args=default_args,
    description='test',
    schedule_interval=None,
)

def n(df, index1):
    return df.columns[index1]

def test():
    file,table, schema = '/opt/sharedfolder/master_data/material/nhom_tp.xlsx','material_upload', 'master_data'
    
    df = pd.read_excel(file)
    df = pd.read_excel(file, converters={n(df, 0):str, n(df, 6):str})
    columns = ['material', 'material_name', 'level3', 'level2', 'level1', 'nganh', 'ph_code']
    data = []
    for row in df.iterrows():
        row = list(row[1])
        if pd.isnull(row[0]):
            continue
        data.append(dict(zip(columns, row)))
    fast_insert(data, table, schema , columns, sql_pre_insert = f"""TRUNCATE TABLE {schema}.{table}""")


begin = DummyOperator(dag=dag, task_id="begin")

task_upload_mkt_campaign = PythonOperator(task_id = "test", python_callable = test, dag = dag)

begin >> task_upload_mkt_campaign


