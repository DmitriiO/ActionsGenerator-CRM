from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow import configuration
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import requests
import pandas as pd

# Для каждого бота запуск отдельного DAGa

DAG_NAME = 'data2excel'

GP_CONN_ID = 'orlovdv'

args = {'owner': 'orlovdv',
	'start_date': datetime(2023,9,26),
	'retries': 3,
	'retry_delay': timedelta(seconds = 120)
       }

# SQL-запрос для извлечения данных из Greenplum
sql_query1 = '''
SELECT * FROM crm_bot_actions_mrf1;
'''
sql_query2 = '''
SELECT * FROM crm_bot_nature_mrf1;
'''

# Оператор PythonOperator для выполнения SQL-запроса и преобразования результата в DataFrame
def extract_data():

    hook = PostgresHook(postgres_conn_id=GP_CONN_ID)  # Идентификатор соединения с Greenplum
    
    connection = hook.get_conn()
    
    cursor = connection.cursor()
    
    cursor.execute(sql_query1)
    
    result = cursor.fetchall()

    # Преобразуем результат в DataFrame
    df_crm_bot_actions_mrf1 = pd.DataFrame(result, columns=[col[0] for col in cursor.description])
    
    cursor.execute(sql_query2)
    
    result = cursor.fetchall()
    
     # Преобразуем результат в DataFrame
    df_crm_bot_nature_mrf1 = pd.DataFrame(result, columns=[col[0] for col in cursor.description])
    
    # Закрытие соединение с базой данных
    cursor.close()
    
    connection.close()

    # Сохранение значения в XCom с ключом 'crm_bot_actions_mrf1'
    kwargs['ti'].xcom_push(key='df_crm_bot_actions_mrf1', value=df_crm_bot_actions_mrf1)
    
    # Сохранение значения в XCom с ключом 'crm_bot_actions_mrf1'
    kwargs['ti'].xcom_push(key='df_crm_bot_nature_mrf1', value=df_crm_bot_nature_mrf1)
    


def process_data():
    
    project_path='/home/orlov-dv/results/'
    os.chdir(project_path)
    
   # Получение значения из XCom
    df_crm_bot_actions_mrf1 = kwargs['ti'].xcom_pull(key='df_crm_bot_actions_mrf1')
    
    df_crm_bot_nature_mrf1 = kwargs['ti'].xcom_pull(key='df_crm_bot_actions_mrf1')
    
    with pd.ExcelWriter(f'{project_path}/df_crm_bot_actions_mrf1.xlsx', engine = 'xlsxwriter') as сw:
        
        df_crm_bot_actions_mrf1.to_excel(сw, '1', index_label=False, index=False, header=True)
    
    with pd.ExcelWriter(f'{project_path}/df_crm_bot_nature_mrf1.xlsx', engine = 'xlsxwriter') as сw:
        
        df_crm_bot_nature_mrf1.to_excel(сw, '1', index_label=False, index=False, header=True)
        
        
                   
with DAG(DAG_NAME, description='crm_bot_mrf1',

	schedule_interval='@hourly',

	catchup=False,

	max_active_runs=1,

	default_args = args,

	params={'labels':{'env': 'prod', 'priority': 'high'}}) as dag:


    extract_data_task = PythonOperator(task_id='extract_data_task', python_callable=extract_data, provide_context=True, dag=dag)
    
    process_data_task = PythonOperator(task_id='process_data_task', python_callable=process_data, provide_context=True, dag=dag)

    
    extract_data_task >> process_data_task





