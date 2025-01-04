import pandas as pd
from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta


def transfrom(profit_table, date):
    """ Собирает таблицу флагов активности по продуктам
        на основании прибыли и количеству совершёных транзакций
        
        :param profit_table: таблица с суммой и кол-вом транзакций
        :param date: дата расчёта флагоа активности
        
        :return df_tmp: pandas-датафрейм флагов за указанную дату
    """
    start_date = pd.to_datetime(date) - pd.DateOffset(months=2)
    end_date = pd.to_datetime(date) + pd.DateOffset(months=1)
    date_list = pd.date_range(
        start=start_date, end=end_date, freq='M'
    ).strftime('%Y-%m-01')
    
    print(date_list)

    df_tmp = (
        profit_table[profit_table['date'].isin(date_list)]
        .drop('date', axis=1)
        .groupby('id')
        .sum()
    )
    
    product_list = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
    for product in tqdm(product_list):
        df_tmp[f'flag_{product}'] = (
            df_tmp.apply(
                lambda x: x[f'sum_{product}'] != 0 and x[f'count_{product}'] != 0,
                axis=1
            ).astype(int)
        )
        
    df_tmp = df_tmp.filter(regex='flag').reset_index()

def main():
    date = datetime.now().date()
    
    # Чтение данных из CSV файла
    profit_data = pd.read_csv('../data/profit_table.csv')
    
    # Получаем текущую дату в нужном формате
    current_date = '2024-01-01'    # datetime.datetime.now().strftime('%Y-%m-%d')  # Формат YYYY-MM-DD

    # Обработка данных
    transform_data =  transfrom(profit_data, current_date)
    
    # Имя выходного файла, в который будут дописываться данные
    output_filename = '../data/flags_activity.csv'
    
    # Проверка существования файла
    file_exists = os.path.isfile(output_filename)

    # Сохранение обработанных данных с добавлением в конец файла
    transform_data.to_csv(output_filename, mode='a', index=False, header=not file_exists)

# DAG
default_args = {
    'max_active_runs': 1,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=0)
}
dag1 = DAG(
        'transform_dag_lastin_maxim',
        default_args=default_args,
        description='Запуск задани №1',
        start_date=datetime(2024, 4, 5),
        schedule_interval='@monthly',
        catchup=False
        )

operation = PythonOperator(task_id = 'transform_dag_lastin_maxim',
            python_callable = main,
            depends_on_past=True,   # Задача будет зависеть от завершения предыдущей задачи
            task_concurrency=1,     # Ограничения одновременного выполнения 1 задачи
            dag = dag1)

operation
