import pandas as pd
from tqdm import tqdm
from datetime import datetime
import os


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
    
    return df_tmp

def main():
    date = datetime.now().date()
    
    # Чтение данных из CSV файла
    profit_data = pd.read_csv('dags/data/profit_table.csv')

    print(profit_data)
    
    # Получаем текущую дату в нужном формате
    current_date = '2024-03-01'    # datetime.datetime.now().strftime('%Y-%m-%d')  # Формат YYYY-MM-DD

    # Обработка данных
    transform_data =  transfrom(profit_data, current_date)
    
    # Имя выходного файла, в который будут дописываться данные
    output_filename = 'dags/data/flags_activity.csv'

    print("Что на выходе: ", transform_data)
    
    # Проверка существования файла
    file_exists = os.path.isfile(output_filename)

    # Сохранение обработанных данных с добавлением в конец файла
    if not os.path.exists(output_filename):
        transform_data.to_csv(output_filename, mode='w', header=True, index=False)
    else:
        transform_data.to_csv(output_filename, mode='a', header=False, index=False)

if __name__ == "__main__":
    main()