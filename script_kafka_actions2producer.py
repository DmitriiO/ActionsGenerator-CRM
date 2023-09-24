from kafka import KafkaProducer
import time
import json
import os
import sqlite3
from datetime import datetime, timedelta, date, time
import sys

project_path='/usr/games/ActionsGenerator-CRM/'

os.chdir(project_path)

# Тема Kafka для подписки (по умолчанию) название бота в каждом МРФ
topic_name = 'crm_bot_actions_mrf1'

target_db = 'actions.db'

# Создание Kafka Producer
producer = KafkaProducer(bootstrap_servers='vm-strmng-s-1.test.local:9092',
                         value_serializer=lambda v:json.dumps(v).encode('utf-8'))

# Функция для чтения и отправки данных из SQLite в Kafka
def read_and_send_data_to_kafka():
    # Подключение к базе данных SQLite
    try:
        conn = sqlite3.connect(target_db)
        cursor = conn.cursor()
    # Получение последнего смещения из метаданных
        cursor.execute('SELECT last_transfer_timestamp FROM last_transfer_metadata')
        last_offset = cursor.fetchone()[0]
    
    # Выбор новых транзакций со смещением больше last_offset
        cursor.execute('SELECT * FROM user_message WHERE date > ?', (last_offset,))
        new_transactions = cursor.fetchall()
    
    except:
        
        print(f'Ошибка подключения/чтения {target_db}')
    #  Выход из скрипта при ошибке подключения к БД
        sys.exit()
   
    else:
        # Отправка новых транзакций в Kafka в виде словаря по каждой отдельно
        for transaction in new_transactions:
    
            message = {
                'bot' : topic_name,
                'user_id' : transaction[1],
                'date' : transaction[2],
                'filial' : transaction[3],
                'week' : transaction[4],
                'text1' : transaction[5],
                'text2' : transaction[6],
                'text3' : transaction[7],
                'text4' : transaction[8],
                'text5' : transaction[9],
                'text6' : transaction[10],
                'text7' : transaction[11],
                'text8' : transaction[12],
                'text9' : transaction[13],
                'text10' : transaction[14],
                'text11' : transaction[15],
                'text12' : transaction[16],
                'text13' : transaction[17],
                'text14' : transaction[18],
                'text15' : transaction[19]
            }

            producer.send(topic_name, value = message)
        
        # Обновление смещения в метаданных после отправки транзакций в kafka - timestamp последней в стриме транзакции
        cursor.execute('UPDATE last_transfer_metadata SET last_transfer_timestamp = ?', (transaction[2],))

        # Закрытие Kafka Producer и соединения с базой данных
        producer.close()
        conn.commit()
        conn.close()
        # Выход из скрипта
        sys.exit()
    
if __name__ == "__main__":
    read_and_send_data_to_kafka()