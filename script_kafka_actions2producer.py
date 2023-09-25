from kafka import KafkaProducer
import time
import json
import os
import sqlite3
from datetime import datetime, timedelta, date
import sys

# project_path='/usr/games/ActionsGenerator-CRM/'
project_path='/home/orlov-dv/ActionsGenerator-CRM/'

os.chdir(project_path)

# Тема Kafka для подписки (по умолчанию) название бота в каждом МРФ
target_db = 'actions.db'
topic_name = 'crm_bot_actions_mrf1'
bootstrap_servers='vm-strmng-s-1.test.local:9092'


# Функция для чтения и отправки данных из SQLite в Kafka
def read_and_send_data_to_kafka():
    # Подключение к базе данных SQLite
    try:
        conn = sqlite3.connect(target_db)
        cursor = conn.cursor()
        print('Connection SQLite - ok!')
    # Получение последнего смещения из метаданных
        cursor.execute('SELECT last_transfer_timestamp FROM last_transfer_metadata')
        last_offset = cursor.fetchone()[0]

    # Выбор новых транзакций со смещением больше last_offset
        cursor.execute('SELECT * FROM user_message WHERE date > ?', (last_offset,))
        new_transactions = cursor.fetchall()

    except Exception as e:

        print(f'Ошибка подключения/чтения {target_db} {str(e)}')

    else:
	# Проверка на наличие новых данных
        if new_transactions != []:

            print(f'Передается {len(new_transactions)} новых транзакций')

            # Создание Kafka Producer
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v:json.dumps(v).encode('utf-8'))

            # Отправка новых транзакций в Kafka в виде словаря по каждой отдельно

            count_trs = 0

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

                try:

                    producer.send(topic_name, value = message)

                except Exception as e:

                    print(f'Ошибка передачи сообщений в Kafka Producer : {str(e)}')

                else:
                    count_trs +=1
                    
                    print(f'{count_trs} новых транзакций чат-бота {topic_name} успешно переданы!')

            # Обновление смещения в метаданных после отправки транзакций в kafka - timestamp последней в стриме транзакции
            try:
                cursor.execute('UPDATE last_transfer_metadata SET last_transfer_timestamp = ?', (transaction[2],))
            
                conn.commit()
            
            except Exception as e:
                    
                print(f'Ошибка обработки и/или сохранения сообщения: {str(e)}')
                    
                conn.rollback()  # Откат транзакции в случае ошибки
            
            else:
                
                print(f'Сохранен timestamp последней в стриме транзакции: {transaction[2]}')

        else:
            print(f'Новые транзакций в чат-бота {topic_name} отсутствуют')

    
    finally:
        # Закрытие соединения с SQLite и Kafka Producer
        if 'cursor' in locals() and cursor:
            
            cursor.close()
        
        if 'conn' in locals() and conn:
            
            conn.close()
        
        if 'consumer' in locals() and consumer:
            
            producer.close()

def main():

    # Запуск проверок наличия новых транзакций с интервалом 5 минут
    while True:

        read_and_send_data_to_kafka()

        time.sleep(300)

if __name__ == "__main__":
    main()