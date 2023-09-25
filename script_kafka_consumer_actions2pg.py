from kafka import KafkaConsumer
import pg8000
import json
from datetime import datetime
import sys

# Настройка kafka consumer
topic_name = 'crm_bot_actions_mrf1'
bootstrap_servers='vm-strmng-s-1.test.local:9092'
group_id = topic_name
auto_offset_reset = 'latest' # Начать чтение с момента остановки vs earliest
enable_auto_commit = True # Автоматически фиксировать смещение
auto_commit_interval_ms = 1000 # Интервал автоматической фиксации смещения (1 секунда)

# Настройка gодключения к базе данных GreenPlum
target_db = 'postgres'
user = 'orlovdv'
password = 'qr559'
host = '192.168.77.21'
port = 5432
table_name = topic_name
insert_query = f"INSERT INTO {table_name}(bot, user_id, date, filial, week, text1, text2, text3, text4, text5, text6, text7, text8, text9, text10, text11, text12, text13, text14, text15) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                

# Функция для чтения и отправки данных из Kafka в GP
def load_topic_kafka_to_GP():
    # Подключение к базе данных GP
    try:
        conn = pg8000.connect(database=target_db, user=user, password=password, host=host, port=port)
        
    except:

        print(f'Ошибка подключения к {target_db}')
        #  Выход из скрипта при ошибке подключения к БД
        sys.exit()

    else:
        # Создание kafka consumer
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
            auto_commit_interval_ms=auto_commit_interval_ms
        )
        
        for transaction in consumer:
                # Получение данных из сообщения и парсинг JSON
                data = json.loads(transaction.value.decode('utf-8'))
                bot = str(topic_name),
                user_id = int(data['user_id']),
                date = str(data['date']),
                filial = str(data['filial']),
                week = str(data['week']),
                text1 = str(data['text1']),
                text2 = str(data['text2']),
                text3 = str(data['text3']),
                text4 = str(data['text4']),
                text5 = str(data['text5']),
                text6 = str(data['text6']),
                text7 = str(data['text7']),
                text8 = str(data['text8']),
                text9 = str(data['text9']),
                text10 = str(data['text10']),
                text11 = str(data['text11']),
                text12 = str(data['text12']),
                text13 = str(data['text13']),
                text14 = str(data['text14']),
                text15 = str(data['text15'])

                # Вставка данных в таблицу, имя соответствует очереди сообщений

                try:
                    with conn.cursor() as curs:
                        
                        curs.execute(insert_query, (bot, user_id, date, filial, week, text1, text2, text3, text4, text5, text6, text7, text8, text9, text10, text11, text12, text13, text14, text15))    
                    # Коммит после каждой транзакции
                        conn.commit()
                except:
                    
                    print(f'Ошибка сохранения данных в GP {target_db}')
                    
                    conn.rollback()

    finally:
        # Закрытие соединения с Greenplum и Kafka Consumer
        conn.close()
        
        consumer.close()
        # Выход из скрипта
        sys.exit()

if __name__ == "__main__":
    load_topic_kafka_to_GP()
