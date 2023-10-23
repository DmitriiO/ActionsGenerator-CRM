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
password = '***'
host = '192.168.77.21'
port = 5432
table_name = topic_name
insert_query = f"INSERT INTO {table_name}(bot, user_id, date, filial, week, text1, text2, text3, text4, text5, text6, text7, text8, text9, text10, text11, text12, text13, text14, text15) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"


# Функция для чтения и отправки данных из Kafka в GP
def load_topic_kafka_to_GP():
    # Подключение к базе данных GP
    try:
        conn = pg8000.connect(database=target_db, user=user, password=password, host=host, port=port)
        cursor = conn.cursor()
        print('Connection PG - ok!')        
    
    except Exception as e:

        print(f'Ошибка подключения/чтения {target_db}: {str(e)}')
    
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

                try:
                    # Получение данных из сообщения и парсинг JSON
                    data = json.loads(transaction.value.decode('utf-8'))
                    bot = topic_name
                    user_id = data['user_id']
                    date = data['date']
                    filial = data['filial']
                    week = data['week']
                    text1 = data['text1']
                    text2 = data['text2']
                    text3 = data['text3']
                    text4 = data['text4']
                    text5 = data['text5']
                    text6 = data['text6']
                    text7 = data['text7']
                    text8 = data['text8']
                    text9 = data['text9']
                    text10 = data['text10']
                    text11 = data['text11']
                    text12 = data['text12']
                    text13 = data['text13']
                    text14 = data['text14']
                    text15 = data['text15']

                    # Вставка данных в таблицу, имя соответствует очереди сообщений

                    cursor.execute(insert_query, (bot, user_id, date, filial, week, text1, text2, text3, text4, text5, text6, text7, text8, text9, text10, text11, text12, text13, text14, text15))
                    
                    conn.commit()

                except Exception as e:
                    
                    print(f'Ошибка обработки и/или сохранения сообщения: {str(e)}')
                    
                    conn.rollback()  # Откат транзакции в случае ошибки
    
    finally:
        # Закрытие соединения с Greenplum и Kafka Consumer
        if 'cursor' in locals() and cursor:
            
            cursor.close()
        
        if 'conn' in locals() and conn:
            
            conn.close()
        
        if 'consumer' in locals() and consumer:
            
            consumer.close()
            
def main():
    # Проверка очереди сообщений каждые 2 минуты 
    while True:
        
        load_topic_kafka_to_GP()
        
        time.sleep(120)
            
            
if __name__ == "__main__":
    main()
