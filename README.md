**Решение**

1. Транспорт синтетических данных модели CRM бота со слоя хранения БД SQLite в распределенное хранилище данных БД GreenPlum.

Описание:

Поскольку БД SQLite не доступна в онлайн режиме, для передачи данных в КХД можно воспользоваться брокером сообщений, в этом случае запуск бота должен происходить на сервере с предварительной установкой и настройкой Apache.Kafka.
Чтение очереди сообщений происходит в KafkaConsumer в корпоративном кластере RT.Streaming.
Для каждого МРФ запускается отдельный бот и формируется отдельная очередь сообщений (topic name) в KafkaProducer.

Хранение данных в GP происходит по каждому МРФ отдельно, в режимах distributed by randomly, append only и в колоночной ориентации.

2. Формирование витрин данных и построение дашбордов.

Описание:

Формирование витрин данных происходит с периодичностью, заданной в DAG файле AirFlow. Формат выходных данных excel файлы для каждого дашборда, а также дашборд в Superset
___________________________________________________________________________________________________________________________________________________
Уровень чат-бота:

actions.db - база чат-бота с данными по активности (лидогенерация)

nature.db - база чат-бота с данными по подключению услуг (конверсия)

generator_bot.py - имитация работы менеджеров в чат-боте (синтетические данные)


Уровень ETL:

_Apache.Kafka_

script_kafka_actions2producer.py - передача новых транзакций по активности в очередь

script_kafka_nature2producer.py - передача новых транзакций по подключению услуг в очередь

_GreenPlum_

create GP table query.sql - скрипт создания таблиц в БД GreenPlum

script_kafka_consumer_actions2gp.py - чтение очереди транзакций с активностями и сохранение в базу GP crm_bot_actions_mrf1

script_kafka_cinsumer_nature2gp.py - чтение очереди транзакций по подключениям и сохранение в базу GP crm_bot_nature_mrf1

_Apache.Airflow_

dag_data2excel.py - выгрузка данных из 2-х таблиц с данными из БД GP и сохранение в Excel файлы:

  /opt/airflow/dags/nature_mrf1.xlsx

  /opt/airflow/dags/actions_mrf1.xlsx

