Транспорт синтетических данных модели CRM бота со слоя хранения БД SQLite в распределенное хранилище данных БД GreenPlum.

Описание:
Поскольку БД SQLite не доступна в онлайн режиме, запуск бота должен происходить на сервере с предварительной установкой Apache.Kafka.
Чтение очереди сообщений происходит в KafkaConsumer в корпоративном кластере RT.Streaming. Для запуска kafkaProducer используется DAG AirFlow.
Для каждого МРФ запускается свой бот и формируется отдельная очередь сообщений.

Хранение данных в GP происходит по каждому МРФ отдельно, в режимах distributed by randomly, append only и в колоночной ориентации.
create table crm_bot_actions_mrf1(
bot text,
user_id int,
date date,
filial text,
week text,
text1 text,
text2 text,
text3 text,
text4 text,
text5 text,
text6 text,
text7 text,
text8 text,
text9 text,
text10 text,
text11 text,
text12 text,
text13 text,
text14 text,
text15 text
)
with (
appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1
)
distributed randomly;
