1. Транспорт синтетических данных модели CRM бота со слоя хранения БД SQLite в распределенное хранилище данных БД GreenPlum.

Описание:

Поскольку БД SQLite не доступна в онлайн режиме, запуск бота должен происходить на сервере с предварительной установкой Apache.Kafka.
Чтение очереди сообщений происходит в KafkaConsumer в корпоративном кластере RT.Streaming.
Для каждого МРФ запускается отдельный бот и формируется отдельная очередь сообщений (topic name).

Хранение данных в GP происходит по каждому МРФ отдельно, в режимах distributed by randomly, append only и в колоночной ориентации.

2. Формирование витрин данных и построение дашбордов.

Описание:

Формирование витрин данных происходит с периодичностью, заданной в DAG файле AirFlow. Формат выходных данных excel файлы для каждого дашборда.
