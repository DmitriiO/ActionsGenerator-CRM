# Создание БД в GP для сохрания активностей (для каждого МРФ отдельная)

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

# Создание БД в GP для сохрания данных о подключении услуг в ИНН (для каждого МРФ отдельная)
create table crm_bot_nature_mrf1(
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
