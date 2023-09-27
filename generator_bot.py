#!/usr/bin/env python
# coding: utf-8

# In[40]:


import os
import sqlite3
from datetime import datetime, timedelta, date, time
import random
import json
import csv
import logging
import time
os.system('clear')


# In[41]:


project_path='/data/data/com.termux/files/home/storage/shared/de_project/'
os.chdir(project_path)


# In[3]:


# Логгирование с выводом в файл
logging.basicConfig(level = logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s (Line: %(lineno)d)", filename='debug.log', filemode='w', encoding='UTF-8')
logger = logging.getLogger(__name__)
# print(logger.handlers)


# In[4]:


# Загрузка базы ИНН и формирование списка ИНН-Наименование клиента
file = 'inn.csv'
inn_lst = []
with open(file) as f:
    reader = csv.DictReader(f)

    for row in reader:
        inn_lst.append(list(row.values()))


# In[5]:


# df_all_inn.loc[df_all_inn.base =='B2B'][['INN', 'cl_name']].drop_duplicates().head(3000).to_csv('inn.csv', index = False)


# In[6]:


# Коннект с БД по активности
def action_connection(func):
        
    def inner(*args, **kwargs):
            
        with sqlite3.connect('actions.db') as action_conn:

            try:
                res = func(*args, action_conn=action_conn, **kwargs)
                    
                return res
                
            except:
                    
                logger.info('Ошибка при установлении соединения action_conn')
                    
                action_conn.rollback()

    return inner


@action_connection
def init_action_db(action_conn):
    
    c = action_conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS user_message (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL,\
    date date, filial NOT NULL, week NOT NULL, text1 NOT NULL, text2 NOT NULL, text3 NOT NULL, text4 NOT NULL,\
    text5 NOT NULL, text6 NOT NULL, text7 NOT NULL, text8 NOT NULL, text9 NOT NULL, text10 NOT NULL,text11 NOT NULL, text12 NOT NULL, text13 NOT NULL, text14 NOT NULL, text15 NOT NULL)")  
    action_conn.commit()

@action_connection
def add_message_action(action_conn, user_id: int, date: str, filial: str, week: str, text1: str, text2: str, text3: str, text4: str, text5: str, text6: str, text7: str, text8: str, text9: str, text10: str, text11: str,text12: str, text13: str, text14: str, text15: str):
    
    c = action_conn.cursor()
    c.execute("INSERT INTO user_message (user_id, date, filial, week, text1, text2, text3, text4, text5, text6, text7, text8, text9, text10, text11, text12, text13, text14, text15) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (user_id, date, filial, week, text1, text2, text3, text4,  text5,  text6,  text7,  text8,  text9,  text10, text11, text12, text13, text14, text15))
    action_conn.commit()

@action_connection
def check_inn(action_conn, text2: str, text8: str, nof: int):
    
    c = action_conn.cursor()
    c.execute("SELECT user_id FROM user_message WHERE text2=? AND text8=? AND filial=? GROUP BY user_id", (text2, text8, nof))
    
    return c.fetchall()


# In[7]:


# Коннект с БД по подключениям
def nature_connection(func):
    
    def inner(*args, **kwargs):
            
        with sqlite3.connect('nature.db') as nature_conn:
                
            try:
                res = func(*args, nature_conn=nature_conn, **kwargs)
                    
                return res
                
            except:
                    
                logger.info('Ошибка при установлении соединения nature_conn')
                    
                nature_conn.rollback()

    return inner


# Сохранение сообщений в БД
@nature_connection
def init_nature_db(nature_conn):
    
    c = nature_conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS user_message (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL,\
    date date, filial NOT NULL, week NOT NULL, text1 NOT NULL, text2 NOT NULL, text3 NOT NULL, text4 NOT NULL,\
    text5 NOT NULL, text6 NOT NULL, text7 NOT NULL, text8 NOT NULL, text9 NOT NULL, text10 NOT NULL,text11 NOT NULL, text12 NOT NULL, text13 NOT NULL, text14 NOT NULL, text15 NOT NULL)")  
    nature_conn.commit()
    
@nature_connection
def add_message_nature(nature_conn, user_id: int, date: str, filial: str, week: str, text1: str, text2: str, text3: str, text4: str, text5: str, text6: str, text7: str, text8: str, text9: str, text10: str, text11: str,text12: str, text13: str, text14: str, text15: str):
    
    c = nature_conn.cursor()
    c.execute("INSERT INTO user_message (user_id, date, filial, week, text1, text2, text3, text4, text5, text6, text7, text8, text9, text10, text11, text12, text13, text14, text15) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (user_id, date, filial, week, text1, text2, text3, text4,  text5,  text6,  text7,  text8,  text9,  text10, text11, text12, text13, text14, text15))
    nature_conn.commit()


# In[ ]:


# list1 = list(set([828798031, 456798031, 828798123, 534798031, 786798031, 986798031, 238798031,
#          254798031,543235654, 456721234, 828798030, 452798031, 328798123, 444798031,
#          666798031, 986798031, 648798031, 764798031, 993235654, 106721234, 554321432]))
# list2 = ['Марина C.', 'Ольга В.', 'Илья И.', 'Николай Н.', 'Николай Д.', 'Николай К.', 'Владимир П.',
#          'Владимир А.','Владимир Е.', 'Владимир Щ.', 'Андрей П.', 'Андрей О.', 'Андрей A.', 'Виктория С.',
#          'Елена З.', 'Елена А.', 'Елена Р.', 'Александр К.', 'Ирина Э.', 'Анастасия В.']
# list(zip(list1, list2))


# In[46]:


# Формирование списка менеджеров user_id + Имя + номер филиала

users_lst = [(554321432, 'Марина C.', '1'),
             (828798123, 'Ольга В.', '1'),
             (328798123, 'Илья И.', '1'),
             (444798031, 'Николай Н.', '2'),
             (452798031, 'Николай Д.', '2'),
             (543235654, 'Николай К.', '2'),
             (993235654, 'Владимир П.', '3'),
             (828798030, 'Владимир А.', '3'),
             (764798031, 'Владимир Е.', '3'),
             (648798031, 'Владимир Щ.', '4'),
             (666798031, 'Андрей П.', '4'),
             (786798031, 'Андрей О.', '4'),
             (534798031, 'Андрей A.', '5'),
             (456798031, 'Виктория С.', '5'),
             (828798031, 'Елена З.', '12'),
             (986798031, 'Елена А.', '12'),
             (238798031, 'Елена Р.', '12'),
             (254798031, 'Александр К.', '12'),
             (456721234, 'Ирина Э.', '15'),
             (106721234, 'Анастасия В.', '15')
              ]


# In[10]:


# Функция возвращает список месяцев, перенося текущий месяц на следующий год
def calendar_buttons():
    
    curr_month = date.today().month
    curr_y = date.today().year
    lst_res_month = [curr_month]
    name_of_month = {1 :'Январь',2 : 'Февраль',3: 'Март',4: 'Апрель',5 : 'Май',6 : 'Июнь',7 : 'Июль',8 : 'Август',9 : 'Сентябрь',10 : 'Октябрь',11 : 'Ноябрь',12 : 'Декабрь'}  
    # curr_month = date(2023, 12, 27).month
    # curr_y = date(2023, 12, 27).year
    lst_month_past = [m for m in range(1,13) if m <= curr_month]
    lst_month_past = [lst_past_res for lst_past_res in map(lambda x: name_of_month.get(x)+'.'+str(curr_y+1), lst_month_past)]
    lst_month_next = [m for m in range(1,13) if m > curr_month]
    lst_month_next = [lst_next_res for lst_next_res in map(lambda x: name_of_month.get(x)+'.'+str(curr_y), lst_month_next)]
    lst_res_month = [name_of_month.get(lst_res_month[0])+'.'+str(curr_y)]
    lst_res_month.extend(lst_month_next)
    lst_res_month.extend(lst_month_past)
    
    return lst_res_month


# In[11]:


# Генератор данных по активности
def generate_random_data_activity():
    
# Параметры для генерации данных по активности
    type_activity = ['КП', 'Звонок', 'Встреча', 'ВКС', 'ВКС(PS)', 'АКС']
    target_services = ['WiFi', 'Видео', 'ВАТС', 'ВЦОД']
    act_result = ['Заявка', 'В работе', 'Отказ']
    type_base = 'B2B'
    comment = 'Комментарий не требуется'
    if_predict = ['0', 'Да']
    type_service = 'ОСНОВНАЯ УСЛУГА'
    contact_number = '0'

    # Выбор случайных значений    
    user_data = random.choice(users_lst)
    
    client = random.choice(inn_lst)
    
    user_id = user_data[0]
# Для генерации данных предыдущих периодов                            
    # timestamp = (datetime.now() - timedelta(days=random.randint(0,365)))
# Для генерации текущих данных онлайн
    timestamp = datetime.now()
    
    filial_number = user_data[2]
    
    week_number = str(timestamp.isocalendar()[1])
    
    user_name =  user_data[1]
    
    inn_number = client[0]
    
    num_act = check_inn(text2 = inn_number, text8 = 'B2B', nof = filial_number)
    
    if num_act != []:
            
        num_act = 'Повторная'
# Дата повторной активностти всегда больше даты первичной   
        timestamp = datetime.now()
        
    else:
            
        num_act = 'Первичная'
    
    type_act = random.choice(type_activity)
    
    type_serv = random.choice(target_services)
        
    act_res = random.choice(act_result)
    
    predict = random.choice(if_predict)
    
    if predict == 'Да':
        
        install = random.randint(0,1000000)
        
        service = random.randint(500,200000)
        
        predict_date = random.choice(calendar_buttons())
    
    else:
        
        install = 0
        
        service = 0
        
        predict_date = 0
    
    cl_name = client[1]
    
  
    data = {
            'gen_type' : 'lids',
            'user_id' : user_id,
            'timestamp' : timestamp,
            'filial_number' : filial_number,
            'week_number' : week_number,
            'user_name' : user_name, 
            'inn_number' : inn_number,
            'num_act' : num_act,
            'type_act' : type_act,
            'type_serv' : type_serv,
            'act_res' : act_res,
            'comment' : comment,
            'type_base' : type_base,
            'predict' : predict,
            'predict_date' : predict_date,
            'install' : install,
            'service' : service,
            'cl_name' : cl_name,
            'type_service' : type_service,
            'contact_number' : contact_number
    } 

    return data


# In[12]:


def generate_random_data_sales():
    
# Параметры для генерации данных по продажам
    type_activity = ['КП', 'Звонок', 'Встреча', 'ВКС', 'ВКС(PS)', 'АКС']
    target_services = ['WiFi', 'Видео', 'ВАТС', 'ВЦОД']
    type_base = 'B2B'
    segments = ['Прочие 3KB2B', 'ТОП 3KB2B', 'ФХ Прочие 3KB2B', 'ФХ ТОП 3КВ2В']
    
    user_data = random.choice(users_lst)
    
    client = random.choice(inn_lst)
    
    user_id = user_data[0]
                            
# Для генерации данных предыдущих периодов                            
    timestamp = (datetime.now() - timedelta(days=random.randint(0,365)))
# Для генерации текущих данных онлайн
    # timestamp = datetime.now()
    
    filial_number = user_data[2]
    
    week_number = str(timestamp.isocalendar()[1])
    
    user_name =  user_data[1]
    
    inn_number = client[0]
    
    num_act = check_inn(text2 = inn_number, text8 = 'B2B', nof = filial_number)
    
    if num_act != []:
            
        num_act = 'Повторная'
        
    else:
            
        num_act = 'Первичная'
    
    type_act = random.choice(type_activity)
    
    type_serv = random.choice(target_services)
        
    act_res = 'Подключение'
    
    quantity = random.randint(1,50)
    
    segment = random.choice(segments)
    
    cur_month = datetime.now().month
 
    install = random.randint(0,1000000)
        
    service = random.randint(500,200000)
    
    cl_name = client[1]
    
  
    data = {
            'gen_type' : 'sales',
            'user_id' : user_id,
            'timestamp' : timestamp,
            'filial_number' : filial_number,
            'week_number' : week_number,
            'user_name' : user_name, 
            'inn_number' : inn_number,
            'num_act' : num_act,
            'type_act' : type_act,
            'type_serv' : type_serv,
            'act_res' : act_res,
            'quantity' : quantity,
            'type_base' : type_base,
            'segment' : segment,
            'cur_month' : cur_month,
            'install' : install,
            'service' : service,
            'cl_name' : cl_name
    } 

    return data


# In[ ]:


def main():
    
    init_action_db()
    init_nature_db()
    
    print('Start generation...')
    logger.info('Start generation...')
    
    while True:
        
        gen_type = random.choice(['sales', 'lids', 'lids', 'lids', 'pass'])
        
        if gen_type == 'lids':
# Сохранение транзакций по активностям в базе ИНН в БД         
            data = generate_random_data_activity()

            add_message_action(
            
                user_id = data.get('user_id'),
                date =  data.get('timestamp'),
                filial =  data.get('filial_number'),
                week =  data.get('week_number'),
                text1 =  data.get('user_name'), 
                text2 =  data.get('inn_number'),
                text3 =  data.get('num_act'),
                text4 =  data.get('type_act'),
                text5 =  data.get('type_serv'),
                text6 =  data.get('act_res'),
                text7 =  data.get('comment'),
                text8 =  data.get('type_base'),
                text9 =  data.get('predict'),
                text10 =  data.get('predict_date'),
                text11 =  data.get('install'),
                text12 =  data.get('service'),
                text13 =  data.get('cl_name'),
                text14 =  data.get('type_service'),
                text15 =  data.get('contact_number')
            )
            logger.info('New lids transaction - DONE!')
            logger.info('Generator wait for 120 sec.')
            time.sleep(60)
        
        elif gen_type == 'sales':
# Сохранение транзакций по продажам в БД        
            data = generate_random_data_sales()
       
            add_message_nature(
            
                user_id = data.get('user_id'),
                date =  data.get('timestamp'),
                filial =  data.get('filial_number'),
                week =  data.get('week_number'),
                text1 =  data.get('user_name'), 
                text2 =  data.get('inn_number'),
                text3 =  data.get('num_act'),
                text4 =  data.get('type_act'),
                text5 =  data.get('type_serv'),
                text6 =  data.get('act_res'),
                text7 =  data.get('quantity'),
                text8 =  data.get('type_base'),
                text9 =  data.get('segment'),
                text10 =  data.get('cur_month'),
                text11 =  data.get('install'),
                text12 =  data.get('service'),
                text13 =  data.get('cl_name'),
                text14 =  '0',
                text15 =  '0'
            )
            logger.info('New sales transaction - DONE!')
            logger.info('Generator wait for 360 sec.')
            time.sleep(360)
        
        else:
            logger.info('Do nothing...')
            logger.info('Generator wait for 120 sec.')
            
            time.sleep(120)

if __name__ == "__main__":
    main()

