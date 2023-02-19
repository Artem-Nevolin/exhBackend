
from PyQt5 import QtCore, QtGui, QtWidgets # для создания окна с кнопками

from PyQt5.QtCore import QThread # для разделения задач на потоки

import pyodbc
import csv
import time
import datetime




##################################### считываю из Kafka###############################################
# from kafka import KafkaConsumer
# consumer = KafkaConsumer('my_fovorite_topic')
#
# for msg in consumer:
#     #print(msg)
#     pass
         ########################### другой вариант подключения Kafka (https://machinelearningmastery.ru/getting-started-with-apache-kafka-in-python-604b3250aa05/)


# print('Running Consumer..')
# parsed_records = []
# topic_name = 'raw_recipes'
# parsed_topic_name = 'parsed_recipes'
#
# consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
#                              bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
# for msg in consumer:
#     html = msg.value
#     result = parse(html)
#     parsed_records.append(result)
# consumer.close()
# sleep(5)
#
# if len(parsed_records) > 0:
#     print('Publishing records..')
#     producer = connect_kafka_producer()
#     for rec in parsed_records:
#         publish_message(producer, parsed_topic_name, 'parsed', rec)

###########################################################################################3

################################# Ещё один вариант подключения к Кафка #######################
# Используйте 127.0.0.1 вместо localhost или любого другого IP-адреса, относящегося к вашему варианту использования. Изменение localhost:9092 на 127.0.0.1:9092 сработало для меня.
#
# from kafka import KafkaConsumer
# consumer = KafkaConsumer('topicname',bootstrap_servers=['127.0.0.1:9092'])
# print(consumer.config)
# print(consumer.bootstrap_connected())



###########################  читаю UserLog.csv файл ##############################################################

# указываю путь, где находится логфайл для чтения
#path = r'C:\Users\User\Desktop\Хакатон\Новая папка (2)\Новая папка (2)\Exh3_Vibr_2.csv.csv'
# создаю пустой список, в который перенесу все данные из Logfile
csv_list = []
with open('Exh3_Vibr_1.csv', 'r', newline='', encoding='ANSI') as f:

    reader = csv.reader(f, quoting=csv.QUOTE_NONE)  # delimiter=' ' - ставит запятую вместо пробела
    for nr in reader:  #
        if len(nr) == 1:
            stroka1 = nr[0]
            #print(1)
        if len(nr) == 2:
            stroka2_temp = nr[0]
            stroka2_2=stroka2_temp[0:19]
            stroka2 = datetime.datetime.strptime((stroka2_2).strip(), '%Y-%m-%d %H:%M:%S')
            stroka3=stroka2_temp[24:25]
            stroka4_temp = nr[1]
            stroka4=stroka4_temp[:2]
            stroka5=[stroka2,stroka3+'.'+stroka4]
            csv_list.append(stroka5)
            #print(stroka2,stroka3, stroka4)
            #print(stroka5)
#print(len(csv_list))
print('Дата1', csv_list[0][0])
print('Дата2', csv_list[1][0])
print('Разность Дата1 - Дата2',csv_list[0][0]-csv_list[1][0])

time_temp1=int((str(csv_list[0][0]))[11:13])
time_temp5= int((str(csv_list[14111][0]))[11:13])

if time_temp1 != 0:
    time_temp2=int((str(csv_list[0][0]))[11:13])-2
    time_temp3=str(csv_list[0][0])[:11]+str(time_temp2)+str(csv_list[0][0])[13:]
    time_temp4=datetime.datetime.strptime((time_temp3).strip(), '%Y-%m-%d %H:%M:%S')
    print('Дата1 - 2 часа = ', time_temp4)

if time_temp5==0: # проверка, если нуль часов, то из нуля вычитать не корректно, поэтому вычитаем из 24
    time_temp6 = 24-2
    time_temp7 = str(csv_list[14111][0])[:11]+str(time_temp6)+str(csv_list[14111][0])[13:]

    time_temp9 = datetime.datetime.strptime((time_temp7).strip(), '%Y-%m-%d %H:%M:%S')

print("Дата3", (str(csv_list[14111][0])))
print('Дата3-2часа =', time_temp9)










            # print(stroka2_temp)
            # print(stroka3)
            #print(2)
    #     for k in nr:
    #         val1=k[0:19]
    #         val2=k[0:22]
    #         print(val1, val2)

        # stroka = nr[0]
        # k=stroka[:28]

        #val_1 = datetime.datetime.strptime((stroka[0:19]).strip(), '%Y-%m-%d %H:%M:%S')
        #val_1 = stroka[0:19]
        #val_2= datetime.datetime.strptime((val_1.strip(), '%Y-%m-%d %H:%M:%S')
        # print(k)
        # print(type(k))


#         if len(nr) == 1:
#             stroka = nr[0]
#             val_1 = datetime.datetime.strptime((stroka[0:19]).strip(), '%Y-%m-%d %H:%M:%S')
#             val_2 = (stroka[20:33]).strip()
#             val_3 = (stroka[34:84]).strip()
#             val_4 = (stroka[85:230]).strip()
#             val_5 = (stroka[231:269]).strip()
#             val_6 = ''
#             val_7 = (stroka[270:330]).strip()
#
#             spisok_tuple = (val_1, val_2, val_3, val_4, val_5, val_6, val_7,)
#             csv_list.append(spisok_tuple)
#         elif len(nr) == 2:
#             #print(nr)
#             stroka2 = nr[0]
#             stroka3 = nr[1]
#             val_1 = datetime.datetime.strptime((stroka2[0:19]).strip(), '%Y-%m-%d %H:%M:%S')  # sDatatime
#             val_2 = (stroka2[20:33]).strip()  # sFullUserName ("Оператор")
#             val_3 = (stroka2[34:84]).strip()  # sMexName ("Конвейер №..")
#             val_4 = (stroka2[85:200]).strip() + ',' + (
#             stroka3[0:50]).strip()  # sTagDesc(название команды, "Конанды СТОП....)
#             val_5 = (stroka3[51:141]).strip()  # sValue ("Подана")
#             val_6 = ''  # это для столбца sENGUNITS. этот столбец не заполняется
#             val_7 = (stroka3[141:190]).strip()  # # это для тега команды из СКАДы
#
#             spisok_tuple2 = (val_1, val_2, val_3, val_4, val_5, val_6, val_7,)
#             csv_list.append(spisok_tuple2)
#
# print('csv_list[3669]', csv_list[3669])











# # создаю пустой список, в который перенесу все данные из Logfile
# csv_list = []
#
# file1 = open('Exh3_Vibr.csv', "r")
#
# for nr in file1:
#     print(nr)

    # stroka = nr
    # val_1 = datetime.datetime.strptime((stroka[0:19]).strip(), '%Y-%m-%d %H:%M:%S')
    # val_2 = (stroka[20:33]).strip()
    # val_3 = (stroka[34:84]).strip()
    # val_4 = (stroka[85:230]).strip()
    # val_5 = (stroka[231:269]).strip()
    # val_6 = ''
    # val_7 = (stroka[270:330]).strip()
    # # собираю значения(строки) в кортеж
    # spisok_tuple = (val_1, val_2, val_3, val_4, val_5, val_6, val_7,)
    # csv_list.append(spisok_tuple)  # добавляю кортеж в список

#file1.close()