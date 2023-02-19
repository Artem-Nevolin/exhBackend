# видос конекшен стринг https://yandex.ru/video/preview/11619706852423636432


from PyQt5 import QtCore, QtGui, QtWidgets # для создания окна с кнопками

from PyQt5.QtCore import QThread # для разделения задач на потоки

import pyodbc
import csv
import time
import datetime

######################## Считываю данные из conn_file_Exh_ для опредения последнего записанного времени


with open('conn_file_Exh3_Vibr2.txt', 'r', encoding='utf-8') as file:

    txt_lines = file.readlines()

txt_connection_string = txt_lines[1].strip() # connection string
#txt_logfile = txt_lines[4].strip()  # logfile
txt_time_ponit = txt_lines[7].strip()  # time_ponit
#txt_command = txt_lines[10].strip()  # command
#txt_status_command = txt_lines[13].strip()  # status_command

    ##################################################################################################################



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



#**********************************************************************************************************************
#**********************************************После подключения Кафка раскоментировать***********************************
#**********************************************************************************************************************
##################################### предлагаю такой вариант для проверки данных, получаемых из Kafka т.е. сравниваю
########### дату и время из Kafka с датой и временем из conn_file_Exh и если она больше то складываю с список.
# Если она меньше или равна, то выхожу из подключения с Кафка и отправляю данные в БД

# перед парсингом данных, необходим принтануть msg и посмотреть, в каком виде получаю данные из Kafka
# for msg in consumer:
#     print(msg) #смотрю, как отображается строки и далее
#     # это условие, когда отлавливаю новые данные т.е. время данных из Кафка, больше времени
#     # в текстовом файле, следовательно необходимо отправить их БД и на визуализацию
#
#     txt_time_ponit_norm = datetime.datetime.strptime((txt_time_ponit[0:19]).strip(), '%Y-%m-%d %H:%M:%S') # нормализую дату из вайла txt
#
#     data_kafka = [] # массив для временного хранения данных
#     if msg[0][0]> txt_time_ponit_norm:
#         # делаю обработку данных, чтобы положить в БД
#         str1_temp = msg[0] #!!!!!!!!!!!!!!!!!!!!!!!!!!!!! достаю полностью всю строку
#         str2_2 = str1_temp[0:19]
#         str2 = datetime.datetime.strptime((str2_2).strip(), '%Y-%m-%d %H:%M:%S') # возможно надо будет удалит- зависит от типа данных из kafka
#         str3 = str1_temp[24:25]
#         str4_temp = nr[1]
#         str4 = str4_temp[:2]
#         str5 = [str2, str3 + '.' + str4]
#         str6 = datetime.datetime.strptime((str5).strip(), '%Y-%m-%d %H:%M:%S')
#         val = (stroka[20:33]).strip()
#         spisok = (str6, val)
#         data_kafka.append(spisok)


#**********************************************************************************************************************
#**********************************************************************************************************************
#**********************************************************************************************************************
#####################################################################################################################








#********************************************************************************************************************
#*******************************временно читаю из файла, как будто из кафки, чтобы проверить алгоритм*************
#********************************************************************************************************************
###########################  читаю csv файл ##############################################################

# указываю путь, где находится логфайл для чтения
#path = r'C:\Users\User\Desktop\Хакатон\Новая папка (2)\Новая папка (2)\Exh3_Vibr_2.csv.csv'
# создаю пустой список, в который перенесу все данные из Logfile
csv_list = []
with open('Exh3_Vibr_2.csv', 'r', newline='', encoding='ANSI') as f:

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
# print('Дата1', csv_list[0][0])
# print('Дата2', csv_list[1][0])
# print('Разность Дата1 - Дата2',csv_list[0][0]-csv_list[1][0])
#
# time_temp1=int((str(csv_list[0][0]))[11:13])
# time_temp5= int((str(csv_list[14111][0]))[11:13])
#
# if time_temp1 != 0:
#     time_temp2=int((str(csv_list[0][0]))[11:13])-2
#     time_temp3=str(csv_list[0][0])[:11]+str(time_temp2)+str(csv_list[0][0])[13:]
#     time_temp4=datetime.datetime.strptime((time_temp3).strip(), '%Y-%m-%d %H:%M:%S')
#     # print('Дата1 - 2 часа = ', time_temp4)
#
# if time_temp5==0: # проверка, если нуль часов, то из нуля вычитать не корректно, поэтому вычитаем из 24
#     time_temp6 = 24-2
#     time_temp7 = str(csv_list[14111][0])[:11]+str(time_temp6)+str(csv_list[14111][0])[13:]
#
#     time_temp9 = datetime.datetime.strptime((time_temp7).strip(), '%Y-%m-%d %H:%M:%S')

# print("Дата3", (str(csv_list[14111][0])))
#print('Дата3-2часа =', time_temp9)




#********************************************************************************************************************
#********************************************************************************************************************
#********************************************************************************************************************
########### перебираю и записываю в список для отправки в БД строчки реверсного файла до тех пор,
    # пока она не будет строчка из реверсного файла не будет равно строчке в connection_file
# конвертирую строку с датой из connection_file.txt в тип datetime
txt_time_ponit_norm = datetime.datetime.strptime((txt_time_ponit[0:19]).strip(), '%Y-%m-%d %H:%M:%S')  # преобразовываю
#print('txt_time_ponit_norm', txt_time_ponit_norm)

select_spisok = []
for row in csv_list:
    #print('row[0]', row[0])
    if row[0] == txt_time_ponit_norm: # если строчка для отправки = строчки из файла txt, то записываю эту строчку
        #print('break')
        select_spisok.append(row)
        break

    elif row[0] >= txt_time_ponit_norm:

        if row[0] > txt_time_ponit_norm:

            select_spisok.append(row)

#print('select_spisok', select_spisok)












data_to_db = list(reversed(select_spisok)) # список для отправки в БД


# временно присваиваю для проверки, после доступа к Кафка, это и код по считыванию UserLog необходимо закоментировать или удалить

data_kafka=select_spisok





#********************************************************************************************************************

################################ отправляю данные из сформированного списка в БД ####################################
if len(data_kafka) > 1:
    #print('len(data_kafka)', len(data_kafka))
    #print('len(data_kafka[0])', len(data_kafka[0]))


    #data_kafka.pop(0)  # удаляю последнию строчку т.к. она уже есть в БД и её не надо повторно отравлять в БД

    try:
        #################################### отрываю БД и считываю таблицу и определяю последнюю(MAX) дату и строки
        # для сравнения #################################################################################################
        #!!!!!!!! заменить конекшенстринг
        conn = pyodbc.connect(txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки
        cur = conn.cursor()  # создали курсор, через который можно работать с БД
        #cur.fast_executemany = True  # это для ускорения передачи данных, но в этой версии Питона не хочет работать

        ######## запись в БД выбранных значений###################

        sql = "INSERT INTO Exh3_Vibr2 (TagTime, TagValue) VALUES (?, ?)"

        #print(11)
        cur.executemany(sql, data_kafka) ############################################################################ДЛЯ ПЕРЕДАЧИ ДАННЫХ РАСКОММЕНТИРОВАТЬ!!!!!!!!!!!!!!!!!!
        ##########################################################################################
        #print(22)
        conn.commit()  # сохраняем изменения в БД

        # повторно читаем, чтобы проверить, записались ли данные в БД
        conn = pyodbc.connect(txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки
        #print(33)
        cur = conn.cursor()  # создали курсор, через который можно работать с БД
        cur.fast_executemany = True # это для ускорения передачи данных, но в этой версии Питона не хочет работать
        #print(44)

        ####### определяю последнее записанное значение по максимальной дате ################################
        cur.execute("select TagTime, TagValue  from Exh3_Vibr2 where TagTime = (select MAX(TagTime) from Exh3_Vibr2)")  # записываю в курсор максимальное значение времени из БД
        #print(1)
        db_list = []  # создаю пустой список для дальнейшей записи в него строки с максимальным времением.
        for line in cur:
            #print(55)
            db_list.append(line)
        # проверяю наличие строчки в БД, которую отправляли по времени.
        #print(db_list[0][0])
        #print(data_kafka[0][0])



        if (db_list[0][0] == data_kafka[0][0]):
            #print(77)

            # считываю connection_file.txt для создания списка lines, который потом редактирую и делаю перезапись
            # файла connection_file.txt
            with open('conn_file_Exh3_Vibr2.txt', 'r', encoding='utf-8') as fr:
                lines = fr.readlines()

            # запись в connection_file.txt
            #print('lines[7]', lines[7])
            lines[7] = str(data_kafka[0][0]) + str('\n') # добавил \n при записи в список пустая строка под записью обрезается
            #print('lines[7]', lines[7])
            #print('lines[10]', lines[10])
            #lines[10] = str(data_kafka[0][3]) + str('\n')
            #print('lines[10]', lines[10])
            #lines[13] = str(data_kafka[0][4]) + str('\n')'lines[7]',
            # записываю в данные в connection_file.txt
            #print(lines)
            with open('conn_file_Exh3_Vibr2.txt', 'w', encoding='utf-8') as fl:
                #print(88)
                fl.writelines(lines)

        conn.commit()  # сохраняем изменения в БД
        conn.close()  # закрываем подключение к базе
    except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
        pass
    finally:
        pass
















