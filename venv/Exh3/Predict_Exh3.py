# видос конекшен стринг https://yandex.ru/video/preview/11619706852423636432


from PyQt5 import QtCore, QtGui, QtWidgets # для создания окна с кнопками

from PyQt5.QtCore import QThread # для разделения задач на потоки

import pyodbc
import csv
import time
import datetime








start = time.time()

####################### Считываю данные из conn_file_Exh_ для опредения последнего записанного времени


with open('conn_file_Exh3_Predict.txt', 'r', encoding='utf-8') as file:

    txt_lines = file.readlines()

txt_connection_string = txt_lines[1].strip() # connection string

txt_time_ponit = txt_lines[7].strip()  # time_ponit

#print('txt_time_ponit', txt_time_ponit)



# Определение выхода из строя ротора по вибрации т.1
import Exh3_Alarm_Vibr1
if Exh3_Alarm_Vibr1.predictor == datetime.datetime.strptime(('9999-02-01 01:01:01').strip(),
                                                      '%Y-%m-%d %H:%M:%S'):
    print('********Cнижение вибрации за рассматриваемый диапазон по вибрации т.2********')
else:
    print('Время выхода из строя из-за Vibr1', Exh3_Alarm_Vibr1.predictor)


# Определение выхода из строя ротора по вибрации т.2
import Exh3_Alarm_Vibr2
if Exh3_Alarm_Vibr2.predictor == datetime.datetime.strptime(('9999-02-01 01:01:01').strip(),
                                                      '%Y-%m-%d %H:%M:%S'):
    print('********Cнижение вибрации за рассматриваемый диапазон по вибрации т.2********')
else:
    print('Время выхода из строя из-за Vibr2', Exh3_Alarm_Vibr2.predictor)

# Определение выхода из строя ротора по температуре т.1
import Exh3_Alarm_Temp1
if Exh3_Alarm_Temp1.predictor == datetime.datetime.strptime(('9999-02-01 01:01:01').strip(),
                                                      '%Y-%m-%d %H:%M:%S'):
    print('********Cнижение вибрации за рассматриваемый диапазон по температуре т.1********')
else:

    print('Время выхода из строя из-за Temp1', Exh3_Alarm_Temp1.predictor)


# Определение выхода из строя ротора по температуре т.2
import Exh3_Alarm_Temp2

if Exh3_Alarm_Temp2.predictor == datetime.datetime.strptime(('9999-02-01 01:01:01').strip(),
                                                      '%Y-%m-%d %H:%M:%S'):
    print('********Cнижение вибрации за рассматриваемый диапазон по температуре т.2********')
else:

    print('Время выхода из строя из-за Temp1', Exh3_Alarm_Temp2.predictor)



print()



time_min_alarm = min(Exh3_Alarm_Vibr1.predictor, Exh3_Alarm_Vibr2.predictor, Exh3_Alarm_Temp1.predictor, Exh3_Alarm_Temp2.predictor)





print('********Минимальное время, когда ротор эксгаустера выйдет из строя', time_min_alarm)


if time_min_alarm == Exh3_Alarm_Vibr1.predictor:
    time_count_alarm = Exh3_Alarm_Vibr1.str_time_to_alarm
    name_alarm = 'Причина выхода из строя ротора - повышенная вибрация т.1'
    print(name_alarm)
    print('Количество дней и часов до аварии(ремонта)', time_count_alarm)

elif time_min_alarm == Exh3_Alarm_Vibr2.predictor:
    time_count_alarm = Exh3_Alarm_Vibr2.str_time_to_alarm
    name_alarm = 'Причина выхода из строя ротора - повышенная вибрация т.2'
    print(name_alarm)
    print('Количество дней и часов до аварии(ремонта)', time_count_alarm)

elif time_min_alarm == Exh3_Alarm_Temp1.predictor:
    time_count_alarm = Exh3_Alarm_Temp1.str_time_to_alarm
    name_alarm = 'Причина выхода из строя ротора - повышенная температура т.1'
    print(name_alarm)
    print('Количество дней и часов до аварии(ремонта)', time_count_alarm)

elif time_min_alarm == Exh3_Alarm_Temp2.predictor:
    time_count_alarm = Exh3_Alarm_Temp2.str_time_to_alarm
    name_alarm = 'Причина выхода из строя ротора - повышенная температура т.2'
    print(name_alarm)
    print('Количество дней и часов до аварии(ремонта)', time_count_alarm)


end = time.time()

print()
#import datetime



date_time_now = datetime.datetime.now()
#print(date_time_now)


# конвертирую строку с датой из connection_file.txt в тип datetime
txt_time_ponit_norm = datetime.datetime.strptime((txt_time_ponit[0:19]).strip(), '%Y-%m-%d %H:%M:%S')
date_time_now_norm = datetime.datetime.strptime(((str(date_time_now))[0:19]).strip(), '%Y-%m-%d %H:%M:%S')
#print('txt_time_ponit_norm', txt_time_ponit_norm)
#print('date_time_now_norm', date_time_now_norm)


if txt_time_ponit_norm < date_time_now_norm: # делаю именно такое сравнение, чтобы не перегружать БД и записывать только изменения

    #print(1111111111111111111)

    # определяю текущую дату и время для записи в БД

    date_time_now = datetime.datetime.now()
    #print(date_time_now)

    ####################################### сформирую список для предачи данных в БД ######################################
    data_alarm = [[date_time_now, Exh3_Alarm_Vibr1.predictor, Exh3_Alarm_Vibr2.predictor, Exh3_Alarm_Temp1.predictor, Exh3_Alarm_Temp2.predictor, time_min_alarm, name_alarm, time_count_alarm]]
    #print(data_alarm)
    ############################################# запишу данные в БД ###################################################


    try:
        #################################### отрываю БД и считываю таблицу и определяю последнюю(MAX) дату и строки

        conn = pyodbc.connect(txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки

        cur = conn.cursor()  # создали курсор, через который можно работать с БД
        cur.fast_executemany = True  # это для ускорения передачи данных



        ####### определяю последнее записанное значение по максимальной дате ################################
        #cur.execute("select TagTime, TagValue  from Exh3_Vibr1 where TagTime = (select MAX(TagTime) from Exh3_Vibr1)")  # записываю в курсор максимальное значение времени из БД
        ######## запись в БД выбранных значений###################

        sql = "INSERT INTO Exh3_Alarm (TagTime, Vibr1_Alarm, Vibr2_Alarm, Temp1_Alarm, Temp2_Alarm, Time_Alarm, Name_Alarm, Count_days_to_Alarm) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

        # print(11)
        cur.executemany(sql, data_alarm)  ############################################################################ДЛЯ ПЕРЕДАЧИ ДАННЫХ РАСКОММЕНТИРОВАТЬ!!!!!!!!!!!!!!!!!!
        ##########################################################################################
        # print(22)
        conn.commit()  # сохраняем изменения в БД

        conn = pyodbc.connect(txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки
        #print(33)
        cur = conn.cursor()  # создали курсор, через который можно работать с БД
        cur.fast_executemany = True # это для ускорения передачи данных, но в этой версии Питона не хочет работать
        #print(44)

        ####### определяю последнее записанное значение по максимальной дате ################################
        cur.execute("select TagTime  from Exh3_Alarm where TagTime = (select MAX(TagTime) from Exh3_Alarm)")  # записываю в курсор максимальное значение времени из БД
        #print(1)
        db_list = []  # создаю пустой список для дальнейшей записи в него строки с максимальным времением.
        for line in cur:
            #print(55)
            db_list.append(line)
        # проверяю наличие строчки в БД, которую отправляли по времени.
        #print('db_list[0][0]', db_list[0][0])
        #print(data_kafka[0][0])



        if (db_list[0][0] != txt_time_ponit_norm):
            #print(77)

            # считываю connection_file.txt для создания списка lines, который потом редактирую и делаю перезапись
            # файла connection_file.txt
            with open('conn_file_Exh3_Predict.txt', 'r', encoding='utf-8') as fr:
                #print(88)
                lines = fr.readlines()

            # запись в connection_file.txt
            #print('lines[7]', lines[7])
            lines[7] = str(db_list[0][0]) + str('\n') # добавил \n при записи в список пустая строка под записью обрезается
            #print('lines[7]', lines[7])
            #print('lines[10]', lines[10])
            #lines[10] = str(data_kafka[0][3]) + str('\n')
            #print('lines[10]', lines[10])
            #lines[13] = str(data_kafka[0][4]) + str('\n')'lines[7]',
            # записываю в данные в connection_file.txt
            #print(lines)
            with open('conn_file_Exh3_Predict.txt', 'w', encoding='utf-8') as fl:
                #print(88)
                fl.writelines(lines)

        conn.commit()  # сохраняем изменения в БД
        conn.close()  # закрываем подключение к ба
    except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
        pass
    finally:
        pass














end = time.time()
print("Время цикла выполнения программы прогноза выхода из строя ротора :", (end - start) * 10 ** 3, "ms")