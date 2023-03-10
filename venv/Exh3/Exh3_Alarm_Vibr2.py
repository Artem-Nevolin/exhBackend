# видос конекшен стринг https://yandex.ru/video/preview/11619706852423636432


from PyQt5 import QtCore, QtGui, QtWidgets # для создания окна с кнопками

from PyQt5.QtCore import QThread # для разделения задач на потоки

import pyodbc
import csv
import time
import datetime

######################## Считываю данные из conn_file_Exh_ для опредения последнего записанного времени


with open('conn_file/conn_file_Exh3_Vibr2.txt', 'r', encoding='utf-8') as file:

    txt_lines = file.readlines()

txt_connection_string = txt_lines[1].strip() # connection string

txt_time_ponit = txt_lines[7].strip()  # time_ponit

##################################################################################################################


############################ считываю данные из БД ###################################################
try:
    #################################### отрываю БД и считываю таблицу и определяю последнюю(MAX) дату и строки

    conn = pyodbc.connect(txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки

    cur = conn.cursor()  # создали курсор, через который можно работать с БД
    cur.fast_executemany = True  # это для ускорения передачи данных

    ####### определяю последнее записанное значение по максимальной дате ################################
    cur.execute("select TagTime, TagValue  from Exh3_Vibr2 where TagTime = (select MAX(TagTime) from Exh3_Vibr2)")  # записываю в курсор максимальное значение времени из БД

    point_2_1 = []  # создаю пустой список для дальнейшей записи в него последнего времени и значения переменной
    for p in cur:

        point_2_1.append(p)
    for k in point_2_1:
        x2_1=k[0]
        m2_1=k[1]

    #print('x2_1', x2_1)
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! для ручного изменеия текущей даты. Потом закоментировать!!!!!!!!!!!!!!!!
    temp777_x2_1='2022-08-06 20:19:52'
    x2_1 = datetime.datetime.strptime((temp777_x2_1).strip(),
                                                  '%Y-%m-%d %H:%M:%S')

#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


#*************************************** нахожу дату и время со сдвигом 1 час и 12 часов *******************************

    delta1_x2_1 = '0001-01-01 01:00:00'  # задаю смещение 1 час. поставил 0001-01-01 т.к. иначе ошибка - не соотвествует формату
    delta1_x2_1_str = str(delta1_x2_1)
    delta2_x2_1 = '0001-01-01 00:00:00'  # для добавления т.к. в смещении вычитаю тоже самое
    delta2_x2_1_str = str(delta2_x2_1)

    delta1_x1_1 = '0001-01-01 12:00:00'  # задаю смещение 12 час. поставил 0001-01-01 т.к. иначе ошибка - не соотвествует формату
    delta1_x1_1_str = str(delta1_x1_1)
    delta2_x1_1 = '0001-01-01 00:00:00'  # для добавления т.к. в смещении вычитаю тоже самое
    delta2_x1_1_str = str(delta2_x1_1)

    delta1_x1_2 = '0001-01-01 01:00:00'  # задаю смещение 12+1 час. от последнего значения поставил 0001-01-01 т.к. иначе ошибка - не соотвествует формату
    delta1_x1_2_str = str(delta1_x1_2)
    delta2_x1_2 = '0001-01-01 00:00:00'  # для добавления т.к. в смещении вычитаю тоже самое
    delta2_x1_2_str = str(delta2_x1_2)

              ################################## x2_2 - минус 1 час     ######################

    delta1_x2_1_norm = datetime.datetime.strptime((delta1_x2_1_str).strip(),
                                                  '%Y-%m-%d %H:%M:%S')  # нормализую для вычислений
    delta2_x2_1_norm = datetime.datetime.strptime((delta2_x2_1_str).strip(),
                                                  '%Y-%m-%d %H:%M:%S')  # нормализую для вычислений

    x2_2 = x2_1 - delta1_x2_1_norm + delta2_x2_1_norm  # вычисляю дату и время для определения среднего значения
                  ############################# x1_1 - минус 12 часов    #############################
    delta1_x2_2_norm = datetime.datetime.strptime((delta1_x1_1_str).strip(),
                                                  '%Y-%m-%d %H:%M:%S')  # нормализую для вычислений
    delta2_x2_2_norm = datetime.datetime.strptime((delta2_x1_1_str).strip(),
                                                  '%Y-%m-%d %H:%M:%S')  # нормализую для вычислений

    x1_1 = x2_1 - delta1_x2_2_norm + delta2_x2_2_norm  # вычисляю дату и время сдвинутую на минус 12 часов

    #print('x1_1', x1_1)
                        ###################### x1_2 - минус 13 часов    #################################
    delta1_x1_2_norm = datetime.datetime.strptime((delta1_x1_2_str).strip(),
                                                  '%Y-%m-%d %H:%M:%S')  # нормализую для вычислений
    delta2_x1_2_norm = datetime.datetime.strptime((delta2_x1_2_str).strip(),
                                                  '%Y-%m-%d %H:%M:%S')  # нормализую для вычислений

    x1_2 = x1_1 - delta1_x1_2_norm + delta2_x1_2_norm  # вычисляю дату и время сдвинутую на минус 12 часов
    #print('x1_2', x1_2)

#******************* преобразую дату и время, иначе не делается запрос из БД по условию********************************
    temp_x2_1 = (str(x2_1))[:10]+'T'+(str(x2_1))[11:]
    #print('x2_1_convert', temp_x2_1)
    temp_x2_2 = (str(x2_2))[:10] + 'T' + (str(x2_2))[11:]
    temp_x1_1 = (str(x1_1))[:10] + 'T' + (str(x1_1))[11:]
    temp_x1_2 = (str(x1_2))[:10] + 'T' + (str(x1_2))[11:]

# ********************************** делаю выгрузку данных за текущий час для нахождения среднего значения **********
    temp_str2 = 'select TagTime, TagValue  from Exh3_Vibr2 where TagTime <= ' + "'" + temp_x2_1 + "'" + ' and ' + 'TagTime >= ' + "'" + temp_x2_2 + "'"
    #print('temp_str2', temp_str2)
    #print('x2_1', x2_1)
    #print('x2_2', x2_2)
    mean_point_2 = []  # создаю пустой список (первый час) для дальнейшей записи в него последнего времени и значения переменной
    cur.execute(temp_str2)  # записываю в курсор данные за последний час
    #print('222')
    for mnp2 in cur:
        mean_point_2.append(mnp2)

    #print('mean_point_2', len(mean_point_2))

#*********************************************************************************************************************

# ********************************** делаю выгрузку данных за 12 час для нахождения среднего значения **********

    temp_str1 = 'select TagTime, TagValue  from Exh3_Vibr2 where TagTime <= ' + "'" + temp_x1_1 + "'" + ' and ' + 'TagTime >= ' + "'" + temp_x1_2 + "'"
    #print('temp_str1', temp_str1)
    #print('x1_1', x1_1)
    #print('x1_2', x1_2)

    mean_point_1 = []  # создаю пустой список (12 час) для дальнейшей записи в него последнего времени и значения переменной
    cur.execute(temp_str1)  # записываю в курсор данные за последний час
    #print('333')
    # print(type(cur))
    for mnp1 in cur:
        mean_point_1.append(mnp1)

    #print('mean_point_1', len(mean_point_1))
    conn.commit()  # сохраняем изменения в БД
    conn.close()
####################################################################################################################


################################ нахожу среднее значение в первый и последний(12) час ##############################
    sum2 = 0
    count_2 = 0
    for kl in mean_point_2:
        value2 = kl[1]
        sum2=sum2+value2
        count_2 +=1
    mean2=round(sum2/count_2,2) # округляю
    #print(sum2)
    #print(count_2)
    #print('среднее значение за 1 час(M2)', mean2)

    sum1 = 0
    count_1 = 0
    for kb in mean_point_1:
        value1 = kb[1]
        sum1 = sum1 + value1
        count_1 += 1
    mean1 = round(sum1 / count_1, 2)  # округляю
    #print(sum1)
    #print(count_1)
    #print('среднее значение за 12 час(M1)', mean1)

#*******************************************************************************************
    mean = 7 # уставка, при достижении, которой необходимо узнать дату и время *************
#*******************************************************************************************

    if mean1 > mean2:
        # print('снижение вибрации за рассматриваемый диапазон по температуре т.2')
        predictor = datetime.datetime.strptime(('9999-02-01 01:01:01').strip(),
                                               '%Y-%m-%d %H:%M:%S')
        # print('predictor', predictor)


    else:

        predictor = ((mean-mean1)*(x2_1-x1_1))/(mean2-mean1) + x1_1
        #print('predictor', predictor)

        #print('дата и время выхода из строя ротора', (str(predictor))[:19])

        time_to_alarm= predictor - x2_1
        str_time_to_alarm=(str(time_to_alarm))[:16]

        #print('количество дней до выхода из строя ротора', str_time_to_alarm)

    conn.commit()  # сохраняем изменения в БД
    conn.close()  # закрываем подключение к базе
except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
    pass
finally:
    pass
