# видос на тему многопоточности https://www.youtube.com/watch?v=vcPTJX-nM94&list=PLdsRQ10VxDSuaO6C_wcWHWLME0yXuNLjn&index=7

from PyQt5 import QtCore, QtGui, QtWidgets # для создания окна с кнопками

from PyQt5.QtCore import QThread # для разделения задач на потоки

import pyodbc
import csv
import time
import datetime
import json
import dateutil.parser

from confluent_kafka import Consumer



start = time.time()
############################################## класс для создания первого потока #############################
#####################################################################################################################
class ProgressBarThead1(QThread): # класс для создания многопоточности

    def  __init__(self, mainwindow, parent=None): # mainwindow - ссылка на основное окно. В конструкторе получаем ссылку
        # на основное окно и переопределяем метод def run

        super().__init__() # делаем метод супер и запускаю его конструктор
        # присваиваем параметру Self.mainwindow ссыклу на окно
        self.mainwindow = mainwindow # mainwindow - это экземпляр класса, который создает окно т.е. в нашем случае это Ui_MainWindow(object)
        # таким образом при инициализации класса плитки нашего потока можем обращаться к  ProgressBar, либо к любому элементу этого окна

        # напишем, что будет происходить в этом рабочем потоке
    def run(self): # в этом потоке будет проиcходить циклическое выполнение рабочей программы
        try:
            while True:
                start = time.time()

                #exh3_Temp7()
                #print(' **************************************************вызов функции Temp7 завершен*****************************************')
                hendlerDB1()  # вызываю функцию, которая выполняет обработку базы данных, csv файла(UserLog), txt файла(ConnectionDB)

                end = time.time()
                print("Время цикла выполнения программы первого потока:", (end - start) * 10 ** 3, "ms")
                time.sleep(65)
        except Exception:  # отлавливаю потерю связи с БЮ и исключения и игнорирую их
            pass

        finally:
            pass

############################################## класс для создания второго потока #############################
#####################################################################################################################
class ProgressBarThead2(QThread): # класс для создания многопоточности

    def  __init__(self, mainwindow, parent=None): # mainwindow - ссылка на основное окно. В конструкторе получаем ссылку
        # на основное окно и переопределяем метод def run

        super().__init__() # делаем метод супер и запускаю его конструктор
        # присваиваем параметру Self.mainwindow ссыклу на окно
        self.mainwindow = mainwindow # mainwindow - это экземпляр класса, который создает окно т.е. в нашем случае это Ui_MainWindow(object)
        # таким образом при инициализации класса плитки нашего потока можем обращаться к  ProgressBar, либо к любому элементу этого окна

        # напишем, что будет происходить в этом рабочем потоке
    def run(self): # в этом потоке будет проиcходить циклическое выполнение рабочей программы
        try:
            while True:
                start = time.time()
                #print(77777777)
                hendlerDB2()# вызываю функцию, которая выполняет обработку базы данных, csv файла(UserLog), txt файла(ConnectionDB)

                end = time.time()
                print("Время цикла выполнения программы второго потока:", (end - start) * 10 ** 3, "ms")
                time.sleep(65) ################################################################################## время лучше увеличить
        except Exception:  # отлавливаю потерю связи с БЮ и исключения и игнорирую их
            pass

        finally:
            pass


############################################## класс для создания третьего потока #############################
#####################################################################################################################
class ProgressBarThead3(QThread): # класс для создания многопоточности

    def  __init__(self, mainwindow, parent=None): # mainwindow - ссылка на основное окно. В конструкторе получаем ссылку
        # на основное окно и переопределяем метод def run

        super().__init__() # делаем метод супер и запускаю его конструктор
        # присваиваем параметру Self.mainwindow ссыклу на окно
        self.mainwindow = mainwindow # mainwindow - это экземпляр класса, который создает окно т.е. в нашем случае это Ui_MainWindow(object)
        # таким образом при инициализации класса плитки нашего потока можем обращаться к  ProgressBar, либо к любому элементу этого окна

        # напишем, что будет происходить в этом рабочем потоке
    def run(self): # в этом потоке будет проиcходить циклическое выполнение рабочей программы
        try:
            while True:
                start = time.time()
                #print(77777777)
                hendlerDB3()# вызываю функцию, которая выполняет обработку базы данных, csv файла(UserLog), txt файла(ConnectionDB)

                end = time.time()
                print("Время цикла выполнения программы третьего потока:", (end - start) * 10 ** 3, "ms")
                time.sleep(65) ################################################################################## время лучше увеличить
        except Exception:  # отлавливаю потерю связи с БЮ и исключения и игнорирую их
            pass

        finally:
            pass

############################################## класс для создания четвертого потока #############################
#####################################################################################################################
class ProgressBarThead4(QThread): # класс для создания многопоточности

    def  __init__(self, mainwindow, parent=None): # mainwindow - ссылка на основное окно. В конструкторе получаем ссылку
        # на основное окно и переопределяем метод def run

        super().__init__() # делаем метод супер и запускаю его конструктор
        # присваиваем параметру Self.mainwindow ссыклу на окно
        self.mainwindow = mainwindow # mainwindow - это экземпляр класса, который создает окно т.е. в нашем случае это Ui_MainWindow(object)
        # таким образом при инициализации класса плитки нашего потока можем обращаться к  ProgressBar, либо к любому элементу этого окна

        # напишем, что будет происходить в этом рабочем потоке
    def run(self): # в этом потоке будет проиcходить циклическое выполнение рабочей программы
        try:
            while True:
                start = time.time()
                #print(77777777)
                data_csv_Exh3_Temp7()
                data_csv_Exh3_Temp8()
                #hendlerDB4()# вызываю функцию, которая выполняет обработку базы данных, csv файла(UserLog), txt файла(ConnectionDB)
                #print('Привет, я четвертый поток', list_Exh3_Temp7)
                end = time.time()
                print("Время цикла выполнения программы четверного потока:", (end - start) * 10 ** 3, "ms")
                time.sleep(0.001) ################################################################################## время лучше увеличить
        except Exception:  # отлавливаю потерю связи с БЮ и исключения и игнорирую их
            pass

        finally:
            pass


############################################## класс для создания пятого потока #############################
#####################################################################################################################
class ProgressBarThead5(QThread): # класс для создания многопоточности

    def  __init__(self, mainwindow, parent=None): # mainwindow - ссылка на основное окно. В конструкторе получаем ссылку
        # на основное окно и переопределяем метод def run

        super().__init__() # делаем метод супер и запускаю его конструктор
        # присваиваем параметру Self.mainwindow ссыклу на окно
        self.mainwindow = mainwindow # mainwindow - это экземпляр класса, который создает окно т.е. в нашем случае это Ui_MainWindow(object)
        # таким образом при инициализации класса плитки нашего потока можем обращаться к  ProgressBar, либо к любому элементу этого окна

        # напишем, что будет происходить в этом рабочем потоке
    def run(self): # в этом потоке будет проиcходить циклическое выполнение рабочей программы
        try:
            while True:
                start = time.time()
                #print(77777777)
                hendlerDB5()# вызываю функцию, которая выполняет обработку базы данных, csv файла(UserLog), txt файла(ConnectionDB)

                end = time.time()
                #print("Время цикла выполнения программы пятого потока:", (end - start) * 10 ** 3, "ms")
                time.sleep(5) ################################################################################## время лучше увеличить
        except Exception:  # отлавливаю потерю связи с БЮ и исключения и игнорирую их
            pass

        finally:
            pass


############################################## класс для создания шестого потока #############################
#####################################################################################################################
class ProgressBarThead6(QThread): # класс для создания многопоточности

    def  __init__(self, mainwindow, parent=None): # mainwindow - ссылка на основное окно. В конструкторе получаем ссылку
        # на основное окно и переопределяем метод def run

        super().__init__() # делаем метод супер и запускаю его конструктор
        # присваиваем параметру Self.mainwindow ссыклу на окно
        self.mainwindow = mainwindow # mainwindow - это экземпляр класса, который создает окно т.е. в нашем случае это Ui_MainWindow(object)
        # таким образом при инициализации класса плитки нашего потока можем обращаться к  ProgressBar, либо к любому элементу этого окна

        # напишем, что будет происходить в этом рабочем потоке
    def run(self): # в этом потоке будет проиcходить циклическое выполнение рабочей программы
        try:
            while True:
                start = time.time()
                #print(77777777)
                hendlerDB6()# вызываю функцию, которая выполняет обработку базы данных, csv файла(UserLog), txt файла(ConnectionDB)

                end = time.time()
                #print("Время цикла выполнения программы шестого потока:", (end - start) * 10 ** 3, "ms")
                time.sleep(5) ################################################################################## время лучше увеличить
        except Exception:  # отлавливаю потерю связи с БЮ и исключения и игнорирую их
            pass

        finally:
            pass


############################################## класс для создания седьмого потока #############################
#####################################################################################################################
class ProgressBarThead7(QThread): # класс для создания многопоточности

    def  __init__(self, mainwindow, parent=None): # mainwindow - ссылка на основное окно. В конструкторе получаем ссылку
        # на основное окно и переопределяем метод def run

        super().__init__() # делаем метод супер и запускаю его конструктор
        # присваиваем параметру Self.mainwindow ссыклу на окно
        self.mainwindow = mainwindow # mainwindow - это экземпляр класса, который создает окно т.е. в нашем случае это Ui_MainWindow(object)
        # таким образом при инициализации класса плитки нашего потока можем обращаться к  ProgressBar, либо к любому элементу этого окна

        # напишем, что будет происходить в этом рабочем потоке
    def run(self): # в этом потоке будет проиcходить циклическое выполнение рабочей программы
        try:
            while True:
                start = time.time()
                #print(77777777)
                hendlerDB7()# вызываю функцию, которая выполняет обработку базы данных, csv файла(UserLog), txt файла(ConnectionDB)

                end = time.time()
                #print("Время цикла выполнения программы седьмого потока:", (end - start) * 10 ** 3, "ms")
                time.sleep(5) ################################################################################## время лучше увеличить
        except Exception:  # отлавливаю потерю связи с БЮ и исключения и игнорирую их
            pass

        finally:
            pass

############################################## класс для создания восьмого потока #############################
#####################################################################################################################
class ProgressBarThead8(QThread): # класс для создания многопоточности

    def  __init__(self, mainwindow, parent=None): # mainwindow - ссылка на основное окно. В конструкторе получаем ссылку
        # на основное окно и переопределяем метод def run

        super().__init__() # делаем метод супер и запускаю его конструктор
        # присваиваем параметру Self.mainwindow ссыклу на окно
        self.mainwindow = mainwindow # mainwindow - это экземпляр класса, который создает окно т.е. в нашем случае это Ui_MainWindow(object)
        # таким образом при инициализации класса плитки нашего потока можем обращаться к  ProgressBar, либо к любому элементу этого окна

        # напишем, что будет происходить в этом рабочем потоке
    def run(self): # в этом потоке будет проиcходить циклическое выполнение рабочей программы
        try:
            while True:
                start = time.time()
                #print(77777777)
                hendlerDB8()# вызываю функцию, которая выполняет обработку базы данных, csv файла(UserLog), txt файла(ConnectionDB)

                end = time.time()
                #print("Время цикла выполнения программы восьмого потока:", (end - start) * 10 ** 3, "ms")
                time.sleep(5) ################################################################################## время лучше увеличить
        except Exception:  # отлавливаю потерю связи с БЮ и исключения и игнорирую их
            pass

        finally:
            pass


#***********************  класс для создания окна с кнопками и запуска обработчиков потоков  *****************************
# класс Ui_MainWindow(object) с кнопками сделан автоматически из QTdisainer******************************************
class Ui_MainWindow(object): # класс для создания окна с кнопками
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(491, 119)
        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.Btn_Start = QtWidgets.QPushButton(self.centralwidget)
        self.Btn_Start.setGeometry(QtCore.QRect(120, 70, 93, 28))
        self.Btn_Start.setAutoDefault(False)
        self.Btn_Start.setDefault(False)
        self.Btn_Start.setFlat(False)
        self.Btn_Start.setObjectName("Btn_Start")
        self.Btn_Stop = QtWidgets.QPushButton(self.centralwidget)
        self.Btn_Stop.setGeometry(QtCore.QRect(290, 70, 93, 28))
        self.Btn_Stop.setObjectName("Btn_Stop")
        self.label = QtWidgets.QLabel(self.centralwidget)
        self.label.setGeometry(QtCore.QRect(10, 20, 491, 20))
        self.label.setObjectName("label")
        MainWindow.setCentralWidget(self.centralwidget)

        self.retranslateUi(MainWindow)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

        ################################################################################################################
        #  добавляю обращение к методу. За счет этого метода будем обращаться к обработчикам событий ко всем кнопками
        self.add_functions()
        ###############################################################################################################


    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "Передача данных в БД ExhHub"))
        self.Btn_Start.setText(_translate("MainWindow", "Ok"))
        self.Btn_Stop.setText(_translate("MainWindow", "Cancel"))
        self.label.setText(_translate("MainWindow", "Включить передачу данных в БД и прогнозирования поломки роторов?"))
#*********************************************************************************************************************
# ********************************************************************************************************************

    # дополнительно прописываю метод для обработки событий при нажатии на кнопки
    def add_functions(self):
        # обращаюсь к нопкe "Ok" при "клике" и в скобках указываю какой метод будет срабатывать
        self.Btn_Start.clicked.connect(self.Launch_progress_bar_filling1) # срабатывает метод Launch_progress_bar_filling
        self.Btn_Start.clicked.connect(self.Launch_progress_bar_filling2) # cрабатывает метод для второго потока
        self.Btn_Start.clicked.connect(self.Launch_progress_bar_filling3)  # cрабатывает метод для третьего потока
        self.Btn_Start.clicked.connect(self.Launch_progress_bar_filling4)  # cрабатывает метод для четвертого потока
        self.Btn_Start.clicked.connect(self.Launch_progress_bar_filling5)  # cрабатывает метод для пятого потока
        self.Btn_Start.clicked.connect(self.Launch_progress_bar_filling6)  # cрабатывает метод для шестого потока
        self.Btn_Start.clicked.connect(self.Launch_progress_bar_filling7)  # cрабатывает метод для седьмого потока
        self.Btn_Start.clicked.connect(self.Launch_progress_bar_filling8)  # cрабатывает метод для восьмого потока

        self.Btn_Stop.clicked.connect(self.write_btn_stop)


        # инициализация класса
        # создаем экземпляр класса ProgressBarThead отдельного потока(нитки). Этому потоку(нитке) будет передаваться на
        # вход наше основное приложение-ссылка (mainwindow=self), чтобы она имела доступ ко всем элементам этого окна и
        # всем его параметрам
        self.ProgresbarTread_instance1 = ProgressBarThead1(mainwindow=self) # первый поток
        self.ProgresbarTread_instance2 = ProgressBarThead2(mainwindow=self)  # второй поток
        self.ProgresbarTread_instance3 = ProgressBarThead3(mainwindow=self)  # третий поток
        self.ProgresbarTread_instance4 = ProgressBarThead4(mainwindow=self)  # четвертый поток
        self.ProgresbarTread_instance5 = ProgressBarThead5(mainwindow=self)  # пятый поток
        self.ProgresbarTread_instance6 = ProgressBarThead6(mainwindow=self)  # шестой поток
        self.ProgresbarTread_instance7 = ProgressBarThead7(mainwindow=self)  # седьмой поток
        self.ProgresbarTread_instance8 = ProgressBarThead8(mainwindow=self)  # восьмой поток


    # создаем функцию, которая будет запускать этот отдельный поток поток. Далее, когда получаем доступ
    def Launch_progress_bar_filling1(self):
        self.ProgresbarTread_instance1.start() # запускаю поток в постоянную работу с помощью метода start()

    # функция для второго потока
    def Launch_progress_bar_filling2(self):
        self.ProgresbarTread_instance2.start()

    # функция для третьего потока
    def Launch_progress_bar_filling3(self):
        self.ProgresbarTread_instance3.start()

    # функция для четвертого потока
    def Launch_progress_bar_filling4(self):
        self.ProgresbarTread_instance4.start()


    # функция для пятого потока
    def Launch_progress_bar_filling5(self):
        self.ProgresbarTread_instance5.start()


    # функция для шестого потока
    def Launch_progress_bar_filling6(self):
        self.ProgresbarTread_instance6.start()


    # функция для седьмого потока
    def Launch_progress_bar_filling7(self):
        self.ProgresbarTread_instance7.start()


    # функция для восьмого потока
    def Launch_progress_bar_filling8(self):
        self.ProgresbarTread_instance8.start()


    def write_btn_stop(self, r = True): # метод, который срабатывае при нажатии кнопки СТОП
         raise SystemExit # закрываю окно
#********************************************************************************************************************

#******** обработчик первого потока ** запись в БД ************************
def hendlerDB1(): # функция, которая выполняет обработку базы данных, csv файла(UserLog), txt файла(ConnectionDB)
    # считывание и запись в БД параметров эксгаустера №3
    param_Exh3()
    print('********************Чтение и запись данных в БД закончена********************')

# вызываю функции считывания и передачи данных в ДБ
def param_Exh3():
    exh3_Vibr1()
    #print(11111111111111111)
    exh3_Vibr2()
    #print(22222222222222222)
    exh3_Temp1()
    #print(3333333333333333)
    exh3_Temp2()
    #print(44444444444444444)
    exh3_Temp3()
    #print(5555555555555555555)
    # exh3_Temp7()
    # print(' **************************************************вызов функции Temp7 завершен*****************************************')

# читаю и передаю данные вибрации т.1
def exh3_Vibr1():



    ######################## Считываю данные из conn_file_Exh_ для опредения последнего записанного времени

    with open('Exh3/conn_file/conn_file_Exh3_Vibr1.txt', 'r', encoding='utf-8') as file:

        txt_lines = file.readlines()

    txt_connection_string = txt_lines[1].strip()  # connection string
    # txt_logfile = txt_lines[4].strip()  # logfile
    txt_time_ponit = txt_lines[7].strip()  # time_ponit
    # txt_command = txt_lines[10].strip()  # command
    # txt_status_command = txt_lines[13].strip()  # status_command

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

    # **********************************************************************************************************************
    # **********************************************После подключения Кафка раскоментировать***********************************
    # **********************************************************************************************************************
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

    # **********************************************************************************************************************
    # **********************************************************************************************************************
    # **********************************************************************************************************************
    #####################################################################################################################

    # ********************************************************************************************************************
    # *******************************временно читаю из файла, как будто из кафки, чтобы проверить алгоритм*************
    # ********************************************************************************************************************
    ###########################  читаю csv файл ##############################################################

    # указываю путь, где находится логфайл для чтения
    # path = r'C:\Users\User\Desktop\Хакатон\Новая папка (2)\Новая папка (2)\Exh3_Vibr_2.csv.csv'
    # создаю пустой список, в который перенесу все данные из Logfile
    csv_list = []
    with open('Exh3/csv_file/Exh3_Vibr_1.csv', 'r', newline='', encoding='ANSI') as f:

        reader = csv.reader(f, quoting=csv.QUOTE_NONE)  # delimiter=' ' - ставит запятую вместо пробела
        for nr in reader:  #
            if len(nr) == 1:
                stroka1 = nr[0]
                # print(1)
            if len(nr) == 2:
                stroka2_temp = nr[0]
                stroka2_2 = stroka2_temp[0:19]
                stroka2 = datetime.datetime.strptime((stroka2_2).strip(), '%Y-%m-%d %H:%M:%S')
                stroka3 = stroka2_temp[24:25]
                stroka4_temp = nr[1]
                stroka4 = stroka4_temp[:2]
                stroka5 = [stroka2, stroka3 + '.' + stroka4]
                csv_list.append(stroka5)
                # print(stroka2,stroka3, stroka4)
                # print(stroka5)
    # print(len(csv_list))
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
    # print('Дата3-2часа =', time_temp9)

    # ********************************************************************************************************************
    # ********************************************************************************************************************
    # ********************************************************************************************************************
    ########### перебираю и записываю в список для отправки в БД строчки реверсного файла до тех пор,
    # пока она не будет строчка из реверсного файла не будет равно строчке в connection_file
    # конвертирую строку с датой из connection_file.txt в тип datetime
    txt_time_ponit_norm = datetime.datetime.strptime((txt_time_ponit[0:19]).strip(),
                                                     '%Y-%m-%d %H:%M:%S')  # преобразовываю
    # print('txt_time_ponit_norm', txt_time_ponit_norm)

    select_spisok = []
    for row in csv_list:
        # print('row[0]', row[0])
        if row[
            0] == txt_time_ponit_norm:  # если строчка для отправки = строчки из файла txt, то записываю эту строчку
            # print('break')
            select_spisok.append(row)
            break

        elif row[0] >= txt_time_ponit_norm:

            if row[0] > txt_time_ponit_norm:
                select_spisok.append(row)

    #print('select_spisok', select_spisok)

    data_to_db = list(reversed(select_spisok))  # список для отправки в БД

    # временно присваиваю для проверки, после доступа к Кафка, это и код по считыванию UserLog необходимо закоментировать или удалить

    data_kafka = select_spisok

    # ********************************************************************************************************************

    ################################ отправляю данные из сформированного списка в БД ####################################
    if len(data_kafka) > 1:
        # print('len(data_kafka)', len(data_kafka))
        #print('len(data_kafka[0])', len(data_kafka[0]))

        # data_kafka.pop(0)  # удаляю последнию строчку т.к. она уже есть в БД и её не надо повторно отравлять в БД

        try:
            #################################### отрываю БД и считываю таблицу и определяю последнюю(MAX) дату и строки
            # для сравнения #################################################################################################
            # !!!!!!!! заменить конекшенстринг
            conn = pyodbc.connect(
                txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки
            cur = conn.cursor()  # создали курсор, через который можно работать с БД
            # cur.fast_executemany = True  # это для ускорения передачи данных, но в этой версии Питона не хочет работать

            ######## запись в БД выбранных значений###################

            sql = "INSERT INTO Exh3_Vibr1 (TagTime, TagValue) VALUES (?, ?)"

            # print(11)
            cur.executemany(sql,
                            data_kafka)  ############################################################################ДЛЯ ПЕРЕДАЧИ ДАННЫХ РАСКОММЕНТИРОВАТЬ!!!!!!!!!!!!!!!!!!
            ##########################################################################################
            # print(22)
            conn.commit()  # сохраняем изменения в БД

            # повторно читаем, чтобы проверить, записались ли данные в БД
            conn = pyodbc.connect(
                txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки
            # print(33)
            cur = conn.cursor()  # создали курсор, через который можно работать с БД
            cur.fast_executemany = True  # это для ускорения передачи данных, но в этой версии Питона не хочет работать
            # print(44)

            ####### определяю последнее записанное значение по максимальной дате ################################
            cur.execute(
                "select TagTime, TagValue  from Exh3_Vibr1 where TagTime = (select MAX(TagTime) from Exh3_Vibr1)")  # записываю в курсор максимальное значение времени из БД
            # print(1)
            db_list = []  # создаю пустой список для дальнейшей записи в него строки с максимальным времением.
            for line in cur:
                # print(55)
                db_list.append(line)
            # проверяю наличие строчки в БД, которую отправляли по времени.
            # print(db_list[0][0])
            # print(data_kafka[0][0])

            if (db_list[0][0] == data_kafka[0][0]):
                # print(77)

                # считываю connection_file.txt для создания списка lines, который потом редактирую и делаю перезапись
                # файла connection_file.txt
                with open('Exh3/conn_file/conn_file_Exh3_Vibr1.txt', 'r', encoding='utf-8') as fr:
                    lines = fr.readlines()

                # запись в connection_file.txt
                # print('lines[7]', lines[7])
                lines[7] = str(data_kafka[0][0]) + str(
                    '\n')  # добавил \n при записи в список пустая строка под записью обрезается
                # print('lines[7]', lines[7])
                # print('lines[10]', lines[10])
                # lines[10] = str(data_kafka[0][3]) + str('\n')
                # print('lines[10]', lines[10])
                # lines[13] = str(data_kafka[0][4]) + str('\n')'lines[7]',
                # записываю в данные в connection_file.txt
                # print(lines)
                with open('Exh3/conn_file/conn_file_Exh3_Vibr1.txt', 'w', encoding='utf-8') as fl:
                    # print(88)
                    fl.writelines(lines)

            conn.commit()  # сохраняем изменения в БД
            #print('Vibr1')
            conn.close()  # закрываем подключение к базе
        except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
            pass
        finally:
            pass

# читаю и передаю данные вибрации т.2
def exh3_Vibr2():
    ######################## Считываю данные из conn_file_Exh_ для опредения последнего записанного времени

    with open('Exh3/conn_file/conn_file_Exh3_Vibr2.txt', 'r', encoding='utf-8') as file:

        txt_lines = file.readlines()

    txt_connection_string = txt_lines[1].strip()  # connection string
    # txt_logfile = txt_lines[4].strip()  # logfile
    txt_time_ponit = txt_lines[7].strip()  # time_ponit
    # txt_command = txt_lines[10].strip()  # command
    # txt_status_command = txt_lines[13].strip()  # status_command

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

    # **********************************************************************************************************************
    # **********************************************После подключения Кафка раскоментировать***********************************
    # **********************************************************************************************************************
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

    # **********************************************************************************************************************
    # **********************************************************************************************************************
    # **********************************************************************************************************************
    #####################################################################################################################

    # ********************************************************************************************************************
    # *******************************временно читаю из файла, как будто из кафки, чтобы проверить алгоритм*************
    # ********************************************************************************************************************
    ###########################  читаю csv файл ##############################################################

    # указываю путь, где находится логфайл для чтения
    # path = r'C:\Users\User\Desktop\Хакатон\Новая папка (2)\Новая папка (2)\Exh3_Vibr_2.csv.csv'
    # создаю пустой список, в который перенесу все данные из Logfile
    csv_list = []
    with open('Exh3/csv_file/Exh3_Vibr_2.csv', 'r', newline='', encoding='ANSI') as f:

        reader = csv.reader(f, quoting=csv.QUOTE_NONE)  # delimiter=' ' - ставит запятую вместо пробела
        for nr in reader:  #
            if len(nr) == 1:
                stroka1 = nr[0]
                # print(1)
            if len(nr) == 2:
                stroka2_temp = nr[0]
                stroka2_2 = stroka2_temp[0:19]
                stroka2 = datetime.datetime.strptime((stroka2_2).strip(), '%Y-%m-%d %H:%M:%S')
                stroka3 = stroka2_temp[24:25]
                stroka4_temp = nr[1]
                stroka4 = stroka4_temp[:2]
                stroka5 = [stroka2, stroka3 + '.' + stroka4]
                csv_list.append(stroka5)
                # print(stroka2,stroka3, stroka4)
                # print(stroka5)
    # print(len(csv_list))
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
    # print('Дата3-2часа =', time_temp9)

    # ********************************************************************************************************************
    # ********************************************************************************************************************
    # ********************************************************************************************************************
    ########### перебираю и записываю в список для отправки в БД строчки реверсного файла до тех пор,
    # пока она не будет строчка из реверсного файла не будет равно строчке в connection_file
    # конвертирую строку с датой из connection_file.txt в тип datetime
    txt_time_ponit_norm = datetime.datetime.strptime((txt_time_ponit[0:19]).strip(),
                                                     '%Y-%m-%d %H:%M:%S')  # преобразовываю
    # print('txt_time_ponit_norm', txt_time_ponit_norm)

    select_spisok = []
    for row in csv_list:
        # print('row[0]', row[0])
        if row[0] == txt_time_ponit_norm:  # если строчка для отправки = строчки из файла txt, то записываю эту строчку
            # print('break')
            select_spisok.append(row)
            break

        elif row[0] >= txt_time_ponit_norm:

            if row[0] > txt_time_ponit_norm:
                select_spisok.append(row)

    # print('select_spisok', select_spisok)

    data_to_db = list(reversed(select_spisok))  # список для отправки в БД

    # временно присваиваю для проверки, после доступа к Кафка, это и код по считыванию UserLog необходимо закоментировать или удалить

    data_kafka = select_spisok

    # ********************************************************************************************************************

    ################################ отправляю данные из сформированного списка в БД ####################################
    if len(data_kafka) > 1:
        # print('len(data_kafka)', len(data_kafka))
        # print('len(data_kafka[0])', len(data_kafka[0]))

        # data_kafka.pop(0)  # удаляю последнию строчку т.к. она уже есть в БД и её не надо повторно отравлять в БД

        try:
            #################################### отрываю БД и считываю таблицу и определяю последнюю(MAX) дату и строки
            # для сравнения #################################################################################################
            # !!!!!!!! заменить конекшенстринг
            conn = pyodbc.connect(
                txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки
            cur = conn.cursor()  # создали курсор, через который можно работать с БД
            # cur.fast_executemany = True  # это для ускорения передачи данных, но в этой версии Питона не хочет работать

            ######## запись в БД выбранных значений###################

            sql = "INSERT INTO Exh3_Vibr2 (TagTime, TagValue) VALUES (?, ?)"

            # print(11)
            cur.executemany(sql,
                            data_kafka)  ############################################################################ДЛЯ ПЕРЕДАЧИ ДАННЫХ РАСКОММЕНТИРОВАТЬ!!!!!!!!!!!!!!!!!!
            ##########################################################################################
            # print(22)
            conn.commit()  # сохраняем изменения в БД

            # повторно читаем, чтобы проверить, записались ли данные в БД
            conn = pyodbc.connect(
                txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки
            # print(33)
            cur = conn.cursor()  # создали курсор, через который можно работать с БД
            cur.fast_executemany = True  # это для ускорения передачи данных, но в этой версии Питона не хочет работать
            # print(44)

            ####### определяю последнее записанное значение по максимальной дате ################################
            cur.execute(
                "select TagTime, TagValue  from Exh3_Vibr2 where TagTime = (select MAX(TagTime) from Exh3_Vibr2)")  # записываю в курсор максимальное значение времени из БД
            # print(1)
            db_list = []  # создаю пустой список для дальнейшей записи в него строки с максимальным времением.
            for line in cur:
                # print(55)
                db_list.append(line)
            # проверяю наличие строчки в БД, которую отправляли по времени.
            # print(db_list[0][0])
            # print(data_kafka[0][0])

            if (db_list[0][0] == data_kafka[0][0]):
                # print(77)

                # считываю connection_file.txt для создания списка lines, который потом редактирую и делаю перезапись
                # файла connection_file.txt
                with open('Exh3/conn_file/conn_file_Exh3_Vibr2.txt', 'r', encoding='utf-8') as fr:
                    lines = fr.readlines()

                # запись в connection_file.txt
                # print('lines[7]', lines[7])
                lines[7] = str(data_kafka[0][0]) + str(
                    '\n')  # добавил \n при записи в список пустая строка под записью обрезается
                # print('lines[7]', lines[7])
                # print('lines[10]', lines[10])
                # lines[10] = str(data_kafka[0][3]) + str('\n')
                # print('lines[10]', lines[10])
                # lines[13] = str(data_kafka[0][4]) + str('\n')'lines[7]',
                # записываю в данные в connection_file.txt
                # print(lines)
                with open('Exh3/conn_file/conn_file_Exh3_Vibr2.txt', 'w', encoding='utf-8') as fl:
                    # print(88)
                    fl.writelines(lines)

            conn.commit()  # сохраняем изменения в БД
            conn.close()  # закрываем подключение к базе
        except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
            pass
        finally:
            pass

# читаю и передаю данные температуры т.1
def exh3_Temp1():
    with open('Exh3/conn_file/conn_file_Exh3_Temp1.txt', 'r', encoding='utf-8') as file:

        txt_lines = file.readlines()

    txt_connection_string = txt_lines[1].strip()  # connection string
    # txt_logfile = txt_lines[4].strip()  # logfile
    txt_time_ponit = txt_lines[7].strip()  # time_ponit
    # txt_command = txt_lines[10].strip()  # command
    # txt_status_command = txt_lines[13].strip()  # status_command

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

    # **********************************************************************************************************************
    # **********************************************После подключения Кафка раскоментировать***********************************
    # **********************************************************************************************************************
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

    # **********************************************************************************************************************
    # **********************************************************************************************************************
    # **********************************************************************************************************************
    #####################################################################################################################

    # ********************************************************************************************************************
    # *******************************временно читаю из файла, как будто из кафки, чтобы проверить алгоритм*************
    # ********************************************************************************************************************
    ###########################  читаю csv файл ##############################################################

    # указываю путь, где находится логфайл для чтения
    # path = r'C:\Users\User\Desktop\Хакатон\Новая папка (2)\Новая папка (2)\Exh3_Vibr_2.csv.csv'
    # создаю пустой список, в который перенесу все данные из Logfile
    csv_list = []
    with open('Exh3/csv_file/Exh3_Temp1.csv', 'r', newline='', encoding='ANSI') as f:

        reader = csv.reader(f, quoting=csv.QUOTE_NONE)  # delimiter=' ' - ставит запятую вместо пробела
        for nr in reader:  #
            if len(nr) == 1:
                stroka1 = nr[0]
                # print(1)
            if len(nr) == 2:
                stroka2_temp = nr[0]
                stroka2_2 = stroka2_temp[0:19]
                stroka2 = datetime.datetime.strptime((stroka2_2).strip(), '%Y-%m-%d %H:%M:%S')
                stroka3 = stroka2_temp[24:26]
                stroka4_temp = nr[1]
                stroka4 = stroka4_temp[:2]
                stroka5 = [stroka2, stroka3 + '.' + stroka4]
                csv_list.append(stroka5)
                # print(stroka2,stroka3, stroka4)
                # print(stroka2_temp)
                # print(stroka4_temp)
                # print(stroka5)
    # print(len(csv_list))
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
    # print('Дата3-2часа =', time_temp9)

    # ********************************************************************************************************************
    # ********************************************************************************************************************
    # ********************************************************************************************************************
    ########### перебираю и записываю в список для отправки в БД строчки реверсного файла до тех пор,
    # пока она не будет строчка из реверсного файла не будет равно строчке в connection_file
    # конвертирую строку с датой из connection_file.txt в тип datetime
    txt_time_ponit_norm = datetime.datetime.strptime((txt_time_ponit[0:19]).strip(),
                                                     '%Y-%m-%d %H:%M:%S')  # преобразовываю
    # print('txt_time_ponit_norm', txt_time_ponit_norm)

    select_spisok = []
    for row in csv_list:
        # print('row[0]', row[0])
        if row[0] == txt_time_ponit_norm:  # если строчка для отправки = строчки из файла txt, то записываю эту строчку
            # print('break')
            select_spisok.append(row)
            break

        elif row[0] >= txt_time_ponit_norm:

            if row[0] > txt_time_ponit_norm:
                select_spisok.append(row)

    # print('select_spisok', select_spisok)

    data_to_db = list(reversed(select_spisok))  # список для отправки в БД

    # временно присваиваю для проверки, после доступа к Кафка, это и код по считыванию UserLog необходимо закоментировать или удалить

    data_kafka = select_spisok

    # print('data_kafka', data_kafka)
    # ********************************************************************************************************************

    ################################ отправляю данные из сформированного списка в БД ####################################
    if len(data_kafka) > 1:
        # print('len(data_kafka)', len(data_kafka))
        # print('len(data_kafka[0])', len(data_kafka[0]))

        # data_kafka.pop(0)  # удаляю последнию строчку т.к. она уже есть в БД и её не надо повторно отравлять в БД

        try:
            #################################### отрываю БД и считываю таблицу и определяю последнюю(MAX) дату и строки
            # для сравнения #################################################################################################
            # !!!!!!!! заменить конекшенстринг
            conn = pyodbc.connect(
                txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки
            cur = conn.cursor()  # создали курсор, через который можно работать с БД
            # cur.fast_executemany = True  # это для ускорения передачи данных, но в этой версии Питона не хочет работать

            ######## запись в БД выбранных значений###################

            sql = "INSERT INTO Exh3_Temp1 (TagTime, TagValue) VALUES (?, ?)"

            # print(11)
            cur.executemany(sql,
                            data_kafka)  ############################################################################ДЛЯ ПЕРЕДАЧИ ДАННЫХ РАСКОММЕНТИРОВАТЬ!!!!!!!!!!!!!!!!!!
            ##########################################################################################
            # print(22)
            conn.commit()  # сохраняем изменения в БД

            # повторно читаем, чтобы проверить, записались ли данные в БД
            conn = pyodbc.connect(
                txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки
            # print(33)
            cur = conn.cursor()  # создали курсор, через который можно работать с БД
            cur.fast_executemany = True  # это для ускорения передачи данных, но в этой версии Питона не хочет работать
            # print(44)

            ####### определяю последнее записанное значение по максимальной дате ################################
            cur.execute(
                "select TagTime, TagValue  from Exh3_Temp1 where TagTime = (select MAX(TagTime) from Exh3_Temp1)")  # записываю в курсор максимальное значение времени из БД
            # print(1)
            db_list = []  # создаю пустой список для дальнейшей записи в него строки с максимальным времением.
            for line in cur:
                # print(55)
                db_list.append(line)
            # проверяю наличие строчки в БД, которую отправляли по времени.
            # print(db_list[0][0])
            # print(data_kafka[0][0])

            if (db_list[0][0] == data_kafka[0][0]):
                # print(77)

                # считываю connection_file.txt для создания списка lines, который потом редактирую и делаю перезапись
                # файла connection_file.txt
                with open('Exh3/conn_file/conn_file_Exh3_Temp1.txt', 'r', encoding='utf-8') as fr:
                    lines = fr.readlines()

                # запись в connection_file.txt
                # print('lines[7]', lines[7])
                lines[7] = str(data_kafka[0][0]) + str(
                    '\n')  # добавил \n при записи в список пустая строка под записью обрезается
                # print('lines[7]', lines[7])
                # print('lines[10]', lines[10])
                # lines[10] = str(data_kafka[0][3]) + str('\n')
                # print('lines[10]', lines[10])
                # lines[13] = str(data_kafka[0][4]) + str('\n')'lines[7]',
                # записываю в данные в connection_file.txt
                # print(lines)
                with open('Exh3/conn_file/conn_file_Exh3_Temp1.txt', 'w', encoding='utf-8') as fl:
                    # print(88)
                    fl.writelines(lines)

            conn.commit()  # сохраняем изменения в БД
            conn.close()  # закрываем подключение к базе
        except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
            pass
        finally:
            pass

# читаю и передаю данные температуры т.2
def exh3_Temp2():
    ######################## Считываю данные из conn_file_Exh_ для опредения последнего записанного времени

    with open('Exh3/conn_file/conn_file_Exh3_Temp2.txt', 'r', encoding='utf-8') as file:

        txt_lines = file.readlines()

    txt_connection_string = txt_lines[1].strip()  # connection string
    # txt_logfile = txt_lines[4].strip()  # logfile
    txt_time_ponit = txt_lines[7].strip()  # time_ponit
    # txt_command = txt_lines[10].strip()  # command
    # txt_status_command = txt_lines[13].strip()  # status_command

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

    # **********************************************************************************************************************
    # **********************************************После подключения Кафка раскоментировать***********************************
    # **********************************************************************************************************************
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

    # **********************************************************************************************************************
    # **********************************************************************************************************************
    # **********************************************************************************************************************
    #####################################################################################################################

    # ********************************************************************************************************************
    # *******************************временно читаю из файла, как будто из кафки, чтобы проверить алгоритм*************
    # ********************************************************************************************************************
    ###########################  читаю csv файл ##############################################################

    # указываю путь, где находится логфайл для чтения
    # path = r'C:\Users\User\Desktop\Хакатон\Новая папка (2)\Новая папка (2)\Exh3_Vibr_2.csv.csv'
    # создаю пустой список, в который перенесу все данные из Logfile
    csv_list = []
    with open('Exh3/csv_file/Exh3_Temp2.csv', 'r', newline='', encoding='ANSI') as f:

        reader = csv.reader(f, quoting=csv.QUOTE_NONE)  # delimiter=' ' - ставит запятую вместо пробела
        for nr in reader:  #
            if len(nr) == 1:
                stroka1 = nr[0]
                # print(1)
            if len(nr) == 2:
                stroka2_temp = nr[0]
                stroka2_2 = stroka2_temp[0:19]
                stroka2 = datetime.datetime.strptime((stroka2_2).strip(), '%Y-%m-%d %H:%M:%S')
                stroka3 = stroka2_temp[24:26]
                stroka4_temp = nr[1]
                stroka4 = stroka4_temp[:2]
                stroka5 = [stroka2, stroka3 + '.' + stroka4]
                csv_list.append(stroka5)
                #print(stroka2,stroka3, stroka4)
                #print(stroka5)
    # print(len(csv_list))
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
    # print('Дата3-2часа =', time_temp9)

    # ********************************************************************************************************************
    # ********************************************************************************************************************
    # ********************************************************************************************************************
    ########### перебираю и записываю в список для отправки в БД строчки реверсного файла до тех пор,
    # пока она не будет строчка из реверсного файла не будет равно строчке в connection_file
    # конвертирую строку с датой из connection_file.txt в тип datetime
    txt_time_ponit_norm = datetime.datetime.strptime((txt_time_ponit[0:19]).strip(),
                                                     '%Y-%m-%d %H:%M:%S')  # преобразовываю
    # print('txt_time_ponit_norm', txt_time_ponit_norm)

    select_spisok = []
    for row in csv_list:
        # print('row[0]', row[0])
        if row[0] == txt_time_ponit_norm:  # если строчка для отправки = строчки из файла txt, то записываю эту строчку
            # print('break')
            select_spisok.append(row)
            break

        elif row[0] >= txt_time_ponit_norm:

            if row[0] > txt_time_ponit_norm:
                select_spisok.append(row)

    # print('select_spisok', select_spisok)

    data_to_db = list(reversed(select_spisok))  # список для отправки в БД

    # временно присваиваю для проверки, после доступа к Кафка, это и код по считыванию UserLog необходимо закоментировать или удалить

    data_kafka = select_spisok

    # ********************************************************************************************************************

    ################################ отправляю данные из сформированного списка в БД ####################################
    if len(data_kafka) > 1:
        # print('len(data_kafka)', len(data_kafka))
        # print('len(data_kafka[0])', len(data_kafka[0]))

        # data_kafka.pop(0)  # удаляю последнию строчку т.к. она уже есть в БД и её не надо повторно отравлять в БД

        try:
            #################################### отрываю БД и считываю таблицу и определяю последнюю(MAX) дату и строки
            # для сравнения #################################################################################################
            # !!!!!!!! заменить конекшенстринг
            conn = pyodbc.connect(
                txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки
            cur = conn.cursor()  # создали курсор, через который можно работать с БД
            # cur.fast_executemany = True  # это для ускорения передачи данных, но в этой версии Питона не хочет работать

            ######## запись в БД выбранных значений###################

            sql = "INSERT INTO Exh3_Temp2 (TagTime, TagValue) VALUES (?, ?)"

            # print(11)
            cur.executemany(sql,
                            data_kafka)  ############################################################################ДЛЯ ПЕРЕДАЧИ ДАННЫХ РАСКОММЕНТИРОВАТЬ!!!!!!!!!!!!!!!!!!
            ##########################################################################################
            # print(22)
            conn.commit()  # сохраняем изменения в БД

            # повторно читаем, чтобы проверить, записались ли данные в БД
            conn = pyodbc.connect(
                txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки
            # print(33)
            cur = conn.cursor()  # создали курсор, через который можно работать с БД
            cur.fast_executemany = True  # это для ускорения передачи данных, но в этой версии Питона не хочет работать
            # print(44)

            ####### определяю последнее записанное значение по максимальной дате ################################
            cur.execute(
                "select TagTime, TagValue  from Exh3_Temp2 where TagTime = (select MAX(TagTime) from Exh3_Temp2)")  # записываю в курсор максимальное значение времени из БД
            # print(1)
            db_list = []  # создаю пустой список для дальнейшей записи в него строки с максимальным времением.
            for line in cur:
                # print(55)
                db_list.append(line)
            # проверяю наличие строчки в БД, которую отправляли по времени.
            # print(db_list[0][0])
            # print(data_kafka[0][0])

            if (db_list[0][0] == data_kafka[0][0]):
                # print(77)

                # считываю connection_file.txt для создания списка lines, который потом редактирую и делаю перезапись
                # файла connection_file.txt
                with open('Exh3/conn_file/conn_file_Exh3_Temp2.txt', 'r', encoding='utf-8') as fr:
                    lines = fr.readlines()

                # запись в connection_file.txt
                # print('lines[7]', lines[7])
                lines[7] = str(data_kafka[0][0]) + str(
                    '\n')  # добавил \n при записи в список пустая строка под записью обрезается
                # print('lines[7]', lines[7])
                # print('lines[10]', lines[10])
                # lines[10] = str(data_kafka[0][3]) + str('\n')
                # print('lines[10]', lines[10])
                # lines[13] = str(data_kafka[0][4]) + str('\n')'lines[7]',
                # записываю в данные в connection_file.txt
                # print(lines)
                with open('Exh3/conn_file/conn_file_Exh3_Temp2.txt', 'w', encoding='utf-8') as fl:
                    # print(88)
                    fl.writelines(lines)

            conn.commit()  # сохраняем изменения в БД
            conn.close()  # закрываем подключение к базе
        except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
            pass
        finally:
            pass

# читаю и передаю данные температуры т.3
def exh3_Temp3():
    ######################## Считываю данные из conn_file_Exh_ для опредения последнего записанного времени

    with open('Exh3/conn_file/conn_file_Exh3_Temp3.txt', 'r', encoding='utf-8') as file:

        txt_lines = file.readlines()

    txt_connection_string = txt_lines[1].strip()  # connection string
    # txt_logfile = txt_lines[4].strip()  # logfile
    txt_time_ponit = txt_lines[7].strip()  # time_ponit
    # txt_command = txt_lines[10].strip()  # command
    # txt_status_command = txt_lines[13].strip()  # status_command

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

    # **********************************************************************************************************************
    # **********************************************После подключения Кафка раскоментировать***********************************
    # **********************************************************************************************************************
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

    # **********************************************************************************************************************
    # **********************************************************************************************************************
    # **********************************************************************************************************************
    #####################################################################################################################

    # ********************************************************************************************************************
    # *******************************временно читаю из файла, как будто из кафки, чтобы проверить алгоритм*************
    # ********************************************************************************************************************
    ###########################  читаю csv файл ##############################################################

    # указываю путь, где находится логфайл для чтения
    # path = r'C:\Users\User\Desktop\Хакатон\Новая папка (2)\Новая папка (2)\Exh3_Vibr_2.csv.csv'
    # создаю пустой список, в который перенесу все данные из Logfile
    csv_list = []
    with open('Exh3/csv_file/Exh3_Temp3.csv', 'r', newline='', encoding='ANSI') as f:

        reader = csv.reader(f, quoting=csv.QUOTE_NONE)  # delimiter=' ' - ставит запятую вместо пробела
        for nr in reader:  #
            if len(nr) == 1:
                stroka1 = nr[0]
                # print(1)
            if len(nr) == 2:
                stroka2_temp = nr[0]
                stroka2_2 = stroka2_temp[0:19]
                stroka2 = datetime.datetime.strptime((stroka2_2).strip(), '%Y-%m-%d %H:%M:%S')
                stroka3 = stroka2_temp[24:26]
                stroka4_temp = nr[1]
                stroka4 = stroka4_temp[:2]
                stroka5 = [stroka2, stroka3 + '.' + stroka4]
                csv_list.append(stroka5)
                # print(stroka2,stroka3, stroka4)
                # print(stroka5)
    # print(len(csv_list))
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
    # print('Дата3-2часа =', time_temp9)

    # ********************************************************************************************************************
    # ********************************************************************************************************************
    # ********************************************************************************************************************
    ########### перебираю и записываю в список для отправки в БД строчки реверсного файла до тех пор,
    # пока она не будет строчка из реверсного файла не будет равно строчке в connection_file
    # конвертирую строку с датой из connection_file.txt в тип datetime
    txt_time_ponit_norm = datetime.datetime.strptime((txt_time_ponit[0:19]).strip(),
                                                     '%Y-%m-%d %H:%M:%S')  # преобразовываю
    # print('txt_time_ponit_norm', txt_time_ponit_norm)

    select_spisok = []
    for row in csv_list:
        # print('row[0]', row[0])
        if row[0] == txt_time_ponit_norm:  # если строчка для отправки = строчки из файла txt, то записываю эту строчку
            # print('break')
            select_spisok.append(row)
            break

        elif row[0] >= txt_time_ponit_norm:

            if row[0] > txt_time_ponit_norm:
                select_spisok.append(row)

    # print('select_spisok', select_spisok)

    data_to_db = list(reversed(select_spisok))  # список для отправки в БД

    # временно присваиваю для проверки, после доступа к Кафка, это и код по считыванию UserLog необходимо закоментировать или удалить

    data_kafka = select_spisok

    # ********************************************************************************************************************

    ################################ отправляю данные из сформированного списка в БД ####################################
    if len(data_kafka) > 1:
        # print('len(data_kafka)', len(data_kafka))
        # print('len(data_kafka[0])', len(data_kafka[0]))

        # data_kafka.pop(0)  # удаляю последнию строчку т.к. она уже есть в БД и её не надо повторно отравлять в БД

        try:
            #################################### отрываю БД и считываю таблицу и определяю последнюю(MAX) дату и строки
            # для сравнения #################################################################################################
            # !!!!!!!! заменить конекшенстринг
            conn = pyodbc.connect(
                txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки
            cur = conn.cursor()  # создали курсор, через который можно работать с БД
            # cur.fast_executemany = True  # это для ускорения передачи данных, но в этой версии Питона не хочет работать

            ######## запись в БД выбранных значений###################

            sql = "INSERT INTO Exh3_Temp3 (TagTime, TagValue) VALUES (?, ?)"

            # print(11)
            cur.executemany(sql,
                            data_kafka)  ############################################################################ДЛЯ ПЕРЕДАЧИ ДАННЫХ РАСКОММЕНТИРОВАТЬ!!!!!!!!!!!!!!!!!!
            ##########################################################################################
            # print(22)
            conn.commit()  # сохраняем изменения в БД

            # повторно читаем, чтобы проверить, записались ли данные в БД
            conn = pyodbc.connect(
                txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки
            # print(33)
            cur = conn.cursor()  # создали курсор, через который можно работать с БД
            cur.fast_executemany = True  # это для ускорения передачи данных, но в этой версии Питона не хочет работать
            # print(44)

            ####### определяю последнее записанное значение по максимальной дате ################################
            cur.execute(
                "select TagTime, TagValue  from Exh3_Temp3 where TagTime = (select MAX(TagTime) from Exh3_Temp3)")  # записываю в курсор максимальное значение времени из БД
            # print(1)
            db_list = []  # создаю пустой список для дальнейшей записи в него строки с максимальным времением.
            for line in cur:
                # print(55)
                db_list.append(line)
            # проверяю наличие строчки в БД, которую отправляли по времени.
            # print(db_list[0][0])
            # print(data_kafka[0][0])

            if (db_list[0][0] == data_kafka[0][0]):
                # print(77)

                # считываю connection_file.txt для создания списка lines, который потом редактирую и делаю перезапись
                # файла connection_file.txt
                with open('Exh3/conn_file/conn_file_Exh3_Temp3.txt', 'r', encoding='utf-8') as fr:
                    lines = fr.readlines()

                # запись в connection_file.txt
                # print('lines[7]', lines[7])
                lines[7] = str(data_kafka[0][0]) + str(
                    '\n')  # добавил \n при записи в список пустая строка под записью обрезается
                # print('lines[7]', lines[7])
                # print('lines[10]', lines[10])
                # lines[10] = str(data_kafka[0][3]) + str('\n')
                # print('lines[10]', lines[10])
                # lines[13] = str(data_kafka[0][4]) + str('\n')'lines[7]',
                # записываю в данные в connection_file.txt
                # print(lines)
                with open('Exh3/conn_file/conn_file_Exh3_Temp3.txt', 'w', encoding='utf-8') as fl:
                    # print(88)
                    fl.writelines(lines)

            conn.commit()  # сохраняем изменения в БД
            conn.close()  # закрываем подключение к базе
        except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
            pass
        finally:
            pass


# читаю и передаю данные температуры т.7


def exh3_Temp7():
    with open('Exh3/conn_file/conn_file_Exh3_Temp7.txt', 'r', encoding='utf-8') as file:

        txt_lines = file.readlines()

    txt_connection_string = txt_lines[1].strip()  # connection string
    # txt_logfile = txt_lines[4].strip()  # logfile
    txt_time_ponit = txt_lines[7].strip()  # time_ponit
    # txt_command = txt_lines[10].strip()  # command
    # txt_status_command = txt_lines[13].strip()  # status_command

    ##################################################################################################################
    list_Exh3_Temp7, list_Exh3_Temp8, list_Exh3_Vibr7, list_Exh3_Vibr8 = hendlerDB4()

    csv_list = list_Exh3_Temp7

    # with open('Exh3/csv_file/Exh3_Temp7.csv', 'r', newline='', encoding='ANSI') as f:
    #
    #     reader = csv.reader(f, quoting=csv.QUOTE_NONE)  # delimiter=' ' - ставит запятую вместо пробела
    #     for nr in reader:  #
    #         if len(nr) == 1:
    #             stroka1 = nr[0]
    #             # print(1)
    #         if len(nr) == 2:
    #             stroka2_temp = nr[0]
    #             stroka2_2 = stroka2_temp[0:19]
    #             stroka2 = datetime.datetime.strptime((stroka2_2).strip(), '%Y-%m-%d %H:%M:%S')
    #             stroka3 = stroka2_temp[24:26]
    #             stroka4_temp = nr[1]
    #             stroka4 = stroka4_temp[:2]
    #             stroka5 = [stroka2, stroka3 + '.' + stroka4]
    #             csv_list.append(stroka5)
    #             # print(stroka2,stroka3, stroka4)
    #             # print(stroka2_temp)
    #             # print(stroka4_temp)
    #             # print(stroka5)
    # # print(len(csv_list))
    # # print('Дата1', csv_list[0][0])
    # # print('Дата2', csv_list[1][0])
    # # print('Разность Дата1 - Дата2',csv_list[0][0]-csv_list[1][0])
    # #
    # # time_temp1=int((str(csv_list[0][0]))[11:13])
    # # time_temp5= int((str(csv_list[14111][0]))[11:13])
    # #
    # # if time_temp1 != 0:
    # #     time_temp2=int((str(csv_list[0][0]))[11:13])-2
    # #     time_temp3=str(csv_list[0][0])[:11]+str(time_temp2)+str(csv_list[0][0])[13:]
    # #     time_temp4=datetime.datetime.strptime((time_temp3).strip(), '%Y-%m-%d %H:%M:%S')
    # #     # print('Дата1 - 2 часа = ', time_temp4)
    # #
    # # if time_temp5==0: # проверка, если нуль часов, то из нуля вычитать не корректно, поэтому вычитаем из 24
    # #     time_temp6 = 24-2
    # #     time_temp7 = str(csv_list[14111][0])[:11]+str(time_temp6)+str(csv_list[14111][0])[13:]
    # #
    # #     time_temp9 = datetime.datetime.strptime((time_temp7).strip(), '%Y-%m-%d %H:%M:%S')
    #
    # # print("Дата3", (str(csv_list[14111][0])))
    # # print('Дата3-2часа =', time_temp9)
    #
    # # ********************************************************************************************************************
    # # ********************************************************************************************************************
    # # ********************************************************************************************************************
    # ########### перебираю и записываю в список для отправки в БД строчки реверсного файла до тех пор,
    # # пока она не будет строчка из реверсного файла не будет равно строчке в connection_file
    # # конвертирую строку с датой из connection_file.txt в тип datetime
    txt_time_ponit_norm = datetime.datetime.strptime((txt_time_ponit[0:19]).strip(),
                                                     '%Y-%m-%d %H:%M:%S')  # преобразовываю
    # print('txt_time_ponit_norm', txt_time_ponit_norm)

    select_spisok = []

    print('csv_list в функции exh3_Temp7()', csv_list)
    for row in csv_list:
        # print('row[0]', row[0])
        #print('row[0]7777777777777777777777777777777777777777777777777777777', row[0])
        #print('txt_time_ponit_norm77777777777777777777777777777777777', txt_time_ponit_norm)
        #print('type(row[0])7777777777777777777777777777777777777777777', type(row[0]))
        #print('type(txt_time_ponit_norm)77777777777777777777777777', type(txt_time_ponit_norm))

        if row[0] == txt_time_ponit_norm:  # если строчка для отправки = строчки из файла txt, то записываю эту строчку
            # print('break')
            select_spisok.append(row)
            break

        elif row[0] >= txt_time_ponit_norm:

            if row[0] > txt_time_ponit_norm:
                select_spisok.append(row)

    # print('select_spisok', select_spisok)

    data_to_db = list(reversed(select_spisok))  # список для отправки в БД

    # временно присваиваю для проверки, после доступа к Кафка, это и код по считыванию UserLog необходимо закоментировать или удалить

    data_kafka = select_spisok

    print('data_kafka777777777777777777777777777777777777777', data_kafka)
    # ********************************************************************************************************************

    ################################ отправляю данные из сформированного списка в БД ####################################
    if len(data_kafka) > 1:
        print('len(data_kafka)777777777777777777777777777777777', len(data_kafka))
        # print('len(data_kafka[0])', len(data_kafka[0]))

        # data_kafka.pop(0)  # удаляю последнию строчку т.к. она уже есть в БД и её не надо повторно отравлять в БД

        try:
            #################################### отрываю БД и считываю таблицу и определяю последнюю(MAX) дату и строки
            # для сравнения #################################################################################################
            # !!!!!!!! заменить конекшенстринг
            conn = pyodbc.connect(
                txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки
            cur = conn.cursor()  # создали курсор, через который можно работать с БД
            # cur.fast_executemany = True  # это для ускорения передачи данных, но в этой версии Питона не хочет работать

            ######## запись в БД выбранных значений###################

            sql = "INSERT INTO Exh3_Temp7 (TagTime, TagValue) VALUES (?, ?)"

            # print(11)
            cur.executemany(sql,
                            data_kafka)  ############################################################################ДЛЯ ПЕРЕДАЧИ ДАННЫХ РАСКОММЕНТИРОВАТЬ!!!!!!!!!!!!!!!!!!
            ##########################################################################################
            # print(22)
            conn.commit()  # сохраняем изменения в БД

            # повторно читаем, чтобы проверить, записались ли данные в БД
            conn = pyodbc.connect(
                txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки
            # print(33)
            cur = conn.cursor()  # создали курсор, через который можно работать с БД
            cur.fast_executemany = True  # это для ускорения передачи данных, но в этой версии Питона не хочет работать
            # print(44)

            ####### определяю последнее записанное значение по максимальной дате ################################
            cur.execute(
                "select TagTime, TagValue  from Exh3_Temp7 where TagTime = (select MAX(TagTime) from Exh3_Temp7)")  # записываю в курсор максимальное значение времени из БД
            # print(1)
            db_list = []  # создаю пустой список для дальнейшей записи в него строки с максимальным времением.
            for line in cur:
                print(212121212121212122112121212121212121212121212121212121212121212121212)
                db_list.append(line)
            # проверяю наличие строчки в БД, которую отправляли по времени.
            # print(db_list[0][0])
            # print(data_kafka[0][0])

            if (db_list[0][0] == data_kafka[0][0]):
                # print(77)

                # считываю connection_file.txt для создания списка lines, который потом редактирую и делаю перезапись
                # файла connection_file.txt
                with open('Exh3/conn_file/conn_file_Exh3_Temp7.txt', 'r', encoding='utf-8') as fr:
                    lines = fr.readlines()

                # запись в connection_file.txt
                # print('lines[7]', lines[7])
                lines[7] = str(data_kafka[0][0]) + str(
                    '\n')  # добавил \n при записи в список пустая строка под записью обрезается
                # print('lines[7]', lines[7])
                # print('lines[10]', lines[10])
                # lines[10] = str(data_kafka[0][3]) + str('\n')
                # print('lines[10]', lines[10])
                # lines[13] = str(data_kafka[0][4]) + str('\n')'lines[7]',
                # записываю в данные в connection_file.txt
                # print(lines)
                with open('Exh3/conn_file/conn_file_Exh3_Temp7.txt', 'w', encoding='utf-8') as fl:
                    # print(88)
                    fl.writelines(lines)

            conn.commit()  # сохраняем изменения в БД
            conn.close()  # закрываем подключение к базе
        except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
            pass
        finally:
            pass


##############################обработчик второй потока ##################################################################

def hendlerDB2(): # функция обработчик второго потока данных

    exh3_Alarm_Vibr1()
    #print(333333)
    exh3_Alarm_Vibr2()
    #print(444444)
    exh3_Alarm_Temp1()
    #print(55555)
    exh3_Alarm_Temp2()
    #print(66666666666666666666666666666666666666666)

    exh3_Alarm_Temp7()
    #print(88888888888888888888888888888888888888888)

    predict_Exh3()
    #print(77777)




# определение выхода из строя ротора из-за вибрации т.1
def exh3_Alarm_Vibr1():
    ######################## Считываю данные из conn_file_Exh_ для опредения последнего записанного времени

    with open('Exh3/conn_file/conn_file_Exh3_Vibr1.txt', 'r', encoding='utf-8') as file:

        txt_lines = file.readlines()

    txt_connection_string = txt_lines[1].strip()  # connection string

    txt_time_ponit = txt_lines[7].strip()  # time_ponit

    ##################################################################################################################

    ############################ считываю данные из БД ###################################################
    try:
        #################################### отрываю БД и считываю таблицу и определяю последнюю(MAX) дату и строки

        conn = pyodbc.connect(
            txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки

        cur = conn.cursor()  # создали курсор, через который можно работать с БД
        cur.fast_executemany = True  # это для ускорения передачи данных

        ####### определяю последнее записанное значение по максимальной дате ################################
        cur.execute(
            "select TagTime, TagValue  from Exh3_Vibr1 where TagTime = (select MAX(TagTime) from Exh3_Vibr1)")  # записываю в курсор максимальное значение времени из БД

        point_2_1 = []  # создаю пустой список для дальнейшей записи в него последнего времени и значения переменной
        for p in cur:
            point_2_1.append(p)
        for k in point_2_1:
            x2_1 = k[0]
            m2_1 = k[1]

        #print('x2_1', x2_1)
        #print('m2_1', m2_1)
        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! для ручного изменеия текущей даты. Потом закоментировать!!!!!!!!!!!!!!!!
        # temp777_x2_1 = '2022-08-06 20:19:52'
        # x2_1 = datetime.datetime.strptime((temp777_x2_1).strip(),
        #                                   '%Y-%m-%d %H:%M:%S')

        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

        # *************************************** нахожу дату и время со сдвигом 1 час и 12 часов *******************************

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

        # print('x1_1', x1_1)
        ###################### x1_2 - минус 13 часов    #################################
        delta1_x1_2_norm = datetime.datetime.strptime((delta1_x1_2_str).strip(),
                                                      '%Y-%m-%d %H:%M:%S')  # нормализую для вычислений
        delta2_x1_2_norm = datetime.datetime.strptime((delta2_x1_2_str).strip(),
                                                      '%Y-%m-%d %H:%M:%S')  # нормализую для вычислений

        x1_2 = x1_1 - delta1_x1_2_norm + delta2_x1_2_norm  # вычисляю дату и время сдвинутую на минус 12 часов
        #print('x1_2', x1_2)

        # ******************* преобразую дату и время, иначе не делается запрос из БД по условию********************************
        temp_x2_1 = (str(x2_1))[:10] + 'T' + (str(x2_1))[11:]
        # print('x2_1_convert', temp_x2_1)
        temp_x2_2 = (str(x2_2))[:10] + 'T' + (str(x2_2))[11:]
        temp_x1_1 = (str(x1_1))[:10] + 'T' + (str(x1_1))[11:]
        temp_x1_2 = (str(x1_2))[:10] + 'T' + (str(x1_2))[11:]

        # ********************************** делаю выгрузку данных за текущий час для нахождения среднего значения **********
        temp_str2 = 'select TagTime, TagValue  from Exh3_Vibr1 where TagTime <= ' + "'" + temp_x2_1 + "'" + ' and ' + 'TagTime >= ' + "'" + temp_x2_2 + "'"
        # print('temp_str2', temp_str2)
        # print('x2_1', x2_1)
        # print('x2_2', x2_2)
        mean_point_2 = []  # создаю пустой список (первый час) для дальнейшей записи в него последнего времени и значения переменной
        cur.execute(temp_str2)  # записываю в курсор данные за последний час
        # print('222')
        for mnp2 in cur:
            mean_point_2.append(mnp2)

        # print('mean_point_2', len(mean_point_2))

        # *********************************************************************************************************************

        # ********************************** делаю выгрузку данных за 12 час для нахождения среднего значения **********

        exh3_Alarm_Vibr1_temp_str1 = 'select TagTime, TagValue  from Exh3_Vibr1 where TagTime <= ' + "'" + temp_x1_1 + "'" + ' and ' + 'TagTime >= ' + "'" + temp_x1_2 + "'"
        # print('temp_str1', temp_str1)
        #print('x1_1', x1_1)
        #print('x1_2', x1_2)

        mean_point_1 = []  # создаю пустой список (12 час) для дальнейшей записи в него последнего времени и значения переменной
        cur.execute(exh3_Alarm_Vibr1_temp_str1)  # записываю в курсор данные за последний час
        # print('333')
        # print(type(cur))
        for mnp1 in cur:
            mean_point_1.append(mnp1)

        # print('mean_point_1', len(mean_point_1))
        conn.commit()  # сохраняем изменения в БД
        conn.close()
        ####################################################################################################################

        ################################ нахожу среднее значение в первый и последний(12) час ##############################
        sum2 = 0
        count_2 = 0
        for kl in mean_point_2:
            value2 = kl[1]
            sum2 = sum2 + value2
            count_2 += 1
        exh3_Alarm_Vibr1_mean2 = round(sum2 / count_2, 2)  # округляю
        # print(sum2)
        # print(count_2)
        # print('среднее значение за 1 час(M2)', mean2)

        sum1 = 0
        count_1 = 0
        for kb in mean_point_1:
            value1 = kb[1]
            sum1 = sum1 + value1
            count_1 += 1
        exh3_Alarm_Vibr1_mean1 = round(sum1 / count_1, 2)  # округляю
        # print(sum1)
        # print(count_1)
        #print('среднее значение за 12 час(M1)', exh3_Alarm_Vibr1_mean1)

        # *******************************************************************************************
        exh3_Alarm_Vibr1_mean = 7  # уставка, при достижении, которой необходимо узнать дату и время *************
        # *******************************************************************************************

        #print('exh3_Alarm_Vibr1_mean1', exh3_Alarm_Vibr1_mean1)
        #print('exh3_Alarm_Vibr1_mean2', exh3_Alarm_Vibr1_mean2)

        if exh3_Alarm_Vibr1_mean1 > exh3_Alarm_Vibr1_mean2:
            print('снижение вибрации за рассматриваемый диапазон по вибрации т.2')
            exh3_Alarm_Vibr1_predictor = datetime.datetime.strptime(('9999-02-01 01:01:01').strip(),
                                                   '%Y-%m-%d %H:%M:%S')
            exh3_Alarm_Vibr1_str_time_to_alarm = '9999-09-09'
            #print('exh3_Alarm_Vibr1_predictor', exh3_Alarm_Vibr1_predictor)

        else:
            exh3_Alarm_Vibr1_predictor = ((exh3_Alarm_Vibr1_mean - exh3_Alarm_Vibr1_mean1) * (x2_1 - x1_1)) / (exh3_Alarm_Vibr1_mean2 - exh3_Alarm_Vibr1_mean1) + x1_1
            #print('exh3_Alarm_Vibr1_predictor', exh3_Alarm_Vibr1_predictor)

            # print('дата и время выхода из строя ротора', (str(predictor))[:19])

            exh3_Alarm_Vibr1_time_to_alarm = exh3_Alarm_Vibr1_predictor - x2_1
            exh3_Alarm_Vibr1_str_time_to_alarm = (str(exh3_Alarm_Vibr1_time_to_alarm))[:16]

            print('количество дней до выхода из строя ротора по Vibr1', exh3_Alarm_Vibr1_str_time_to_alarm)
        return exh3_Alarm_Vibr1_predictor, exh3_Alarm_Vibr1_str_time_to_alarm
        conn.commit()  # сохраняем изменения в БД
        conn.close()  # закрываем подключение к базе
    except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
        pass
    finally:
        pass

# определение времени выхода из строя ротора  по вибрации т.2
def exh3_Alarm_Vibr2():
    ######################## Считываю данные из conn_file_Exh_ для опредения последнего записанного времени

    with open('Exh3/conn_file/conn_file_Exh3_Vibr2.txt', 'r', encoding='utf-8') as file:

        txt_lines = file.readlines()

    txt_connection_string = txt_lines[1].strip()  # connection string

    txt_time_ponit = txt_lines[7].strip()  # time_ponit

    ##################################################################################################################

    ############################ считываю данные из БД ###################################################
    try:
        #################################### отрываю БД и считываю таблицу и определяю последнюю(MAX) дату и строки

        conn = pyodbc.connect(
            txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки

        cur = conn.cursor()  # создали курсор, через который можно работать с БД
        cur.fast_executemany = True  # это для ускорения передачи данных

        ####### определяю последнее записанное значение по максимальной дате ################################
        cur.execute(
            "select TagTime, TagValue  from Exh3_Vibr2 where TagTime = (select MAX(TagTime) from Exh3_Vibr2)")  # записываю в курсор максимальное значение времени из БД

        point_2_1 = []  # создаю пустой список для дальнейшей записи в него последнего времени и значения переменной
        for p in cur:
            point_2_1.append(p)
        for k in point_2_1:
            x2_1 = k[0]
            m2_1 = k[1]

        #print('x2_1', x2_1)
        #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! для ручного изменеия текущей даты. Потом закоментировать!!!!!!!!!!!!!!!!
        # temp777_x2_1 = '2022-08-06 20:19:52'
        # x2_1 = datetime.datetime.strptime((temp777_x2_1).strip(),
        #                                   '%Y-%m-%d %H:%M:%S')

        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

        # *************************************** нахожу дату и время со сдвигом 1 час и 12 часов *******************************

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

        # print('x1_1', x1_1)
        ###################### x1_2 - минус 13 часов    #################################
        delta1_x1_2_norm = datetime.datetime.strptime((delta1_x1_2_str).strip(),
                                                      '%Y-%m-%d %H:%M:%S')  # нормализую для вычислений
        delta2_x1_2_norm = datetime.datetime.strptime((delta2_x1_2_str).strip(),
                                                      '%Y-%m-%d %H:%M:%S')  # нормализую для вычислений

        x1_2 = x1_1 - delta1_x1_2_norm + delta2_x1_2_norm  # вычисляю дату и время сдвинутую на минус 12 часов
        # print('x1_2', x1_2)

        # ******************* преобразую дату и время, иначе не делается запрос из БД по условию********************************
        temp_x2_1 = (str(x2_1))[:10] + 'T' + (str(x2_1))[11:]
        # print('x2_1_convert', temp_x2_1)
        temp_x2_2 = (str(x2_2))[:10] + 'T' + (str(x2_2))[11:]
        temp_x1_1 = (str(x1_1))[:10] + 'T' + (str(x1_1))[11:]
        temp_x1_2 = (str(x1_2))[:10] + 'T' + (str(x1_2))[11:]

        # ********************************** делаю выгрузку данных за текущий час для нахождения среднего значения **********
        exh3_Alarm_Vibr2_temp_str2 = 'select TagTime, TagValue  from Exh3_Vibr2 where TagTime <= ' + "'" + temp_x2_1 + "'" + ' and ' + 'TagTime >= ' + "'" + temp_x2_2 + "'"
        # print('temp_str2', temp_str2)
        # print('x2_1', x2_1)
        # print('x2_2', x2_2)
        mean_point_2 = []  # создаю пустой список (первый час) для дальнейшей записи в него последнего времени и значения переменной
        cur.execute(exh3_Alarm_Vibr2_temp_str2)  # записываю в курсор данные за последний час
        # print('222')
        for mnp2 in cur:
            mean_point_2.append(mnp2)

        # print('mean_point_2', len(mean_point_2))

        # *********************************************************************************************************************

        # ********************************** делаю выгрузку данных за 12 час для нахождения среднего значения **********

        exh3_Alarm_Vibr2_temp_str1 = 'select TagTime, TagValue  from Exh3_Vibr2 where TagTime <= ' + "'" + temp_x1_1 + "'" + ' and ' + 'TagTime >= ' + "'" + temp_x1_2 + "'"
        # print('temp_str1', temp_str1)
        # print('x1_1', x1_1)
        # print('x1_2', x1_2)

        mean_point_1 = []  # создаю пустой список (12 час) для дальнейшей записи в него последнего времени и значения переменной
        cur.execute(exh3_Alarm_Vibr2_temp_str1)  # записываю в курсор данные за последний час
        # print('333')
        # print(type(cur))
        for mnp1 in cur:
            mean_point_1.append(mnp1)

        # print('mean_point_1', len(mean_point_1))
        conn.commit()  # сохраняем изменения в БД
        conn.close()
        ####################################################################################################################

        ################################ нахожу среднее значение в первый и последний(12) час ##############################
        sum2 = 0
        count_2 = 0
        for kl in mean_point_2:
            value2 = kl[1]
            sum2 = sum2 + value2
            count_2 += 1
        exh3_Alarm_Vibr2_mean2 = round(sum2 / count_2, 2)  # округляю
        # print(sum2)
        # print(count_2)
        # print('среднее значение за 1 час(M2)', mean2)

        sum1 = 0
        count_1 = 0
        for kb in mean_point_1:
            value1 = kb[1]
            sum1 = sum1 + value1
            count_1 += 1
        exh3_Alarm_Vibr2_mean1 = round(sum1 / count_1, 2)  # округляю
        # print(sum1)
        # print(count_1)
        # print('среднее значение за 12 час(M1)', exh3_Alarm_Vibr2_mean1)

        # *******************************************************************************************
        exh3_Alarm_Vibr2_mean = 7  # уставка, при достижении, которой необходимо узнать дату и время *************
        # *******************************************************************************************

        # print('exh3_Alarm_Vibr2_mean1', exh3_Alarm_Vibr2_mean1)
        # print('exh3_Alarm_Vibr2_mean2', exh3_Alarm_Vibr2_mean2)


        if exh3_Alarm_Vibr2_mean1 > exh3_Alarm_Vibr2_mean2:
            print('снижение вибрации за рассматриваемый диапазон по вибрации Vibr2 т.2')
            exh3_Alarm_Vibr2_predictor = datetime.datetime.strptime(('9999-02-01 01:01:01').strip(),
                                                                    '%Y-%m-%d %H:%M:%S')
            #print('exh3_Alarm_Vibr2_predictor', exh3_Alarm_Vibr2_predictor)
            exh3_Alarm_Vibr2_str_time_to_alarm = '9999-09-09'

        else:
            exh3_Alarm_Vibr2_predictor = ((exh3_Alarm_Vibr2_mean - exh3_Alarm_Vibr2_mean1) * (x2_1 - x1_1)) / (
                        exh3_Alarm_Vibr2_mean2 - exh3_Alarm_Vibr2_mean1) + x1_1
            # print('exh3_Alarm_Vibr2_predictor', exh3_Alarm_Vibr2_predictor)

            # print('дата и время выхода из строя ротора', (str(exh3_Alarm_Vibr2_predictor))[:19])

            exh3_Alarm_Vibr2_time_to_alarm = exh3_Alarm_Vibr2_predictor - x2_1
            exh3_Alarm_Vibr2_str_time_to_alarm = (str(exh3_Alarm_Vibr2_time_to_alarm))[:16]

            print('количество дней до выхода из строя ротора по Vibr2', exh3_Alarm_Vibr2_str_time_to_alarm)
        return exh3_Alarm_Vibr2_predictor, exh3_Alarm_Vibr2_str_time_to_alarm
        conn.commit()  # сохраняем изменения в БД
        conn.close()  # закрываем подключение к базе
    except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
        pass
    finally:
        pass

# определение времени выхода из строя ротора  по температуре т.1
def exh3_Alarm_Temp1():
    ######################## Считываю данные из conn_file_Exh_ для опредения последнего записанного времени

    with open('Exh3/conn_file/conn_file_Exh3_Temp1.txt', 'r', encoding='utf-8') as file:

        txt_lines = file.readlines()

    txt_connection_string = txt_lines[1].strip()  # connection string

    txt_time_ponit = txt_lines[7].strip()  # time_ponit

    ##################################################################################################################

    ############################ считываю данные из БД ###################################################
    try:
        #################################### отрываю БД и считываю таблицу и определяю последнюю(MAX) дату и строки

        conn = pyodbc.connect(
            txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки

        cur = conn.cursor()  # создали курсор, через который можно работать с БД
        cur.fast_executemany = True  # это для ускорения передачи данных

        ####### определяю последнее записанное значение по максимальной дате ################################
        cur.execute(
            "select TagTime, TagValue  from Exh3_Temp1 where TagTime = (select MAX(TagTime) from Exh3_Temp1)")  # записываю в курсор максимальное значение времени из БД

        point_2_1 = []  # создаю пустой список для дальнейшей записи в него последнего времени и значения переменной
        for p in cur:
            point_2_1.append(p)
        for k in point_2_1:
            x2_1 = k[0]
            m2_1 = k[1]

        #print('x2_1', x2_1)
        #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! для ручного изменеия текущей даты. Потом закоментировать!!!!!!!!!!!!!!!!
        # temp777_x2_1 = '2022-08-06 20:19:52'
        # x2_1 = datetime.datetime.strptime((temp777_x2_1).strip(),
        #                                   '%Y-%m-%d %H:%M:%S')

        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

        # *************************************** нахожу дату и время со сдвигом 1 час и 12 часов *******************************

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

        # print('x1_1', x1_1)
        ###################### x1_2 - минус 13 часов    #################################
        delta1_x1_2_norm = datetime.datetime.strptime((delta1_x1_2_str).strip(),
                                                      '%Y-%m-%d %H:%M:%S')  # нормализую для вычислений
        delta2_x1_2_norm = datetime.datetime.strptime((delta2_x1_2_str).strip(),
                                                      '%Y-%m-%d %H:%M:%S')  # нормализую для вычислений

        x1_2 = x1_1 - delta1_x1_2_norm + delta2_x1_2_norm  # вычисляю дату и время сдвинутую на минус 12 часов
        # print('x1_2', x1_2)

        # ******************* преобразую дату и время, иначе не делается запрос из БД по условию********************************
        temp_x2_1 = (str(x2_1))[:10] + 'T' + (str(x2_1))[11:]
        # print('x2_1_convert', temp_x2_1)
        temp_x2_2 = (str(x2_2))[:10] + 'T' + (str(x2_2))[11:]
        temp_x1_1 = (str(x1_1))[:10] + 'T' + (str(x1_1))[11:]
        temp_x1_2 = (str(x1_2))[:10] + 'T' + (str(x1_2))[11:]

        # ********************************** делаю выгрузку данных за текущий час для нахождения среднего значения **********
        exh3_Alarm_Temp1_temp_str2 = 'select TagTime, TagValue  from Exh3_Temp1 where TagTime <= ' + "'" + temp_x2_1 + "'" + ' and ' + 'TagTime >= ' + "'" + temp_x2_2 + "'"
        # print('temp_str2', temp_str2)
        # print('x2_1', x2_1)
        # print('x2_2', x2_2)
        mean_point_2 = []  # создаю пустой список (первый час) для дальнейшей записи в него последнего времени и значения переменной
        cur.execute(exh3_Alarm_Temp1_temp_str2)  # записываю в курсор данные за последний час
        # print('222')
        for mnp2 in cur:
            mean_point_2.append(mnp2)

        #print('mean_point_2', len(mean_point_2))

        # *********************************************************************************************************************

        # ********************************** делаю выгрузку данных за 12 час для нахождения среднего значения **********

        exh3_Alarm_Temp1_temp_str1 = 'select TagTime, TagValue  from Exh3_Temp1 where TagTime <= ' + "'" + temp_x1_1 + "'" + ' and ' + 'TagTime >= ' + "'" + temp_x1_2 + "'"
        #print('temp_str1', exh3_Alarm_Temp1_temp_str1)
        # print('x1_1', x1_1)
        # print('x1_2', x1_2)

        mean_point_1 = []  # создаю пустой список (12 час) для дальнейшей записи в него последнего времени и значения переменной
        cur.execute(exh3_Alarm_Temp1_temp_str1)  # записываю в курсор данные за последний час
        #print('333')
        # print(type(cur))
        for mnp1 in cur:
            #print('mnp1[1]', mnp1[1])
            mean_point_1.append(mnp1)

        # print('mean_point_1', len(mean_point_1))
        conn.commit()  # сохраняем изменения в БД
        conn.close()
        ####################################################################################################################

        ################################ нахожу среднее значение в первый и последний(12) час ##############################
        sum2 = 0
        count_2 = 0
        for kl in mean_point_2:
            value2 = kl[1]
            sum2 = sum2 + value2
            count_2 += 1
        exh3_Alarm_Temp1_mean2 = round(sum2 / count_2, 2)  # округляю
        # print(sum2)
        # print(count_2)
        # print('среднее значение за 1 час(M2)', mean2)

        sum1 = 0
        count_1 = 0
        for kb in mean_point_1:
            value1 = kb[1]
            sum1 = sum1 + value1
            count_1 += 1

        # print('temp1 sum1',sum1)
        # print('temp1 count_1', count_1)

        exh3_Alarm_Temp1_mean1 = round(sum1 / count_1, 2)  # округляю
        # print(sum1)
        # print(count_1)
        # print('среднее значение за 12 час(M1)', exh3_Alarm_Temp1_mean1)

        # *******************************************************************************************
        exh3_Alarm_Temp1_mean = 85  # уставка, при достижении, которой необходимо узнать дату и время *************
        # *******************************************************************************************

        if exh3_Alarm_Temp1_mean1 > exh3_Alarm_Temp1_mean2:
            # print('exh3_Alarm_Temp1_mean1', exh3_Alarm_Temp1_mean1)
            # print('exh3_Alarm_Temp1_mean2', exh3_Alarm_Temp1_mean2)

            print('снижение вибрации за рассматриваемый диапазон по Температуре т.1')
            exh3_Alarm_Temp1_predictor = datetime.datetime.strptime(('9999-02-01 01:01:01').strip(),
                                                                    '%Y-%m-%d %H:%M:%S')
            # print('exh3_Alarm_Vibr2_predictor', exh3_Alarm_Vibr2_predictor)
            exh3_Alarm_Temp1_str_time_to_alarm = '9999-09-09'
        else:
            exh3_Alarm_Temp1_predictor = ((exh3_Alarm_Temp1_mean - exh3_Alarm_Temp1_mean1) * (x2_1 - x1_1)) / (
                    exh3_Alarm_Temp1_mean2 - exh3_Alarm_Temp1_mean1) + x1_1
            # print('exh3_Alarm_Vibr2_predictor', exh3_Alarm_Vibr2_predictor)

            # print('дата и время выхода из строя ротора', (str(exh3_Alarm_Vibr2_predictor))[:19])

            exh3_Alarm_Temp1_time_to_alarm = exh3_Alarm_Temp1_predictor - x2_1
            exh3_Alarm_Temp1_str_time_to_alarm = (str(exh3_Alarm_Temp1_time_to_alarm))[:16]

            print('количество дней до выхода из строя ротора по Temp1', exh3_Alarm_Temp1_str_time_to_alarm)
        return exh3_Alarm_Temp1_predictor, exh3_Alarm_Temp1_str_time_to_alarm
        conn.commit()  # сохраняем изменения в БД
        conn.close()  # закрываем подключение к базе
    except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
        pass
    finally:
        pass


# определение времени выхода из строя ротора  по температуре т.2
def exh3_Alarm_Temp2():
    ######################## Считываю данные из conn_file_Exh_ для опредения последнего записанного времени

    with open('Exh3/conn_file/conn_file_Exh3_Temp2.txt', 'r', encoding='utf-8') as file:

        txt_lines = file.readlines()

    txt_connection_string = txt_lines[1].strip()  # connection string

    txt_time_ponit = txt_lines[7].strip()  # time_ponit

    ##################################################################################################################

    ############################ считываю данные из БД ###################################################
    try:
        #################################### отрываю БД и считываю таблицу и определяю последнюю(MAX) дату и строки

        conn = pyodbc.connect(
            txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки

        cur = conn.cursor()  # создали курсор, через который можно работать с БД
        cur.fast_executemany = True  # это для ускорения передачи данных

        ####### определяю последнее записанное значение по максимальной дате ################################
        cur.execute(
            "select TagTime, TagValue  from Exh3_Temp2 where TagTime = (select MAX(TagTime) from Exh3_Temp2)")  # записываю в курсор максимальное значение времени из БД

        point_2_1 = []  # создаю пустой список для дальнейшей записи в него последнего времени и значения переменной
        for p in cur:
            point_2_1.append(p)
        for k in point_2_1:
            x2_1 = k[0]
            m2_1 = k[1]

        # print('x2_1', x2_1)
        # print('m2_1', m2_1)
        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! для ручного изменеия текущей даты. Потом закоментировать!!!!!!!!!!!!!!!!
        # temp777_x2_1 = '2022-07-27 20:19:52'
        # x2_1 = datetime.datetime.strptime((temp777_x2_1).strip(),
        #                                   '%Y-%m-%d %H:%M:%S')

        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

        # *************************************** нахожу дату и время со сдвигом 1 час и 12 часов *******************************

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

        # print('x1_1', x1_1)
        ###################### x1_2 - минус 13 часов    #################################
        delta1_x1_2_norm = datetime.datetime.strptime((delta1_x1_2_str).strip(),
                                                      '%Y-%m-%d %H:%M:%S')  # нормализую для вычислений
        delta2_x1_2_norm = datetime.datetime.strptime((delta2_x1_2_str).strip(),
                                                      '%Y-%m-%d %H:%M:%S')  # нормализую для вычислений

        x1_2 = x1_1 - delta1_x1_2_norm + delta2_x1_2_norm  # вычисляю дату и время сдвинутую на минус 12 часов
        # print('x1_2', x1_2)

        # ******************* преобразую дату и время, иначе не делается запрос из БД по условию********************************
        temp_x2_1 = (str(x2_1))[:10] + 'T' + (str(x2_1))[11:]
        # print('x2_1_convert', temp_x2_1)
        temp_x2_2 = (str(x2_2))[:10] + 'T' + (str(x2_2))[11:]
        temp_x1_1 = (str(x1_1))[:10] + 'T' + (str(x1_1))[11:]
        temp_x1_2 = (str(x1_2))[:10] + 'T' + (str(x1_2))[11:]

        # ********************************** делаю выгрузку данных за текущий час для нахождения среднего значения **********
        exh3_Alarm_Temp2_temp_str2 = 'select TagTime, TagValue  from Exh3_Temp2 where TagTime <= ' + "'" + temp_x2_1 + "'" + ' and ' + 'TagTime >= ' + "'" + temp_x2_2 + "'"
        # print('temp_str2', exh3_Alarm_Temp2_temp_str2)
        # print('x2_1', x2_1)
        # print('x2_2', x2_2)
        mean_point_2 = []  # создаю пустой список (первый час) для дальнейшей записи в него последнего времени и значения переменной
        cur.execute(exh3_Alarm_Temp2_temp_str2)  # записываю в курсор данные за последний час
        #print('222')
        for mnp2 in cur:
            mean_point_2.append(mnp2)

        #print('mean_point_2', len(mean_point_2))

        # *********************************************************************************************************************

        # ********************************** делаю выгрузку данных за 12 час для нахождения среднего значения **********

        exh3_Alarm_Temp2_temp_str1 = 'select TagTime, TagValue  from Exh3_Temp2 where TagTime <= ' + "'" + temp_x1_1 + "'" + ' and ' + 'TagTime >= ' + "'" + temp_x1_2 + "'"
        #print('temp_str1', exh3_Alarm_Temp2_temp_str1)
        # print('x1_1', x1_1)
        # print('x1_2', x1_2)

        mean_point_1 = []  # создаю пустой список (12 час) для дальнейшей записи в него последнего времени и значения переменной
        cur.execute(exh3_Alarm_Temp2_temp_str1)  # записываю в курсор данные за последний час
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
        count_2_1 = 0
        for kl in mean_point_2:
            value2 = kl[1]
            sum2 = sum2 + value2
            count_2_1 += 1
        # print('среднее значение за 1 час(M2)', sum2)
        # print('среднее значение за 1 час(M2)', len(mean_point_2))
        exh3_Alarm_Temp2_mean2 = round(sum2 / count_2_1, 2)  # округляю
        # print(sum2)
        # print(count_2)
        # print('среднее значение за 1 час(M2)', exh3_Alarm_Temp2_mean2)

        sum1 = 0
        count_1 = 0
        for kb in mean_point_1:
            value1 = kb[1]
            sum1 = sum1 + value1
            count_1 += 1
        exh3_Alarm_Temp2_mean1 = round(sum1 / count_1, 2)  # округляю
        # print(sum1)
        # print(count_1)
        # print('среднее значение за 12 час(M1)', exh3_Alarm_Temp2_mean1)

        # *******************************************************************************************
        exh3_Alarm_Temp2_mean = 85  # уставка, при достижении, которой необходимо узнать дату и время *************
        # *******************************************************************************************

        if exh3_Alarm_Temp2_mean1 > exh3_Alarm_Temp2_mean2:
            print('снижение за рассматриваемый диапазон по Температуре т.2')
            exh3_Alarm_Temp2_predictor = datetime.datetime.strptime(('9999-02-01 01:01:01').strip(),
                                                                    '%Y-%m-%d %H:%M:%S')

            exh3_Alarm_Temp2_str_time_to_alarm = '9999-09-09'
            # print('exh3_Alarm_Vibr2_predictor', exh3_Alarm_Vibr2_predictor)
            #print(56565656565656565655)
        else:
            exh3_Alarm_Temp2_predictor = ((exh3_Alarm_Temp2_mean - exh3_Alarm_Temp2_mean1) * (x2_1 - x1_1)) / (
                    exh3_Alarm_Temp2_mean2 - exh3_Alarm_Temp2_mean1) + x1_1
            # print('exh3_Alarm_Vibr2_predictor', exh3_Alarm_Vibr2_predictor)

            # print('дата и время выхода из строя ротора', (str(exh3_Alarm_Vibr2_predictor))[:19])

            exh3_Alarm_Temp2_time_to_alarm = exh3_Alarm_Temp2_predictor - x2_1
            exh3_Alarm_Temp2_str_time_to_alarm = (str(exh3_Alarm_Temp2_time_to_alarm))[:16]

            print('количество дней до выхода из строя ротора по Temp2', exh3_Alarm_Temp2_str_time_to_alarm)
            #print(555555555555555555555555555555555555555555555555)
        return exh3_Alarm_Temp2_predictor, exh3_Alarm_Temp2_str_time_to_alarm
        conn.commit()  # сохраняем изменения в БД
        conn.close()  # закрываем подключение к базе
    except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
        pass
    finally:
        pass

# определение времени выхода из строя ротора  по температуре т.1
def exh3_Alarm_Temp7():
    ######################## Считываю данные из conn_file_Exh_ для опредения последнего записанного времени

    with open('Exh3/conn_file/conn_file_Exh3_Temp7.txt', 'r', encoding='utf-8') as file:

        txt_lines = file.readlines()

    txt_connection_string = txt_lines[1].strip()  # connection string

    txt_time_ponit = txt_lines[7].strip()  # time_ponit

    ##################################################################################################################

    ############################ считываю данные из БД ###################################################
    try:
        #################################### отрываю БД и считываю таблицу и определяю последнюю(MAX) дату и строки

        conn = pyodbc.connect(
            txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки

        cur = conn.cursor()  # создали курсор, через который можно работать с БД
        cur.fast_executemany = True  # это для ускорения передачи данных

        ####### определяю последнее записанное значение по максимальной дате ################################
        cur.execute(
            "select TagTime, TagValue  from Exh3_Temp7 where TagTime = (select MAX(TagTime) from Exh3_Temp7)")  # записываю в курсор максимальное значение времени из БД

        point_2_1 = []  # создаю пустой список для дальнейшей записи в него последнего времени и значения переменной
        for p in cur:
            point_2_1.append(p)
        for k in point_2_1:
            x2_1 = k[0]
            m2_1 = k[1]

        #print('x2_1', x2_1)
        #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! для ручного изменеия текущей даты. Потом закоментировать!!!!!!!!!!!!!!!!
        # temp777_x2_1 = '2022-08-06 20:19:52'
        # x2_1 = datetime.datetime.strptime((temp777_x2_1).strip(),
        #                                   '%Y-%m-%d %H:%M:%S')

        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

        # *************************************** нахожу дату и время со сдвигом 1 час и 12 часов *******************************

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

        # print('x1_1', x1_1)
        ###################### x1_2 - минус 13 часов    #################################
        delta1_x1_2_norm = datetime.datetime.strptime((delta1_x1_2_str).strip(),
                                                      '%Y-%m-%d %H:%M:%S')  # нормализую для вычислений
        delta2_x1_2_norm = datetime.datetime.strptime((delta2_x1_2_str).strip(),
                                                      '%Y-%m-%d %H:%M:%S')  # нормализую для вычислений

        x1_2 = x1_1 - delta1_x1_2_norm + delta2_x1_2_norm  # вычисляю дату и время сдвинутую на минус 12 часов
        # print('x1_2', x1_2)

        # ******************* преобразую дату и время, иначе не делается запрос из БД по условию********************************
        temp_x2_1 = (str(x2_1))[:10] + 'T' + (str(x2_1))[11:]
        # print('x2_1_convert', temp_x2_1)
        temp_x2_2 = (str(x2_2))[:10] + 'T' + (str(x2_2))[11:]
        temp_x1_1 = (str(x1_1))[:10] + 'T' + (str(x1_1))[11:]
        temp_x1_2 = (str(x1_2))[:10] + 'T' + (str(x1_2))[11:]

        # ********************************** делаю выгрузку данных за текущий час для нахождения среднего значения **********
        exh3_Alarm_Temp7_temp_str2 = 'select TagTime, TagValue  from Exh3_Temp7 where TagTime <= ' + "'" + temp_x2_1 + "'" + ' and ' + 'TagTime >= ' + "'" + temp_x2_2 + "'"
        # print('temp_str2', temp_str2)
        # print('x2_1', x2_1)
        # print('x2_2', x2_2)
        mean_point_2 = []  # создаю пустой список (первый час) для дальнейшей записи в него последнего времени и значения переменной
        cur.execute(exh3_Alarm_Temp7_temp_str2)  # записываю в курсор данные за последний час
        # print('222')
        for mnp2 in cur:
            mean_point_2.append(mnp2)

        #print('mean_point_2', len(mean_point_2))

        # *********************************************************************************************************************

        # ********************************** делаю выгрузку данных за 12 час для нахождения среднего значения **********

        exh3_Alarm_Temp7_temp_str1 = 'select TagTime, TagValue  from Exh3_Temp7 where TagTime <= ' + "'" + temp_x1_1 + "'" + ' and ' + 'TagTime >= ' + "'" + temp_x1_2 + "'"
        #print('temp_str1', exh3_Alarm_Temp1_temp_str1)
        # print('x1_1', x1_1)
        # print('x1_2', x1_2)

        mean_point_1 = []  # создаю пустой список (12 час) для дальнейшей записи в него последнего времени и значения переменной
        cur.execute(exh3_Alarm_Temp7_temp_str1)  # записываю в курсор данные за последний час
        #print('333')
        # print(type(cur))
        for mnp1 in cur:
            #print('mnp1[1]', mnp1[1])
            mean_point_1.append(mnp1)

        # print('mean_point_1', len(mean_point_1))
        conn.commit()  # сохраняем изменения в БД
        conn.close()
        ####################################################################################################################

        ################################ нахожу среднее значение в первый и последний(12) час ##############################
        sum2 = 0
        count_2 = 0
        for kl in mean_point_2:
            value2 = kl[1]
            sum2 = sum2 + value2
            count_2 += 1
        exh3_Alarm_Temp7_mean2 = round(sum2 / count_2, 2)  # округляю
        # print(sum2)
        # print(count_2)
        # print('среднее значение за 1 час(M2)', mean2)

        sum1 = 0
        count_1 = 0
        for kb in mean_point_1:
            value1 = kb[1]
            sum1 = sum1 + value1
            count_1 += 1

        # print('temp1 sum1',sum1)
        # print('temp1 count_1', count_1)

        exh3_Alarm_Temp7_mean1 = round(sum1 / count_1, 2)  # округляю
        # print(sum1)
        # print(count_1)
        # print('среднее значение за 12 час(M1)', exh3_Alarm_Temp1_mean1)

        # *******************************************************************************************
        exh3_Alarm_Temp7_mean = 75  # уставка, при достижении, которой необходимо узнать дату и время *************
        # *******************************************************************************************

        if exh3_Alarm_Temp7_mean1 > exh3_Alarm_Temp7_mean2:
            # print('exh3_Alarm_Temp1_mean1', exh3_Alarm_Temp1_mean1)
            # print('exh3_Alarm_Temp1_mean2', exh3_Alarm_Temp1_mean2)

            print('снижение вибрации за рассматриваемый диапазон по Температуре т.7')
            exh3_Alarm_Temp7_predictor = datetime.datetime.strptime(('9999-02-01 01:01:01').strip(),
                                                                    '%Y-%m-%d %H:%M:%S')

            exh3_Alarm_Temp7_str_time_to_alarm = '9999-09-09'
        else:
            exh3_Alarm_Temp7_predictor = ((exh3_Alarm_Temp7_mean - exh3_Alarm_Temp7_mean1) * (x2_1 - x1_1)) / (
                    exh3_Alarm_Temp7_mean2 - exh3_Alarm_Temp7_mean1) + x1_1
            # print('exh3_Alarm_Vibr2_predictor', exh3_Alarm_Vibr2_predictor)

            # print('дата и время выхода из строя ротора', (str(exh3_Alarm_Vibr2_predictor))[:19])

            exh3_Alarm_Temp7_time_to_alarm = exh3_Alarm_Temp7_predictor - x2_1
            exh3_Alarm_Temp7_str_time_to_alarm = (str(exh3_Alarm_Temp7_time_to_alarm))[:16]

            print('количество дней до выхода из строя ротора по Temp7', exh3_Alarm_Temp7_str_time_to_alarm)
        return exh3_Alarm_Temp7_predictor, exh3_Alarm_Temp7_str_time_to_alarm
        conn.commit()  # сохраняем изменения в БД
        conn.close()  # закрываем подключение к базе
    except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
        pass
    finally:
        pass



#предсказания выхода из строя ротора экскгаустера №3
def predict_Exh3():

    ####################### Считываю данные из conn_file_Exh_ для опредения последнего записанного времени
    try:
        with open('Exh3/conn_file/conn_file_Exh3_Predict.txt', 'r', encoding='utf-8') as file:


            txt_lines = file.readlines()

        txt_connection_string = txt_lines[1].strip()  # connection string

        txt_time_ponit = txt_lines[7].strip()  # time_ponit

        #print('txt_time_ponit', txt_time_ponit)

        # Определение выхода из строя ротора по вибрации т.1


        #print('exh3_Alarm_Vibr1_predictor', exh3_Alarm_Vibr1_predictor)

        # записываю текущие значения предиктора и времени в переменные для дальнейшей обработки
        exh3_Alarm_Vibr1_predictor, exh3_Alarm_Vibr1_str_time_to_alarm = exh3_Alarm_Vibr1()



        if exh3_Alarm_Vibr1_predictor == datetime.datetime.strptime(('9999-02-01 01:01:01').strip(),
                                                                    '%Y-%m-%d %H:%M:%S'):
            print('********Cнижение вибрации за рассматриваемый диапазон по вибрации т.1********')

        else:

            print('Время выхода из строя из-за Vibr1', exh3_Alarm_Vibr1_predictor)

        # Определение выхода из строя ротора по вибрации т.2
        #print('##############################################################################################################')
        # записываю текущие значения предиктора и времени в переменные для дальнейшей обработки
        exh3_Alarm_Vibr2_predictor, exh3_Alarm_Vibr2_str_time_to_alarm = exh3_Alarm_Vibr2()


        if exh3_Alarm_Vibr2_predictor == datetime.datetime.strptime(('9999-02-01 01:01:01').strip(),
                                                                    '%Y-%m-%d %H:%M:%S'):
            print('********Cнижение вибрации за рассматриваемый диапазон по вибрации т.2********')
        else:
            print('Время выхода из строя из-за Vibr2', exh3_Alarm_Vibr2_predictor)

        # Определение выхода из строя ротора по температуре т.1
        # записываю текущие значения предиктора и времени в переменные для дальнейшей обработки
        exh3_Alarm_Temp1_predictor, exh3_Alarm_Temp1_str_time_to_alarm = exh3_Alarm_Temp1()

        if exh3_Alarm_Temp1_predictor == datetime.datetime.strptime(('9999-02-01 01:01:01').strip(),
                                                                    '%Y-%m-%d %H:%M:%S'):
            print('********Cнижение вибрации за рассматриваемый диапазон по температуре т.1********')
        else:

            print('Время выхода из строя из-за Temp1', exh3_Alarm_Temp1_predictor)

        # Определение выхода из строя ротора по температуре т.2
        # записываю текущие значения предиктора и времени в переменные для дальнейшей обработки
        exh3_Alarm_Temp2_predictor, exh3_Alarm_Temp2_str_time_to_alarm = exh3_Alarm_Temp2()

        if exh3_Alarm_Temp2_predictor == datetime.datetime.strptime(('9999-02-01 01:01:01').strip(),
                                                                    '%Y-%m-%d %H:%M:%S'):
            print('********Cнижение вибрации за рассматриваемый диапазон по температуре т.2********')
        else:

            print('Время выхода из строя из-за Temp2', exh3_Alarm_Temp2_predictor)

        #print(77777333333333333333)
        time_min_alarm = min(exh3_Alarm_Vibr1_predictor, exh3_Alarm_Vibr2_predictor, exh3_Alarm_Temp1_predictor,
                             exh3_Alarm_Temp2_predictor)
        #print(77777111111111111111111)
        print('********Минимальное время, когда ротор эксгаустера выйдет из строя', time_min_alarm)


        # print('time_min_alarm', time_min_alarm)
        # print('exh3_Alarm_Vibr1_predictor', exh3_Alarm_Vibr1_predictor)
        # print('exh3_Alarm_Vibr2_predictor', exh3_Alarm_Vibr2_predictor)
        # print('exh3_Alarm_Temp1_predictor', exh3_Alarm_Temp1_predictor)
        # print('exh3_Alarm_Temp2_predictor', exh3_Alarm_Temp2_predictor)




        if time_min_alarm == exh3_Alarm_Vibr1_predictor:
            time_count_alarm = exh3_Alarm_Vibr1_str_time_to_alarm
            name_alarm = 'Причина выхода из строя ротора - повышенная вибрация т.1'
            print(name_alarm)
            print('Количество дней и часов до аварии(ремонта)', time_count_alarm)



        elif time_min_alarm == exh3_Alarm_Vibr2_predictor:
            #print(3333333333333333333333)
            time_count_alarm = exh3_Alarm_Vibr2_str_time_to_alarm
            #print(4444444444444444444444)
            name_alarm = 'Причина выхода из строя ротора - повышенная вибрация т.2'
            print(name_alarm)
            print('Количество дней и часов до аварии(ремонта)', time_count_alarm)

        elif time_min_alarm == exh3_Alarm_Temp1_predictor:
            time_count_alarm = exh3_Alarm_Temp1_str_time_to_alarm
            name_alarm = 'Причина выхода из строя ротора - повышенная температура т.1'
            print(name_alarm)
            print('Количество дней и часов до аварии(ремонта)', time_count_alarm)

        elif time_min_alarm == exh3_Alarm_Temp2_predictor:
            time_count_alarm = exh3_Alarm_Temp2_str_time_to_alarm
            name_alarm = 'Причина выхода из строя ротора - повышенная температура т.2'
            print(name_alarm)
            print('Количество дней и часов до аварии(ремонта)', time_count_alarm)

        end = time.time()


        # import datetime

        date_time_now = datetime.datetime.now()
        # print(date_time_now)

        # конвертирую строку с датой из connection_file.txt в тип datetime
        txt_time_ponit_norm = datetime.datetime.strptime((txt_time_ponit[0:19]).strip(), '%Y-%m-%d %H:%M:%S')
        date_time_now_norm = datetime.datetime.strptime(((str(date_time_now))[0:19]).strip(), '%Y-%m-%d %H:%M:%S')
        # print('txt_time_ponit_norm', txt_time_ponit_norm)
        # print('date_time_now_norm', date_time_now_norm)

        if txt_time_ponit_norm < date_time_now_norm:  # делаю именно такое сравнение, чтобы не перегружать БД и записывать только изменения

            # print(1111111111111111111)

            # определяю текущую дату и время для записи в БД

            date_time_now = datetime.datetime.now()


            # print(date_time_now)

            ####################################### сформирую список для предачи данных в БД ######################################
            data_alarm = [
                [date_time_now, exh3_Alarm_Vibr1_predictor, exh3_Alarm_Vibr2_predictor, exh3_Alarm_Temp1_predictor,
                 exh3_Alarm_Temp2_predictor, time_min_alarm, name_alarm, time_count_alarm]]
            # print(data_alarm)


        ############################################# запишу данные в БД ###################################################
    except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
        pass
    finally:
        pass





        try:
            #################################### отрываю БД и считываю таблицу и определяю последнюю(MAX) дату и строки

            conn = pyodbc.connect(
                txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки

            cur = conn.cursor()  # создали курсор, через который можно работать с БД
            cur.fast_executemany = True  # это для ускорения передачи данных

            ####### определяю последнее записанное значение по максимальной дате ################################
            # cur.execute("select TagTime, TagValue  from Exh3_Vibr1 where TagTime = (select MAX(TagTime) from Exh3_Vibr1)")  # записываю в курсор максимальное значение времени из БД
            ######## запись в БД выбранных значений###################

            sql = "INSERT INTO Exh3_Alarm (TagTime, Vibr1_Alarm, Vibr2_Alarm, Temp1_Alarm, Temp2_Alarm, Time_Alarm, Name_Alarm, Count_days_to_Alarm) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

            # print(11)
            cur.executemany(sql,
                            data_alarm)  ############################################################################ДЛЯ ПЕРЕДАЧИ ДАННЫХ РАСКОММЕНТИРОВАТЬ!!!!!!!!!!!!!!!!!!
            ##########################################################################################
            # print(22)
            conn.commit()  # сохраняем изменения в БД

            conn = pyodbc.connect(
                txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки
            # print(33)
            cur = conn.cursor()  # создали курсор, через который можно работать с БД
            cur.fast_executemany = True  # это для ускорения передачи данных, но в этой версии Питона не хочет работать
            # print(44)

            ####### определяю последнее записанное значение по максимальной дате ################################
            cur.execute(
                "select TagTime  from Exh3_Alarm where TagTime = (select MAX(TagTime) from Exh3_Alarm)")  # записываю в курсор максимальное значение времени из БД
            # print(1)
            db_list = []  # создаю пустой список для дальнейшей записи в него строки с максимальным времением.
            for line in cur:
                # print(55)
                db_list.append(line)
            # проверяю наличие строчки в БД, которую отправляли по времени.
            # print('db_list[0][0]', db_list[0][0])
            # print(data_kafka[0][0])

            if (db_list[0][0] != txt_time_ponit_norm):
                # print(77)

                # считываю connection_file.txt для создания списка lines, который потом редактирую и делаю перезапись
                # файла connection_file.txt
                with open('Exh3/conn_file/conn_file_Exh3_Predict.txt', 'r', encoding='utf-8') as fr:
                    # print(88)
                    lines = fr.readlines()

                # запись в connection_file.txt
                # print('lines[7]', lines[7])
                lines[7] = str(db_list[0][0]) + str(
                    '\n')  # добавил \n при записи в список пустая строка под записью обрезается
                # print('lines[7]', lines[7])
                # print('lines[10]', lines[10])
                # lines[10] = str(data_kafka[0][3]) + str('\n')
                # print('lines[10]', lines[10])
                # lines[13] = str(data_kafka[0][4]) + str('\n')'lines[7]',
                # записываю в данные в connection_file.txt
                # print(lines)
                with open('Exh3/conn_file/conn_file_Exh3_Predict.txt', 'w', encoding='utf-8') as fl:
                    # print(88)
                    fl.writelines(lines)

            conn.commit()  # сохраняем изменения в БД
            conn.close()  # закрываем подключение к ба
        except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
            pass
        finally:
            pass

    end = time.time()
    print("Время цикла выполнения программы прогноза выхода из строя ротора :", (end - start) * 10 ** 3, "ms")


#############################обработчик третьего потока ##################################################################

def hendlerDB3(): # функция обработчик третьего потока данных

    #data_csv_Exh3_Vibr1()
    data_csv_Exh3_Temp7()
    #data_csv_Exh3_Alarm()


# передача данных экгаустера №3 вибрация т1 в csv файл
def data_csv_Exh3_Vibr1():
    ######################## Считываю данные из csv_conn_file_Exh_ для опредения последнего записанного времени

    with open('Exh3/conn_file/csv_conn_file_Exh3_Vibr1.txt', 'r', encoding='utf-8') as file:
        txt_lines = file.readlines()

    txt_connection_string = txt_lines[1].strip()  # connection string
    # txt_logfile = txt_lines[4].strip()  # logfile
    txt_time_ponit = txt_lines[7].strip()  # time_ponit
    txt_value = txt_lines[10].strip()  # command
    # txt_status_command = txt_lines[13].strip()  # status_command

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

    # **********************************************************************************************************************
    # **********************************************После подключения Кафка раскоментировать***********************************
    # **********************************************************************************************************************
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

    # **********************************************************************************************************************
    # **********************************************************************************************************************
    # **********************************************************************************************************************
    #####################################################################################################################

    # ********************************************************************************************************************
    # *******************************временно читаю из файла, как будто из кафки, чтобы проверить алгоритм*************
    # ********************************************************************************************************************
    ###########################  читаю csv файл ##############################################################

    # указываю путь, где находится логфайл для чтения
    # path = r'C:\Users\User\Desktop\Хакатон\Новая папка (2)\Новая папка (2)\Exh3_Vibr_2.csv.csv'
    # создаю пустой список, в который перенесу все данные из Logfile
    csv_list = []
    with open('Exh3/csv_file/Exh3_Vibr_1.csv', 'r', newline='', encoding='ANSI') as f:
        reader = csv.reader(f, quoting=csv.QUOTE_NONE)  # delimiter=' ' - ставит запятую вместо пробела
        for nr in reader:  #
            if len(nr) == 1:
                stroka1 = nr[0]
                # print(1)
            if len(nr) == 2:
                stroka2_temp = nr[0]
                stroka2_2 = stroka2_temp[0:19]
                stroka2 = (stroka2_2).strip()
                stroka3 = stroka2_temp[24:25]
                stroka4_temp = nr[1]
                stroka4 = stroka4_temp[:2]
                stroka5 = [stroka2, stroka3 + '.' + stroka4]
                csv_list.append(stroka5)

    # ****************************************************
    # ********************************************************************************************************************
    ########### перебираю и записываю в список для отправки в БД строчки реверсного файла до тех пор,
    # пока она не будет строчка из реверсного файла не будет равно строчке в connection_file
    # конвертирую строку с датой из connection_file.txt в тип datetime
    txt_time_ponit_norm = txt_time_ponit[0:19].strip()  # преобразовываю
    # print('txt_time_ponit_norm', txt_time_ponit_norm)

    select_spisok = []

    for row in csv_list:
        # print('row[0]', row[0])
        # print('txt_time_ponit_norm', txt_time_ponit_norm)

        if row[0] == txt_time_ponit_norm:  # если строчка для отправки = строчки из файла txt, то записываю эту строчку
            # print('break')
            select_spisok.append(row)
            break

        elif row[0] >= txt_time_ponit_norm:

            if row[0] > txt_time_ponit_norm:
                # print(len(select_spisok))
                select_spisok.append(row)

    # print('select_spisok', select_spisok)
    # print(select_spisok)
    data_to_db = list(reversed(select_spisok))  # список для отправки в БД(csv-файл)

    # временно присваиваю для проверки, после доступа к Кафка, это и код по считыванию UserLog необходимо закоментировать или удалить

    data_kafka = data_to_db

    # ********************************************************************************************************************

    ################################ отправляю данные из сформированного списка в БД ####################################
    if len(data_kafka) > 1:
        # print('len(data_kafka)', len(data_kafka))
        # print('len(data_kafka[0])', len(data_kafka[0]))

        # data_kafka.pop(0)  # удаляю последнию строчку т.к. она уже есть в БД и её не надо повторно отравлять в БД

        try:

            with open("Exh3/csv_file/csv_Exh3_Vibr1.csv", mode="a", encoding='utf-8') as w_file:

                for j in data_kafka:
                    str_j = j
                    list_j = [str_j]
                    file_writer = csv.writer(w_file, delimiter=",", lineterminator="\r")
                    file_writer.writerow(list_j)

            # print(data_kafka[-1])

            past_str = data_kafka[-1]
            #print(past_str[0])
            #print(past_str[1])

            with open('Exh3/conn_file/csv_conn_file_Exh3_Vibr1.txt', 'r', encoding='utf-8') as fr:
                lines = fr.readlines()

            # запись в connection_file.txt
            #print('lines[7]', lines[7])
            lines[7] = str(past_str[0]) + str(
                '\n')  # добавил \n при записи в список пустая строка под записью обрезается
            #print('lines[7]', lines[7])
            #print('lines[10]', lines[10])
            lines[10] = str(past_str[1]) + str('\n')
            #print('lines[10]', lines[10])
            # lines[13] = str(data_kafka[0][4]) + str('\n')'lines[7]',
            # записываю в данные в connection_file.txt
            # print(lines)
            with open('Exh3/conn_file/csv_conn_file_Exh3_Vibr1.txt', 'w', encoding='utf-8') as fl:
                # print(88)
                fl.writelines(lines)


        except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
            pass
        finally:
            pass


    #print('Привет, я третий поток')

# передача данных экгаустера №3 вибрация т7 в csv файл
def data_csv_Exh3_Temp7():
    ######################## Считываю данные из csv_conn_file_Exh_ для опредения последнего записанного времени

    with open('Exh3/conn_file/csv_conn_file_Exh3_Temp7.txt', 'r', encoding='utf-8') as file:
        txt_lines = file.readlines()

    txt_connection_string = txt_lines[1].strip()  # connection string
    # txt_logfile = txt_lines[4].strip()  # logfile
    txt_time_ponit = txt_lines[7].strip()  # time_ponit
    txt_value = txt_lines[10].strip()  # command
    # txt_status_command = txt_lines[13].strip()  # status_command

    ##################################################################################################################

    ##################################### считываю из Kafka###############################################
    #print(88888888888888888888888888888888811111111111111111111111111111111111)
    list_Exh3_Temp7, list_Exh3_Temp8, list_Exh3_Vibr7, list_Exh3_Vibr8 = hendlerDB4()
    #print(88888888888888888888888888888888822222222222222222222222222222222222)


    #########################################################################################################

    csv_list = list_Exh3_Temp7

    #print('csv_list88888888888888888222222222222222222', csv_list)
    # with open('Exh3/csv_file/Exh3_Temp7.csv', 'r', newline='', encoding='ANSI') as f:
    #     reader = csv.reader(f, quoting=csv.QUOTE_NONE)  # delimiter=' ' - ставит запятую вместо пробела
    #     for nr in reader:  #
    #         if len(nr) == 1:
    #             stroka1 = nr[0]
    #             # print(1)
    #         if len(nr) == 2:
    #             stroka2_temp = nr[0]
    #             stroka2_2 = stroka2_temp[0:19]
    #             stroka2 = (stroka2_2).strip()
    #             stroka3 = stroka2_temp[24:25]
    #             stroka4_temp = nr[1]
    #             stroka4 = stroka4_temp[:2]
    #             stroka5 = [stroka2, stroka3 + '.' + stroka4]
    #             csv_list.append(stroka5)

    # ****************************************************
    # ********************************************************************************************************************
    ########### перебираю и записываю в список для отправки в БД строчки реверсного файла до тех пор,
    # пока она не будет строчка из реверсного файла не будет равно строчке в connection_file
    # конвертирую строку с датой из connection_file.txt в тип datetime
    txt_time_ponit_norm = txt_time_ponit[0:19].strip()  # преобразовываю

    # print('txt_time_ponit_norm', txt_time_ponit_norm)
    txt_time_ponit_norm_date = datetime.datetime.strptime((txt_time_ponit_norm).strip(), '%Y-%m-%d %H:%M:%S')
    select_spisok = []

    for row in csv_list:
        # print(1111111111111111111111111111111111111111111111111111111111111111)
        # print('row[0]', row[0])
        # print('txt_time_ponit_norm', txt_time_ponit_norm)
        # print(' тип row[0]', type(row[0]))
        # print('txt_time_ponit_norm_date', type(txt_time_ponit_norm_date))


        if row[0] == txt_time_ponit_norm_date:  # если строчка для отправки = строчки из файла txt, то записываю эту строчку
            #print('break')
            select_spisok.append(row)
            break

        elif row[0] >= txt_time_ponit_norm_date:
            #print(55555555555555555555555555555555555555555555555555555555555555555555)

            if row[0] > txt_time_ponit_norm_date:
                #print(len(select_spisok))
                select_spisok.append(row)

    # print('select_spisok', select_spisok)
    # print(select_spisok)
    data_to_db = list(reversed(select_spisok))  # список для отправки в БД(csv-файл)

    # временно присваиваю для проверки, после доступа к Кафка, это и код по считыванию UserLog необходимо закоментировать или удалить

    data_kafka = data_to_db

    # ********************************************************************************************************************

    ################################ отправляю данные из сформированного списка в БД ####################################
    if len(data_kafka) > 1:
        # print('len(data_kafka)', len(data_kafka))
        # print('len(data_kafka[0])', len(data_kafka[0]))

        # data_kafka.pop(0)  # удаляю последнию строчку т.к. она уже есть в БД и её не надо повторно отравлять в БД
        #print(77777777777777777777777777777777777777777777777777777777)
        try:
            ################ запись в csv файл ###############################################
            with open("Exh3/csv_file/csv_Exh3_Temp7.csv", mode="a", encoding='utf-8') as w_file:
                #print(5656565656565656565656565656565665666565656565656665)

                for j in data_kafka:
                    #print(787878787878787887878787878787878787888878787878788)
                    str_j = j

                    str_1 = str(str_j[0])
                    str_2 = str_j[1]
                    # print('str_j++++++++++++++++++++++++++++++++++++++++++++++++++++++', str_j)
                    # print('str_1++++++++++++++++++++++++++++++++++++++++++++++++++++++', str_1)
                    # print('str_2++++++++++++++++++++++++++++++++++++++++++++++++++++++', str_2)


                    list_j = [[str_1, str_2]]

                    file_writer = csv.writer(w_file, delimiter=",", lineterminator="\r")
                    file_writer.writerow(list_j)

                    # print('запись333333333333333333333333333333333333333333333333333333333333333333333333333')
                    # print('list_j', list_j)

            #print(data_kafka[-1])

            past_str = data_kafka[-1]
            #print(past_str[0])
            #print(past_str[1])

            with open('Exh3/conn_file/csv_conn_file_Exh3_Temp7.txt', 'r', encoding='utf-8') as fr:
                lines = fr.readlines()

            # запись в connection_file.txt
            #print('lines[7]', lines[7])
            lines[7] = str(past_str[0]) + str(
                '\n')  # добавил \n при записи в список пустая строка под записью обрезается
            #print('lines[7]', lines[7])
            #print('lines[10]', lines[10])
            lines[10] = str(past_str[1]) + str('\n')
            #print('lines[10]', lines[10])
            # lines[13] = str(data_kafka[0][4]) + str('\n')'lines[7]',
            # записываю в данные в connection_file.txt
            # print(lines)
            with open('Exh3/conn_file/csv_conn_file_Exh3_Temp7.txt', 'w', encoding='utf-8') as fl:
                # print(88)
                fl.writelines(lines)



            ####################################### запись в БД ###############################################
                ######################## Считываю данные из conn_file_Exh_ для опредения последнего записанного времени

            with open('Exh3/conn_file/csv_conn_file_Exh3_Temp7.txt', 'r', encoding='utf-8') as file:
                txt_lines = file.readlines()

            txt_connection_string = txt_lines[1].strip()  # connection string

            txt_time_ponit = txt_lines[7].strip()  # time_ponit

            ##################################################################################################################

            ############################ считываю данные из БД ###################################################
            try:
                #################################### отрываю БД и считываю таблицу и определяю последнюю(MAX) дату и строки

                conn = pyodbc.connect(
                    txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки

                cur = conn.cursor()  # создали курсор, через который можно работать с БД
                cur.fast_executemany = True  # это для ускорения передачи данных
                #print(' sql111111111111111111111111111===========================')
                sql = "INSERT INTO Exh3_Temp7 (TagTime, TagValue) VALUES (?, ?)"
                #print(' sql211111111111111111111111111===========================')
                # print(11)

                cur.executemany(sql, data_kafka)  ############################################################################ДЛЯ ПЕРЕДАЧИ ДАННЫХ РАСКОММЕНТИРОВАТЬ!!!!!!!!!!!!!!!!!!
                print(' записалось в БД ===================================================Temp7=')
                ##########################################################################################
                # print(22)
                conn.commit()  # сохраняем изменения в БД
                conn.close()


            ########################################################################################################
            except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
                pass
            finally:
                pass




        except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
            pass
        finally:
            pass



    #print('Привет, я третий поток')

# передача данных экгаустера №3 вибрация т7 в csv файл
def data_csv_Exh3_Temp8():
    ######################## Считываю данные из csv_conn_file_Exh_ для опредения последнего записанного времени

    with open('Exh3/conn_file/csv_conn_file_Exh3_Temp8.txt', 'r', encoding='utf-8') as file:
        txt_lines = file.readlines()

    txt_connection_string = txt_lines[1].strip()  # connection string
    # txt_logfile = txt_lines[4].strip()  # logfile
    txt_time_ponit = txt_lines[7].strip()  # time_ponit
    txt_value = txt_lines[10].strip()  # command
    # txt_status_command = txt_lines[13].strip()  # status_command

    ##################################################################################################################

    ##################################### считываю из Kafka###############################################
    print(88888888888888888888888888888888811111111111111111111111111111111111)
    list_Exh3_Temp7, list_Exh3_Temp8, list_Exh3_Vibr7, list_Exh3_Vibr8 = hendlerDB4()
    print(88888888888888888888888888888888822222222222222222222222222222222222)


    #########################################################################################################

    csv_list = list_Exh3_Temp8

    print('csv_list88888888888888888222222222222222222', csv_list)
    # with open('Exh3/csv_file/Exh3_Temp7.csv', 'r', newline='', encoding='ANSI') as f:
    #     reader = csv.reader(f, quoting=csv.QUOTE_NONE)  # delimiter=' ' - ставит запятую вместо пробела
    #     for nr in reader:  #
    #         if len(nr) == 1:
    #             stroka1 = nr[0]
    #             # print(1)
    #         if len(nr) == 2:
    #             stroka2_temp = nr[0]
    #             stroka2_2 = stroka2_temp[0:19]
    #             stroka2 = (stroka2_2).strip()
    #             stroka3 = stroka2_temp[24:25]
    #             stroka4_temp = nr[1]
    #             stroka4 = stroka4_temp[:2]
    #             stroka5 = [stroka2, stroka3 + '.' + stroka4]
    #             csv_list.append(stroka5)

    # ****************************************************
    # ********************************************************************************************************************
    ########### перебираю и записываю в список для отправки в БД строчки реверсного файла до тех пор,
    # пока она не будет строчка из реверсного файла не будет равно строчке в connection_file
    # конвертирую строку с датой из connection_file.txt в тип datetime
    txt_time_ponit_norm = txt_time_ponit[0:19].strip()  # преобразовываю

    # print('txt_time_ponit_norm', txt_time_ponit_norm)
    txt_time_ponit_norm_date = datetime.datetime.strptime((txt_time_ponit_norm).strip(), '%Y-%m-%d %H:%M:%S')
    select_spisok = []

    for row in csv_list:
        print(1111111111111111111111111111111111111111111111111111111111111111)
        print('row[0]', row[0])
        print('txt_time_ponit_norm', txt_time_ponit_norm)
        print(' тип row[0]', type(row[0]))
        print('txt_time_ponit_norm_date', type(txt_time_ponit_norm_date))


        if row[0] == txt_time_ponit_norm_date:  # если строчка для отправки = строчки из файла txt, то записываю эту строчку
            print('break')
            select_spisok.append(row)
            break

        elif row[0] >= txt_time_ponit_norm_date:
            print(55555555555555555555555555555555555555555555555555555555555555555555)

            if row[0] > txt_time_ponit_norm_date:
                print(len(select_spisok))
                select_spisok.append(row)

    # print('select_spisok', select_spisok)
    # print(select_spisok)
    data_to_db = list(reversed(select_spisok))  # список для отправки в БД(csv-файл)

    # временно присваиваю для проверки, после доступа к Кафка, это и код по считыванию UserLog необходимо закоментировать или удалить

    data_kafka = data_to_db

    # ********************************************************************************************************************

    ################################ отправляю данные из сформированного списка в БД ####################################
    if len(data_kafka) > 1:
        # print('len(data_kafka)', len(data_kafka))
        # print('len(data_kafka[0])', len(data_kafka[0]))

        # data_kafka.pop(0)  # удаляю последнию строчку т.к. она уже есть в БД и её не надо повторно отравлять в БД
        print(77777777777777777777777777777777777777777777777777777777)
        try:
            ################ запись в csv файл ###############################################
            with open("Exh3/csv_file/csv_Exh3_Temp8.csv", mode="a", encoding='utf-8') as w_file:
                print(5656565656565656565656565656565665666565656565656665)

                for j in data_kafka:
                    print(787878787878787887878787878787878787888878787878788)
                    str_j = j

                    str_1 = str(str_j[0])
                    str_2 = str_j[1]
                    print('str_j++++++++++++++++++++++++++++++++++++++++++++++++++++++', str_j)
                    print('str_1++++++++++++++++++++++++++++++++++++++++++++++++++++++', str_1)
                    print('str_2++++++++++++++++++++++++++++++++++++++++++++++++++++++', str_2)


                    list_j = [[str_1, str_2]]

                    file_writer = csv.writer(w_file, delimiter=",", lineterminator="\r")
                    file_writer.writerow(list_j)

                    print('запись333333333333333333333333333333333333333333333333333333333333333333333333333')
                    print('list_j', list_j)

            #print(data_kafka[-1])

            past_str = data_kafka[-1]
            #print(past_str[0])
            #print(past_str[1])

            with open('Exh3/conn_file/csv_conn_file_Exh3_Temp8.txt', 'r', encoding='utf-8') as fr:
                lines = fr.readlines()

            # запись в connection_file.txt
            #print('lines[7]', lines[7])
            lines[7] = str(past_str[0]) + str(
                '\n')  # добавил \n при записи в список пустая строка под записью обрезается
            #print('lines[7]', lines[7])
            #print('lines[10]', lines[10])
            lines[10] = str(past_str[1]) + str('\n')
            #print('lines[10]', lines[10])
            # lines[13] = str(data_kafka[0][4]) + str('\n')'lines[7]',
            # записываю в данные в connection_file.txt
            # print(lines)
            with open('Exh3/conn_file/csv_conn_file_Exh3_Temp8.txt', 'w', encoding='utf-8') as fl:
                # print(88)
                fl.writelines(lines)



            ####################################### запись в БД ###############################################
                ######################## Считываю данные из conn_file_Exh_ для опредения последнего записанного времени

            with open('Exh3/conn_file/csv_conn_file_Exh3_Temp8.txt', 'r', encoding='utf-8') as file:
                txt_lines = file.readlines()

            txt_connection_string = txt_lines[1].strip()  # connection string

            txt_time_ponit = txt_lines[7].strip()  # time_ponit

            ##################################################################################################################

            ############################ считываю данные из БД ###################################################
            try:
                #################################### отрываю БД и считываю таблицу и определяю последнюю(MAX) дату и строки

                conn = pyodbc.connect(
                    txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки

                cur = conn.cursor()  # создали курсор, через который можно работать с БД
                cur.fast_executemany = True  # это для ускорения передачи данных
                print(' sql111111111111111111111111111===========================')
                sql = "INSERT INTO Exh3_Temp8 (TagTime, TagValue) VALUES (?, ?)"
                print(' sql211111111111111111111111111===========================')
                # print(11)

                cur.executemany(sql, data_kafka)  ############################################################################ДЛЯ ПЕРЕДАЧИ ДАННЫХ РАСКОММЕНТИРОВАТЬ!!!!!!!!!!!!!!!!!!
                print(' записалось в БД ======================================')
                ##########################################################################################
                # print(22)
                conn.commit()  # сохраняем изменения в БД
                conn.close()


            ########################################################################################################
            except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
                pass
            finally:
                pass




        except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
            pass
        finally:
            pass



    #print('Привет, я третий поток')






# передача данных предсказаний выхода из строя ротора экгаустера №3  в csv файл
def data_csv_Exh3_Alarm():
    try:
        ######################## Считываю данные из csv_conn_file_Exh_ для опредения последнего записанного времени

        with open('Exh3/conn_file/csv_conn_file_Exh3_Predict.txt', 'r', encoding='utf-8') as file:
            txt_lines = file.readlines()

        txt_connection_string = txt_lines[1].strip()  # connection string
        # txt_logfile = txt_lines[4].strip()  # logfile
        txt_time_ponit = txt_lines[7].strip()  # time_ponit
        txt_value = txt_lines[10].strip()  # command
        # txt_status_command = txt_lines[13].strip()  # status_command

        ##################################################################################################################

        ################### читаю данные из БД для передачи в csv файл ##################################################
        #################################### отрываю БД и считываю таблицу и определяю последнюю(MAX) дату и строки

        conn = pyodbc.connect(
            txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки

        cur = conn.cursor()  # создали курсор, через который можно работать с БД
        cur.fast_executemany = True  # это для ускорения передачи данных

        ####### определяю последнее записанное значение по максимальной дате ################################
        cur.execute("select TagTime from Exh3_Alarm where TagTime = (select MAX(TagTime) from Exh3_Alarm)")  # записываю в курсор максимальное значение времени из БД

        alarm_list = []  # создаю пустой список для дальнейшей записи в него последнего времени и значения переменной
        for plm in cur:
            str_plm = (str(plm[0]))[0:19]
            alarm_list.append(str_plm)
        conn.commit()
        conn.close()
        alarm_list_norm = datetime.datetime.strptime((alarm_list[0]).strip(), '%Y-%m-%d %H:%M:%S')

        txt_time_ponit_norm = datetime.datetime.strptime((txt_time_ponit[0:19]).strip(), '%Y-%m-%d %H:%M:%S')
        # print('max_alarm_list+++++++++++++++++++++++++++++++++++++++++++++++++++++++', alarm_list[0])

        alarm_list_t = (str(alarm_list[0]))[:10] + 'T' + (str(alarm_list[0]))[11:]
        txt_time_ponit_t = (str(txt_time_ponit[0:19]))[:10] + 'T' + (str(txt_time_ponit[0:19]))[11:]

        # ********************************** делаю выгрузку отсутствующих данных **********
        conn = pyodbc.connect(txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки

        cur = conn.cursor()  # создали курсор, через который можно работать с БД

        cur.fast_executemany = True  # это для ускорения передачи данных

        exh3_Alarm_str1 = 'select TagTime, Vibr1_Alarm, Vibr2_Alarm, Temp1_Alarm, Temp2_Alarm, Time_Alarm, Name_Alarm, Count_days_to_Alarm  from Exh3_Alarm where TagTime <= ' + "'" + alarm_list_t + "'" + ' and ' + 'TagTime >= ' + "'" + txt_time_ponit_t + "'"
        # print('exh3_Alarm_str1', exh3_Alarm_str1)

          # создаю пустой список (12 час) для дальнейшей записи в него последнего времени и значения переменной

        cur.execute(exh3_Alarm_str1)  # записываю в курсор данные
        data_alrmw = []

        for mnpl in cur:
            ft=mnpl
            str_fin = [str(ft[0])[0: 19]+", "+str(ft[1])[0: 19]+", "+str(ft[2])[0: 19]+", "+str(ft[3])[0: 19]+", "+str(ft[4])[0: 19]+", "+str(ft[5])[0: 19]+", "+ft[6]+", "+str(ft[7])[0: 19]]

            data_alrmw.append(str_fin)
        # print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!',len(data_alrmw))
        conn.commit()
        conn.close()
        # print('str@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@', (str(data_alrmw[-1]))[2:21])
        # print('***************************************', data_alrmw)
        data_alrmw_norm_x = (str(data_alrmw[-1]))[2:21]
        data_alrmw_norm = datetime.datetime.strptime((data_alrmw_norm_x).strip(), '%Y-%m-%d %H:%M:%S')
        # alarm_list_norm
        # print('data_alrmw_norm', data_alrmw_norm)
        #
        # print(' txt_time_ponit_norm', txt_time_ponit_norm)
        # print('alarm_list_norm', alarm_list_norm)
        # print('!!!!!!!!!!!!!!!!!!!!!!!!!!!**********************!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!', data_alrmw)
        if txt_time_ponit_norm < alarm_list_norm:

            with open("Exh3/csv_file/csv_Exh3_Alarm.csv", mode="a", encoding='Utf-8') as w_file:
                #print(5555555555555555555555555555555555)
                for j in data_alrmw:
                    str_j = j
                    list_j = [str_j]
                    file_writer = csv.writer(w_file, delimiter=",", lineterminator="\r")
                    file_writer.writerow(list_j)

            #print(data_alrmw[-1])
            #past_str = data_alrmw[-1]


    ######################### обновляю данные в txt файле ###########################################################

            with open('Exh3/conn_file/csv_conn_file_Exh3_Predict.txt', 'r', encoding='utf-8') as fr:
                lines = fr.readlines()

            # запись в connection_file.txt
            #print('lines[7]', lines[7])
            lines[7] = str(alarm_list_norm) + str('\n')  # добавил \n при записи в список пустая строка под записью обрезается
            #print('lines[7]', lines[7])
            #print('lines[10]', lines[10])
            #lines[10] = str(past_str[1]) + str('\n')
            #print('lines[10]', lines[10])
            # lines[13] = str(data_kafka[0][4]) + str('\n')'lines[7]',
            # записываю в данные в connection_file.txt
            # print(lines)
            with open('Exh3/conn_file/csv_conn_file_Exh3_Predict.txt', 'w', encoding='utf-8') as fl:
                # print(88)
                fl.writelines(lines)


    except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
        pass
    finally:
        pass


#print('Привет, я третий поток')



#############################обработчик четвертого потока ##################################################################

#list_Exh3_Temp7 = [] # SM_Exgauster\\[0:33] - эксгаустер №3 температура т.2

def hendlerDB4(): # функция обработчик четвертого потока данных
    list_Exh3_Temp7 = []  # SM_Exgauster\\[0:33] - эксгаустер №3 температура т.7
    list_Exh3_Temp8 = []  # SM_Exgauster\\[0:37] - эксгаустер №3 температура т.8
    list_Exh3_Vibr7 = []  # SM_Exgauster\\[0:7] - эксгаустер №3 вибрация т.1
    list_Exh3_Vibr8 = []  # SM_Exgauster\\[0:10] - эксгаустер №3 вибрация т.2
# rc1a-b5e65f36lm3an1d5.mdb.yandexcloud.net:9091

    try:

        cfg = {
            'bootstrap.servers': 'rc1a-2ar1hqnl386tvq7k.mdb.yandexcloud.net:9091',
            'group.id': 'ExhausHub',
            'auto.offset.reset': 'earliest',
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': 'CA.pem',
            'sasl.username': '9433_reader',
            'sasl.password': 'eUIpgWu0PWTJaTrjhjQD3.hoyhntiK',
            'sasl.mechanisms': 'SCRAM-SHA-512'
        }

        C = Consumer(cfg)
        C.subscribe(['zsmk-9433-dev-01'])

        for _ in range(100):
            msg = C.poll(1)

            if msg:
                dat = {
                    'msg_value': msg.value(),
                    # 'msg_headers': msg.headers(),
                    # 'msg_key': msg.key(),
                    # 'msg_partition': msg.partition(),
                    # 'msg_topic': msg.topic(),
                }

                rtn = json.loads(dat['msg_value'])
                sp_Exh3_Temp7 = [dateutil.parser.isoparse((rtn['moment'])[0:19]), (str(rtn['SM_Exgauster\\[0:33]']))[0:4]]
                sp_Exh3_Temp8 = [dateutil.parser.isoparse((rtn['moment'])[0:19]), (str(rtn['SM_Exgauster\\[0:34]']))[0:4]]
                sp_Exh3_Vibr7 = [dateutil.parser.isoparse((rtn['moment'])[0:19]), (str(rtn['SM_Exgauster\\[0:7]']))[0:4]]
                sp_Exh3_Vibr8 = [dateutil.parser.isoparse((rtn['moment'])[0:19]), (str(rtn['SM_Exgauster\\[0:10]']))[0:4]]
                list_Exh3_Temp7.append(sp_Exh3_Temp7)
                list_Exh3_Temp8.append(sp_Exh3_Temp8)
                list_Exh3_Vibr7.append(sp_Exh3_Vibr7)
                list_Exh3_Vibr8.append(sp_Exh3_Vibr8)

        C.close()

    except Exception:  # отлавливаю потерю связи с БД и исключения и игнорирую их
        pass
    finally:
        pass

    return list_Exh3_Temp7, list_Exh3_Temp8, list_Exh3_Vibr7, list_Exh3_Vibr8

#list_Exh3_Temp7 = hendlerDB4()

#print('Привет, я четвертый поток', list_Exh3_Temp7)


#############################обработчик пятого потока ##################################################################

def hendlerDB5(): # функция обработчик пятого потока данных


    pass

def exh1_Alarm_Temp7():
    pass







    #print('Привет, я пятый поток')

#############################обработчик шестого потока ##################################################################

def hendlerDB6(): # функция обработчик шестого потока данных
    pass
    #print('Привет, я шестой поток')

#############################обработчик седьмого потока ##################################################################

def hendlerDB7(): # функция обработчик седьмого потока данных
    pass
    #print('Привет, я седьмой поток')

#############################обработчик восьмого потока ##################################################################

def hendlerDB8(): # функция обработчик восьмого потока данных
    pass
    #print('Привет, я восьмой поток')





if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    MainWindow = QtWidgets.QMainWindow()
    ui = Ui_MainWindow()
    ui.setupUi(MainWindow)
    MainWindow.show()
    sys.exit(app.exec_())






