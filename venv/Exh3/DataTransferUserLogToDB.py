
from PyQt5 import QtCore, QtGui, QtWidgets # для создания окна с кнопками

from PyQt5.QtCore import QThread # для разделения задач на потоки

import pyodbc
import csv
import time
import datetime


class ProgressBarThead(QThread): # класс для создания многопоточности

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
                hendlerDB()# вызываю функцию, которая выполняет обработку базы данных, csv файла(UserLog), txt файла(ConnectionDB)

                end = time.time()
                #print("Время цикла выполнения программы :", (end - start) * 10 ** 3, "ms")
                time.sleep(30)
        except Exception:  # отлавливаю потерю связи с БЮ и исключения и игнорирую их
            pass

        finally:
            pass

#********************************************************************************************************************
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
        MainWindow.setWindowTitle(_translate("MainWindow", "Передача данных в БД"))
        self.Btn_Start.setText(_translate("MainWindow", "Ok"))
        self.Btn_Stop.setText(_translate("MainWindow", "Cancel"))
        self.label.setText(_translate("MainWindow", "Включить передачу сообщений из журнала действий оператора SCADA --> БД"))
#*********************************************************************************************************************
# ********************************************************************************************************************

    # дополнительно прописываю метод для обработки событий при нажатии на кнопки
    def add_functions(self):
        # обращаюсь к нопкe "Ok" при "клике" и в скобках указываю какой метод будет срабатывать
        self.Btn_Start.clicked.connect(self.Launch_progress_bar_filling) # срабатывает метод Launch_progress_bar_filling

        self.Btn_Stop.clicked.connect(self.write_btn_stop)


        # инициализация класса
        # создаем экземпляр класса ProgressBarThead отдельного потока(нитки). Этому потоку(нитке) будет передаваться на
        # вход наше основное приложение-ссылка (mainwindow=self), чтобы она имела доступ ко всем элементам этого окна и
        # всем его параметрам
        self.ProgresbarTread_instance = ProgressBarThead(mainwindow=self) #

    # создаем функцию, которая будет запускать этот отдельный поток поток. Далее, когда получаем доступ
    def Launch_progress_bar_filling(self):
        self.ProgresbarTread_instance.start() # запускаю поток в постоянную работу с помощью метода start()

    def write_btn_stop(self, r = True): # метод, который срабатывае при нажатии кнопки СТОП
        raise SystemExit # закрываю окно
#********************************************************************************************************************
#********************************************************************************************************************
#********************************************************************************************************************
def hendlerDB(): # функция, которая выполняет обработку базы данных, csv файла(UserLog), txt файла(ConnectionDB)

    ####################################читаю connection_file.txt###################################################

    with open('connection_file.txt', 'r', encoding='utf-8') as file:

        txt_lines = file.readlines()

    txt_connection_string = txt_lines[1].strip() # connection string
    txt_logfile = txt_lines[4].strip()  # logfile
    txt_time_ponit = txt_lines[7].strip()  # time_ponit
    txt_command = txt_lines[10].strip()  # command
    txt_status_command = txt_lines[13].strip()  # status_command
    ##################################################################################################################

    ###########################  читаю UserLog.csv файл ##############################################################

    # создаю пустой список, в который перенесу все данные из Logfile
    csv_list = []

    file1 = open(txt_logfile, "r")

    for nr in file1:
        stroka = nr
        val_1 = datetime.datetime.strptime((stroka[0:19]).strip(), '%Y-%m-%d %H:%M:%S')
        val_2 = (stroka[20:33]).strip()
        val_3 = (stroka[34:84]).strip()
        val_4 = (stroka[85:230]).strip()
        val_5 = (stroka[231:269]).strip()
        val_6 = ''
        val_7 = (stroka[270:330]).strip()
        # собираю значения(строки) в кортеж
        spisok_tuple = (val_1, val_2, val_3, val_4, val_5, val_6, val_7,)
        csv_list.append(spisok_tuple)  # добавляю кортеж в список

    file1.close()

    ####################################################################################################################
    ################### переворачиваю порядок строк в csv файле ######################################################
    revers_csv_list = list(reversed(csv_list))
    ##################################################################################################################
    # конвертирую строку с датой из connection_file.txt в тип datetime
    txt_time_ponit_norm = datetime.datetime.strptime((txt_time_ponit[0:19]).strip(), '%Y-%m-%d %H:%M:%S')  # преобразовываю
    ####################################################################################################################

    ########### перебираю и записываю в список для отправки в БД строчки реверсного файла до тех пор,
    # пока она не будет строчка из реверсного файла не будет равно строчке в connection_file
    select_spisok = []
    for row in revers_csv_list:
        if row[0] == txt_time_ponit_norm and row[3] == txt_command and \
             row[4] == txt_status_command: # если строчка для отправки = строчки из файла txt, то записываю эту строчку
            select_spisok.append(row)
            break

        elif row[0] >= txt_time_ponit_norm:

            if row[0] > txt_time_ponit_norm:

                select_spisok.append(row)

            elif row[0] == txt_time_ponit_norm:
                if row[3] != txt_command:
                    select_spisok.append(row)
                elif row[4] != txt_status_command:
                        select_spisok.append(row)
    # переворачиваю чередование строк в списке, чтобы в БД записывались "сверху-вниз"
    data_to_db = list(reversed(select_spisok)) # список для отправки в БД

    if len(data_to_db) > 1:
        data_to_db.pop(0) # удаляю последнию строчку т.к. она уже есть в БД и её не надо повторно отравлять в БД

        try:
            #################################### отрываю БД и считываю таблицу и определяю последнюю(MAX) дату и строки
            # для сравнения #################################################################################################
            #!!!!!!!! заменить конекшенстринг
            conn = pyodbc.connect(txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки
            cur = conn.cursor()  # создали курсор, через который можно работать с БД
            #cur.fast_executemany = True  # это для ускорения передачи данных, но в этой версии Питона не хочет работать

            ######## запись в БД выбранных значений, которые в data_to_db###################################################ДЛЯ ПЕРЕДАЧИ ДАННЫХ РАСКОММЕНТИРОВАТЬ!!!!!!!!!!!!!!!!!!

            sql = "INSERT INTO UserLogOkat (sDATETIME, sFULLUSERNAME, sMECHNAME,sTAGDESC,sVALUE,sENGUNITS,sTAGNAME) VALUES ( ?, ?, ?, ?, ?, ?, ?)"
            #print(11)
            cur.executemany(sql, data_to_db) ############################################################################ДЛЯ ПЕРЕДАЧИ ДАННЫХ РАСКОММЕНТИРОВАТЬ!!!!!!!!!!!!!!!!!!
            ##########################################################################################
            conn.commit()  # сохраняем изменения в БД

            # повторно читаем, чтобы проверить, записались ли данные в БД
            conn = pyodbc.connect(txt_connection_string)  # конекшенстринг заключен в тройные кавычки для обозначения многострочной строки, иначе надо либо в одну строку, либо каждую строчку в кавычки
            cur = conn.cursor()  # создали курсор, через который можно работать с БД
            #cur.fast_executemany = True # это для ускорения передачи данных, но в этой версии Питона не хочет работать
            #print(22)

            ####### определяю последнее записанное значение по максимальной дате ################################
            cur.execute("select sDATETIME, sFULLUSERNAME, sMECHNAME,sTAGDESC,sVALUE,sENGUNITS,sTAGNAME  from UserLogOkat where sDATETIME = (select MAX(sDATETIME) from UserLogOkat)")  # записываю в курсор максимальное значение времени из БД

            db_list = []  # создаю пустой список для дальнейшей записи в него строки с максимальным времением.
            for line in cur:
                db_list.append(line)
            # проверяю наличие строчки в БД, которую отправляли. Так много проверок потому, что в одну секунду может быть несколько значений
            if (db_list[0][0] == select_spisok[0][0] and \
                    db_list[0][3] == select_spisok[0][3] and \
                    db_list[0][4] == select_spisok[0][4]) or \
                    (db_list[1][0] == select_spisok[0][0] and \
                     db_list[1][3] == select_spisok[0][3] and \
                     db_list[1][4] == select_spisok[0][4]) or \
                    (db_list[2][0] == select_spisok[0][0] and \
                     db_list[2][3] == select_spisok[0][3] and \
                     db_list[2][4] == select_spisok[0][4]) or \
                    (db_list[3][0] == select_spisok[0][0] and \
                     db_list[3][3] == select_spisok[0][3] and \
                     db_list[3][4] == select_spisok[0][4]):

                # считываю connection_file.txt для создания списка lines, который потом редактирую и делаю перезапись
                # файла connection_file.txt
                with open('connection_file.txt', 'r', encoding='utf-8') as fr:
                    lines = fr.readlines()
                # запись в connection_file.txt
                lines[7] = str(select_spisok[0][0]) + str('\n') # добавил \n при записи в список пустая строка под записью обрезается
                lines[10] = str(select_spisok[0][3]) + str('\n')
                lines[13] = str(select_spisok[0][4]) + str('\n')
                # записываю в данные в connection_file.txt
                with open('connection_file.txt', 'w', encoding='utf-8') as fl:
                    fl.writelines(lines)

            conn.commit()  # сохраняем изменения в БД
            conn.close()  # закрываем подключение к базе
        except Exception:  # отлавливаю потерю связи с БЮ и исключения и игнорирую их
            pass
        finally:
            pass


if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    MainWindow = QtWidgets.QMainWindow()
    ui = Ui_MainWindow()
    ui.setupUi(MainWindow)
    MainWindow.show()
    sys.exit(app.exec_())


