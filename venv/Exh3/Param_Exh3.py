# видос конекшен стринг https://yandex.ru/video/preview/11619706852423636432


# from PyQt5 import QtCore, QtGui, QtWidgets # для создания окна с кнопками
#
# from PyQt5.QtCore import QThread # для разделения задач на потоки
#
# import pyodbc
# import csv
import time
import datetime

start = time.time()
try:



    # чтение и запись данных эксгаустер №3 вибрация т.1
    import Exh3_Vibr1

    # чтение и запись данных эксгаустер №3 вибрация т.2
    import Exh3_Vibr2

    # чтение и запись данных эксгаустер №3 температура т.1print("Время цикла выполнения программы :", (end - start) * 10 ** 3, "ms")
    import Exh3_Temp1

    # чтение и запись данных эксгаустер №3 температура т.2
    import Exh3_Temp2

    # чтение и запись данных эксгаустер №3 температура т.3
    import Exh3_Temp3








except Exception:  # отлавливаю потерю связи с БЮ и исключения и игнорирую их
    pass

finally:
    pass
time.sleep(2)
end = time.time()
print("Время цикла выполнения программы считывания и записи данных в БД:", (end - start) * 10 ** 3, "ms")

breakpoint()

