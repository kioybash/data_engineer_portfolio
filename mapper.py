#!/usr/bin/env python
"""mapper.py"""

import sys

# Список различных типов оплаты
payment_dic = [
    'Credit card',
    'Cash',
    'No charge',
    'Dispute',
    'Unknown',
    'Voided trip'
]

# Функция для выполнения основной логики
def perform_map():
    for line in sys.stdin:  # Читаем строки из стандартного потока ввода
        line = line.strip()  # Убираем пробелы и символы новой строки
        rows = line.split(',')  # Разбиваем строку на список, разделяя по запятой
        try:
            tips = float(rows[13])  # Преобразуем сумму чаевых в число с плавающей запятой
            payment_type = payment_dic[int(rows[9]) - 1]  # Определяем тип оплаты по индексу в списке
        except ValueError:
            continue  # Если возникает ошибка, пропускаем строку и переходим к следующей

        split_date = rows[1].split('-')  # Разбиваем дату на год и месяц
        if split_date[0] != '2020' or tips < 0:  # Проверяем условие
            continue  # Если условие не выполняется, пропускаем строку

        month = str('{}-{}'.format(split_date[0], split_date[1]))  # Форматируем месяц и год

        # Выводим результат в формате "год-месяц, тип оплаты    сумма чаевых"
        print('{},{}\t{}'.format(month, payment_type, tips))

# Запускаем функцию perform_map, если скрипт выполняется непосредственно
if __name__ == '__main__':
    perform_map()
