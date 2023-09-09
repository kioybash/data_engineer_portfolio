#!/usr/bin/env python
"""reducer.py"""

import sys

def perform_reduce():
    global date, payment, current_date

    current_payment = None  # Текущий тип оплаты
    current_count = 0  # Текущее количество записей
    current_tips = 0  # Текущая сумма чаевых
    for line in sys.stdin:
        line = line.strip()
        payment_date, tips = line.split('\t', 1)
        date, payment = payment_date.split(',', 1)

        try:
            tips = float(tips)  # Преобразуем сумму чаевых в число с плавающей запятой
        except ValueError:
            continue  # Если возникает ошибка, пропускаем строку и переходим к следующей

        if current_payment == payment and current_date == date:
            current_tips += tips  # Если тип оплаты и дата совпадают, увеличиваем сумму чаевых и количество записей
            current_count += 1
        else:
            if current_payment and current_date:
                # Выводим результат: "год-месяц, тип оплаты, средние чаевые"
                print('{},{},{}'.format(current_date, current_payment, round(current_tips / current_count, 2)))
            current_payment = payment  # Обновляем текущий тип оплаты
            current_date = date  # Обновляем текущую дату
            current_tips = tips  # Обновляем текущую сумму чаевых
            current_count = 1  # Сбрасываем счетчик

    if current_payment == payment and current_date == date:
        # Выводим последний результат
        print('{},{},{}'.format(current_date, current_payment, round(current_tips / current_count, 2)))

if __name__ == '__main__':
    perform_reduce()
