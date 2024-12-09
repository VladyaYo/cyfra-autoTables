import os
import asyncio

from getMonth import get_user_number
from sortCategoryAnalytic import sort_category_analytic
from pivotClientData import pivot_client_data
from clientDataCategoryFilter import client_data_category_filter
from mergeAllData import merge_all_data

async def main():
    # Создание таблиц
    print("Разбивка по категориям таблицы из google ads...")

    # Используем обертку для передачи параметров в функцию
    def sort_category_analytic_wrapper():
        return sort_category_analytic()

    table_category_analytic_path = await asyncio.to_thread(sort_category_analytic_wrapper)
    print(f"Таблица создана: {table_category_analytic_path}")

    print("Сводим клиентскую таблицу...")

    def pivot_client_data_wrapper():
        return pivot_client_data()

    table_pivot_client_path = await asyncio.to_thread(pivot_client_data_wrapper)
    print(f"Таблица создана: {table_pivot_client_path}")

    print("Фильтруем клиентскую таблицу...")

    def client_data_category_filter_wrapper():
        return client_data_category_filter()

    processed_client_data = await asyncio.to_thread(client_data_category_filter_wrapper)
    print(f"Обработка клиентской таблицы завершена: {processed_client_data}")

    print("Соединяем все в финальную таблицу")

    def merge_all_data_wrapper():
        return merge_all_data()

    merge_table = await asyncio.to_thread(merge_all_data_wrapper)
    print(f"Соединение финальной таблицы завершено: {merge_table}")

if __name__ == "__main__":
     asyncio.run(main())
