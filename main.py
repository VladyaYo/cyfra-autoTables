import asyncio

from getMonth import get_user_number
from sortCategoryAnalytic import sort_category_analytic
from pivotClientData import pivot_client_data
from clientDataCategoryFilter import client_data_category_filter
from mergeAllData import merge_all_data

async def main():
    # Создание таблиц
    print("Разбивка по категориям таблицы из google ads...")
    table_category_analytic_path = await asyncio.to_thread(sort_category_analytic)
    print(f"Таблица создана: {table_category_analytic_path}")
    # user_number = get_user_number()
    print("Сводим клиентскую таблицу, добавляем колонку 'источник', удаляем все строки без источника ...")
    table_pivot_client_path = await asyncio.to_thread(pivot_client_data)
    print(f"Таблица создана: {table_pivot_client_path}")

    print("Фильтруем клиентскую таблицу по категориям...")
    processed_client_data = await asyncio.to_thread(client_data_category_filter)
    print(f"Обработка клиентской таблицы завершена: {processed_client_data}")

    print("Соединяем все в финальную таблицу")
    merge_table = await asyncio.to_thread(merge_all_data)
    print(f"Соединение финальной таблицы завершено: {merge_table}")

if __name__ == "__main__":
    asyncio.run(main())