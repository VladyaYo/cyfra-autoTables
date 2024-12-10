import pandas as pd


def convert_xlsx_to_csv(input_file, output_file, sheet_name=0):
    """
    Конвертирует файл Excel (.xlsx) в компактный CSV с очисткой числовых данных.

    :param input_file: Путь к входному .xlsx файлу.
    :param output_file: Путь к выходному .csv файлу.
    :param sheet_name: Название листа (по умолчанию первый).
    """
    # Загрузка данных из файла Excel
    df = pd.read_excel(input_file, sheet_name=sheet_name)

    # Указание колонок для фильтрации
    columns_to_keep = [
        'Подкатегория товара1', 'Подкатегория товара2', 'Подкатегория товара3',
        'Наименование товара', 'Цена ГРН', 'Сумма грн', 'Номер заказа на сайте',
        'Закупка ГРН', 'Маржа ГРН', 'Количество'
    ]

    # Фильтруем и работаем с копией данных
    filtered_df = df[columns_to_keep].copy()


    # Сохранение в компактный CSV
    filtered_df.to_csv(output_file, index=False, encoding='utf-8')
    # print(f"Файл сохранен как {output_file}")
    return output_file
