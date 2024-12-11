import pandas as pd

def client_data_category_filter():
    # Шаг 1: Загрузка таблицы
    df = pd.read_csv("export_data/client_data_with_chanel.csv")

    # Шаг 2: Определяем категории и условия для фильтрации
    categories = {
        'Apple + аксессуары': (
            (df['Подкатегория товара1'] == 'Apple') &
            (
                (df['Подкатегория товара2'] == 'Аксессуары для техники Apple') |
                (df['Подкатегория товара2'] == 'iPhone')
            )
        ),
        'Смартфоны': (
            (df['Подкатегория товара1'] == 'Мобильная связь') &
            ~df['Подкатегория товара3'].str.contains('Зарядные станции', case=False, na=False)
        ),
        'Ноутбуки и пк': (
            (df['Подкатегория товара1'] == 'Ноутбуки, ПК, Планшеты') &
            (
                (df['Подкатегория товара2'] == 'Ноутбуки и моноблоки') |
                (df['Подкатегория товара2'] == 'ПК и комплектующие')
            )
        ),
        'Мелкая бытовая техника': (
                (df['Подкатегория товара1'] == 'Бытовая техника') &
                (df['Подкатегория товара2'] == 'Мелкая бытовая техника')
        ),
        'Крупная бытовая техника': (
                (df['Подкатегория товара1'] == 'Бытовая техника') &
                (df['Подкатегория товара2'] == 'Крупная бытовая техника') |
                (df['Подкатегория товара2'] == 'Встраиваемая бытовая техника') |
                (df['Подкатегория товара2'] == 'Оборудование для магазинов и ресторанов')
        ),
        'Тв, аудио-видео техника': (
                (df['Подкатегория товара1'] == 'ТВ, Аудио-видео техника') &
                (df['Подкатегория товара2'] == 'Телевизоры') |
                (df['Подкатегория товара2'] == 'Аксессуары для телевизоров') |
                (df['Подкатегория товара2'] == 'Проекционное оборудование') |
                (df['Подкатегория товара2'] == 'Домашнее аудио. Кино.')
        ),
        'Mac/iPad': (
                (df['Подкатегория товара1'] == 'Apple') &
                (df['Подкатегория товара2'] == 'Mac') |
                (df['Подкатегория товара2'] == 'iPad')
        ),
        'Планшеты/ эл книги': (
                (df['Подкатегория товара1'] == 'Ноутбуки, ПК, Планшеты') &
                (df['Подкатегория товара2'] == 'Планшеты и электронные книги')
        ),
        'Apple watch/AirPods': (
                (df['Подкатегория товара1'] == 'Apple') &
                (df['Подкатегория товара2'] == 'Apple Watch')|
                #добавить airpods
                (df['Подкатегория товара3'] == 'Наушники') &
                (df['Наименование товара'].str.contains('AirPods', case=False, na=False))

        ),
        'Цифровые фотоаппараты/видеокамеры': (
                (df['Подкатегория товара1'] == 'Фотосъемка')

        ),
        'SSD накопители': (
                (df['Подкатегория товара3'] == 'SSD-накопители')
        ),
        'Наушники': (
                (df['Подкатегория товара3'] == 'Наушники')&
                #bcrk.xftv  AirPods
                ~(df['Наименование товара'].str.contains('AirPods', case=False, na=False))
        ),
        'Умные часы': (
                (df['Подкатегория товара2'] == 'Умные часы (Smart Watch)') |
                (df['Подкатегория товара2'] == 'Фитнес-браслеты')
        ),
        'БАДы': (
                (df['Подкатегория товара1'] == 'БАДы')
        ),
        'Зарядные станции': (
                (df['Подкатегория товара3'] == 'Зарядные станции')
        ),
        'БУ': (
                (df['Подкатегория товара1'] == 'Прочие') &
                (df['Подкатегория товара2'] == 'Уценка и услуги')
        ),
        'Дроны': (
                (df['Подкатегория товара2'] == 'Радиоуправляемые модели хобби')
        ),
        'Офисная техника': (
                (df['Подкатегория товара2'] == 'Офисная техника')
        ),
    }

    # Шаг 3: Создаём таблицу с категориями
    result_df = pd.DataFrame()

    for category, condition in categories.items():
        category_data = df[condition].copy()  # Фильтруем строки, которые подходят под условие
        category_data['Категория'] = category  # Добавляем колонку с названием категории
        result_df = pd.concat([result_df, category_data], ignore_index=True)

    # Шаг 4: Сохранение результата
    result_df.to_csv("export_data/filtered_client_sales.csv", index=False)
    # print(result_df)

    # df_res = pd.read_csv("export_data/filtered_client_sales.csv")

    # for col in ['Маржа ГРН', 'Сумма грн', 'Закупка ГРН']:
    #     result_df[col] = result_df[col].astype(str).str.replace(',', '.').astype(float)

    # Шаг 2: Обработка колонки "количество"
    # Преобразуем колонку "количество" в значения 1 или -1
    result_df['Количество'] = result_df['Количество'].apply(lambda x: 1 if x > 0 else -1)

    # Шаг 3: Объединение по категориям и подсчёт сумм
    # Суммируем "количество", "Маржа ГРН", "Сумма грн", "Закупка ГРН" для каждой категории
    grouped_df = result_df.groupby('Категория', as_index=False).agg({
        'Количество': 'sum',     # Сумма преобразованных значений в колонке "количество"
        'Маржа ГРН': 'sum',      # Суммируем колонку "Маржа ГРН"
        'Сумма грн': 'sum',      # Суммируем колонку "Сумма грн"
        'Закупка ГРН': 'sum'     # Суммируем колонку "Закупка ГРН"
    })

    # Шаг 4: Сохранение результата
    export_path = "export_data/grouped_categorized_summary.csv"
    grouped_df.to_csv(export_path, index=False)
    print(grouped_df)
    return export_path