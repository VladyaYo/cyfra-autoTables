import pandas as pd

def merge_all_data():
    # Шаг 1: Загрузка таблиц
    grouped_table = pd.read_csv("export_data/grouped_categorized_summary.csv")
    filtered_table = pd.read_csv("export_data/categorized_table.csv")

    # Шаг 2: Добавление колонок "Вартість з ПДВ" и "Маржа-Вартість (з ПДВ)"
    filtered_table['Вартість з пдв'] = filtered_table['Вартість'] * 1.2
    filtered_table = filtered_table.merge(
        grouped_table[['Категория', 'Маржа ГРН', 'Количество', 'Сумма грн']],
        on='Категория',
        how='left'
    )
    filtered_table['Маржа-Вартість (з ПДВ)'] = filtered_table['Маржа ГРН'] - filtered_table['Вартість з пдв']

    # Шаг 3: Добавление строки с итогами
    sum_row = filtered_table.select_dtypes(include=['number']).sum().to_dict()
    sum_row['Категория'] = 'Итого'
    filtered_table = pd.concat([filtered_table, pd.DataFrame([sum_row])], ignore_index=True)

    # Шаг 4: Округление значений
    # Округляем все числовые значения до 2 знаков, "Конверсии" — до целого числа
    numeric_columns = filtered_table.select_dtypes(include=['float', 'int']).columns
    filtered_table[numeric_columns] = filtered_table[numeric_columns].round(2)

    if 'Конверсії' in filtered_table.columns:
        filtered_table['Конверсії'] = filtered_table['Конверсії'].round(0).astype('Int64')
    if 'Количество' in filtered_table.columns:
        filtered_table['Количество'] = filtered_table['Количество'].round(0).astype('Int64')

    # Шаг 5: Упорядочивание колонок
    desired_order = [
        'Категория', 'Вартість', 'Вартість з пдв', 'Конверсії', 'Цінність конв.',
        'Сумма грн', 'Закупка ГРН', 'Количество', 'Маржа ГРН', 'Маржа-Вартість (з ПДВ)'
    ]
    existing_columns = [col for col in desired_order if col in filtered_table.columns]
    filtered_table = filtered_table[existing_columns]

    export_path = "export_data/final.csv"
    # Шаг 6: Сохранение результата
    filtered_table.to_csv("export_data/final.csv", index=False)
    print(filtered_table)
    return export_path
