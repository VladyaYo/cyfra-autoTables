import pandas as pd

def pivot_client_data():
    # Чтение данных из файла CSV
    file_path = "data/client_data.csv"
    df = pd.read_csv(file_path)

    # Обработка колонки с числами (например, "Цена ГРН", "Закупка ГРН", "Сумма грн")
    columns_to_clean = ["Цена ГРН", "Закупка ГРН", "Сумма грн"]  # Замените на ваши названия колонок

    for col in columns_to_clean:
        # Удаляем лишние символы и преобразуем в числовой формат
        df[col] = df[col].astype(str)  # Преобразуем в строки для обработки
        df[col] = df[col].str.replace(",", ".")  # Заменяем запятую на точку
        df[col] = df[col].str.replace(r"[^\d\.-]", "", regex=True)  # Удаляем всё, кроме цифр, "-" и "."
        df[col] = pd.to_numeric(df[col], errors="coerce")  # Преобразуем в числа

    df["Номер заказа на сайте"] = pd.to_numeric(df["Номер заказа на сайте"], errors="coerce").astype("Int64")

    # # Фильтрация по колонке "месяц"
    # filtered_month = 9  # Укажите нужный месяц
    # filtered_df = df[df["Месяц отгрузки заказа"] == filtered_month]

    # Создание сводной таблицы
    pivot_table = df.pivot_table(
        index="Номер заказа на сайте",  # Строки
        values=["Цена ГРН", "Закупка ГРН", "Сумма грн"],  # Колонки для агрегации
        aggfunc="sum"  # Суммирование
    )

    # Сохранение результата в новый CSV
    pivot_table.to_csv("export_data/pivot_table.csv")

    # добавление колонки "источник"
    # Чтение таблиц
    df1 = pd.read_csv("data/client_data.csv")  # Первая таблица
   ##
    # ga_table_path = "data/ga_original_data.csv"
    # indicator = "Джерело"
    #
    # # Чтение файла без указания заголовков
    # input_df = pd.read_csv(ga_table_path, header=None, encoding='utf-8')
    #
    # # Поиск строки с индикатором
    # start_row = input_df.apply(lambda row: row.astype(str).str.contains(indicator).any(), axis=1).idxmax()

    # Чтение данных начиная с найденной строки
    # df2 = pd.read_csv(ga_table_path, skiprows=start_row, encoding='utf-8')
    ##

    # df2 = pd.read_csv("data/ga_original_data.csv", skiprows=9)
    file_path_ga = "data/ga_original_data.csv"

    # Шаг 1: Найти первую строку с более чем 10 колонками
    with open(file_path_ga, "r", encoding="utf-8") as file:
        for i, line in enumerate(file):
            if len(line.split(",")) > 3:  # Предполагаем, что разделитель - запятая
                header_row = i
                break

    df2 = pd.read_csv(file_path_ga, skiprows=header_row)

    # Фильтруем данные за 9-й месяц в первой таблице
    # df1_filtered = df1[df1["Месяц отгрузки заказа"] == 9]  # Здесь "месяц" — колонка в первой таблице
    #
    # # Убираем ".0" и приводим колонки с идентификаторами к числовому формату
    # df1_filtered = df1_filtered.copy()  # Создаем копию DataFrame
    # df1_filtered["Номер заказа на сайте"] = pd.to_numeric(df1_filtered["Номер заказа на сайте"], errors="coerce").astype("Int64")

    df2 = df2.copy()  # Создаем копию DataFrame
    df2["Ідентифікатор трансакції"] = pd.to_numeric(df2["Ідентифікатор трансакції"], errors="coerce").astype("Int64")

    # Добавляем колонку "источник" в df1_filtered, совмещая таблицы
    df_merged = df1.merge(
        df2[["Ідентифікатор трансакції", "Джерело / канал сеансу"]],  # Берём только нужные колонки из df2
        left_on="Номер заказа на сайте",  # Ключ из df1_filtered
        right_on="Ідентифікатор трансакції",  # Ключ из df2
        how="left"  # 'left' сохраняет все строки из df1_filtered
    )

    # Переименовываем колонку "Джерело / канал" в "источник"
    df_merged.rename(columns={"Джерело / канал сеансу": "источник"}, inplace=True)

    # Оставляем только строки, где "источник" не пустой
    df_result = df_merged[df_merged["источник"].notna()]

    # Сохраняем результат в новый CSV
    table_path = "export_data/client_data_with_chanel.csv"
    df_result.to_csv(table_path, index=False)
    print(df_result.head())
    return table_path


