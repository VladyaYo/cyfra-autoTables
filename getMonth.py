def get_user_number():
    while True:
        try:
            user_input = input("Какой месяц обрабатываем? Введите целое число от 1 до 12: ")
            number = int(user_input)  # Преобразуем ввод в целое число

            if 1 <= number <= 12:
                return number  # Если число в диапазоне, возвращаем его
            else:
                print("Ошибка: число должно быть от 1 до 12. Повторите ввод.")
        except ValueError:
            print("Ошибка: введите целое числовое значение. Повторите ввод.")