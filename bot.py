import os
from dotenv import load_dotenv
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

from main import main  # Импорт вашей программы

# Загрузка переменных окружения
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    raise ValueError("Токен бота не найден. Убедитесь, что он указан в файле .env")

# Папка для исходных файлов
DATA_FOLDER = "data/"
os.makedirs(DATA_FOLDER, exist_ok=True)

# Папка для сохранения обработанных данных
EXPORT_FOLDER = "export_data/"
os.makedirs(EXPORT_FOLDER, exist_ok=True)

# Требуемые файлы
REQUIRED_FILES = ["ads_data.csv", "ga_original_data.csv", "client_data.xlsx"]

# Хранилище для файлов сессии
user_files = {}


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [['/start', '/cancel']]  # Клавиатура с кнопками
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)  # Уменьшенный размер
    await update.message.reply_text(
        "Привет! Отправь мне три CSV-файла с точными названиями:\n"
        "- ads_data.csv\n"
        "- ga_original_data.csv\n"
        "- client_data.csv\n"
        "Чтобы сбросить процесс загрузки, используйте команду /cancel.",
        reply_markup=reply_markup
    )


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id not in user_files:
        user_files[user_id] = {}

    # Получаем имя файла
    file_name = update.message.document.file_name

    # Проверяем, ожидается ли файл
    if file_name not in REQUIRED_FILES:
        await update.message.reply_text(
            f"Ошибка: файл {file_name} не ожидается. Пожалуйста, отправьте файлы с правильными именами."
        )
        return

    # Сохраняем файл
    file = await update.message.document.get_file()
    file_path = os.path.join(DATA_FOLDER, file_name)
    await file.download_to_drive(file_path)
    user_files[user_id][file_name] = file_path

    # Проверяем, все ли файлы получены
    if len(user_files[user_id]) == len(REQUIRED_FILES):
        await update.message.reply_text("Все файлы получены. Начинаю обработку...")

        try:
            # Асинхронный запуск вашего main
            await run_processing(user_files[user_id], update, context)
        except Exception as e:
            await update.message.reply_text(f"Произошла ошибка при обработке: {e}")
        finally:
            # Чистим временные файлы
            for file in user_files[user_id].values():
                os.remove(file)
            user_files[user_id] = {}
    else:
        remaining_files = set(REQUIRED_FILES) - set(user_files[user_id].keys())
        await update.message.reply_text(
            f"Файл {file_name} сохранен. Ожидаются еще: {', '.join(remaining_files)}."
        )


async def run_processing(files, update, context):
    try:
        # Получаем пути к файлам из переданных данных
        ads_data = files["ads_data.csv"]
        ga_original_data = files["ga_original_data.csv"]
        client_data = files["client_data.xlsx"]

        # Установка переменных окружения для main
        os.environ["ADS_DATA"] = ads_data
        os.environ["GA_ORIGINAL_DATA"] = ga_original_data
        os.environ["CLIENT_DATA"] = client_data

        # Запуск вашей программы
        await main()

        result_file = os.path.join(EXPORT_FOLDER, "final.csv")  # Итоговый файл
        await context.bot.send_document(chat_id=update.effective_chat.id, document=open(result_file, "rb"))
    except Exception as e:
        await update.message.reply_text(f"Ошибка обработки: {e}")


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id in user_files:
        for file in user_files[user_id].values():
            if os.path.exists(file):
                os.remove(file)
        user_files[user_id] = {}
    await update.message.reply_text("Сессия сброшена. Вы можете начать заново.")


def main_bot():
    # Инициализация приложения
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Обработчики
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_file))  # Используем фильтр для всех документов

    print("Бот запущен...")
    app.run_polling()


if __name__ == "__main__":
    main_bot()
