# @title
# ======================================================================
# ПАРСИНГ ДАННЫХ - Часть 1 (ИСПРАВЛЕННАЯ И ДОПОЛНЕННАЯ ВЕРСИЯ)
# ======================================================================
# Установка необходимых библиотек
#!pip install -q gspread oauth2client pandas openpyxl
#!pip install -q --upgrade google-api-python-client
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
import json
import re
import warnings
import os
warnings.filterwarnings('ignore')

import time as sleepTime


# -------------------------------
# 1. НАСТРОЙКА ПОДКЛЮЧЕНИЯ
# -------------------------------
print("Настройка подключения к Google Sheets...")
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')

# СЕРВИСНЫЙ АККАУНТ - ВСТРОЕННЫЙ JSON
SERVICE_ACCOUNT_JSON = os.environ.get('SERVICE_ACCOUNT_INFO')


if not SPREADSHEET_ID or not SERVICE_ACCOUNT_JSON:
  raise Exception("Secrets not found")

SERVICE_ACCOUNT_INFO = json.loads(SERVICE_ACCOUNT_JSON)


scope = ['https://spreadsheets.google.com/feeds ','https://www.googleapis.com/auth/drive ']
try:
    creds = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_INFO, scope)
    client = gspread.authorize(creds)
    print("Аутентификация успешна!")
    DEMO_MODE = False
except Exception as e:
    print(f"Ошибка аутентификации: {e}")
    print("Используем демо-данные")
    DEMO_MODE = True
# -------------------------------
# 2. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# -------------------------------
def parse_time_from_cell(cell_value):
    """Извлекает время из ячейки"""
    if not cell_value:
        return ""
    time_match = re.search(r'(\d{1,2}:\d{2})', str(cell_value))
    if time_match:
        return time_match.group(1)
    return ""

def parse_number(value):
    """Парсит число из строки"""
    if value is None or value == "" or str(value).strip() == "":
        return 0
    try:
        str_value = str(value).strip()
        str_value = str_value.replace(',', '.')
        str_value = re.sub(r'(\d)\s+(\d)', r'\1\2', str_value)
        str_value = re.sub(r'[^\d\.\-]', '', str_value)
        if str_value in ['', '-', '.']:
            return 0
        result = float(str_value)
        if abs(result - int(result)) < 0.001:
            return int(result)
        return result
    except Exception as e:
        return 0

def parse_percent(value):
    """Парсит процент из строки и возвращает строку в формате 'X,X%' или '100%'"""
    if value is None or value == "" or str(value).strip() == "":
        return "0,0%"
    try:
        str_value = str(value).strip()
        str_value = str_value.replace('%', '')
        str_value = str_value.replace(',', '.')
        str_value = str_value.replace(' ', '')
        if str_value in ['', '-', '.']:
            return "0,0%"
        result = float(str_value)
        rounded_result = round(result, 1)
        if abs(rounded_result - 100.0) < 0.001:
            return "100%"
        formatted_result = f"{rounded_result:.1f}".replace('.', ',') + '%'
        return formatted_result
    except Exception as e:
        return "0,0%"

def determine_day_type(data):
    """Определяет тип дня по значению в ячейке E4 (строка 4, столбец 4)"""
    if len(data) > 3 and len(data[3]) > 4:
        cell_value = str(data[3][4]).strip().lower()
        print(f"  Значение в E4: '{cell_value}'")
        if '7:15' in cell_value:
            return 'weekday'
        elif '8:00' in cell_value:
            return 'weekend'
    return 'weekday'

# -------------------------------
# 3. КОНФИГУРАЦИЯ БЛОКОВ ДЛЯ РАЗНЫХ ТИПОВ ДНЕЙ
# -------------------------------
def get_time_blocks_config(day_type):
    """Возвращает конфигурацию блоков в зависимости от типа дня"""
    if day_type == 'weekday':
        return [
            {'time_cell_row': 3, 'time_cell_col': 4, 'bus_col_start': 2, 'ebus_col_start': 2},
            {'time_cell_row': 3, 'time_cell_col': 10, 'bus_col_start': 8, 'ebus_col_start': 8},
            {'time_cell_row': 3, 'time_cell_col': 16, 'bus_col_start': 14, 'ebus_col_start': 14},
            {'time_cell_row': 3, 'time_cell_col': 22, 'bus_col_start': 20, 'ebus_col_start': 20},
            {'time_cell_row': 3, 'time_cell_col': 28, 'bus_col_start': 26, 'ebus_col_start': 26},
            {'time_cell_row': 3, 'time_cell_col': 34, 'bus_col_start': 32, 'ebus_col_start': 32},
            {'time_cell_row': 3, 'time_cell_col': 40, 'bus_col_start': 38, 'ebus_col_start': 38},
            {'time_cell_row': 3, 'time_cell_col': 46, 'bus_col_start': 44, 'ebus_col_start': 44},
            {'time_cell_row': 3, 'time_cell_col': 52, 'bus_col_start': 50, 'ebus_col_start': 50},
            {'time_cell_row': 3, 'time_cell_col': 58, 'bus_col_start': 56, 'ebus_col_start': 56},
            {'time_cell_row': 3, 'time_cell_col': 64, 'bus_col_start': 62, 'ebus_col_start': 62},
            {'time_cell_row': 3, 'time_cell_col': 70, 'bus_col_start': 68, 'ebus_col_start': 68},
            {'time_cell_row': 3, 'time_cell_col': 76, 'bus_col_start': 74, 'ebus_col_start': 74},
            {'time_cell_row': 3, 'time_cell_col': 82, 'bus_col_start': 80, 'ebus_col_start': 80},
        ]
    else:
        return [
            {'time_cell_row': 3, 'time_cell_col': 4, 'bus_col_start': 2, 'ebus_col_start': 2},
            {'time_cell_row': 3, 'time_cell_col': 10, 'bus_col_start': 8, 'ebus_col_start': 8},
            {'time_cell_row': 3, 'time_cell_col': 16, 'bus_col_start': 14, 'ebus_col_start': 14},
            {'time_cell_row': 3, 'time_cell_col': 22, 'bus_col_start': 20, 'ebus_col_start': 20},
            {'time_cell_row': 3, 'time_cell_col': 28, 'bus_col_start': 26, 'ebus_col_start': 26},
            {'time_cell_row': 3, 'time_cell_col': 34, 'bus_col_start': 32, 'ebus_col_start': 32},
        ]

# -------------------------------
# 4. ПАРСИНГ ЛИСТА С ДАТОЙ
# -------------------------------
def parse_sheet_by_date(sheet_data, date_str):
    """Парсит данные листа с указанной датой"""
    records = []
    day_type = determine_day_type(sheet_data)
    print(f"  Тип дня: {'Будний (14 блоков)' if day_type == 'weekday' else 'Выходной (6 блоков)'}")
    time_blocks = get_time_blocks_config(day_type)
    for block_idx, block_config in enumerate(time_blocks, 1):
        if (block_config['time_cell_row'] < len(sheet_data) and
            block_config['time_cell_col'] < len(sheet_data[block_config['time_cell_row']])):
            time_str = parse_time_from_cell(sheet_data[block_config['time_cell_row']][block_config['time_cell_col']])
        else:
            time_str = ""
        print(f"  Блок {block_idx}: Время {time_str}")
        parse_bus_block(sheet_data, date_str, time_str, block_config['bus_col_start'], records)
        parse_ebus_block(sheet_data, date_str, time_str, block_config['ebus_col_start'], records)
    return records

def parse_bus_block(data, date_str, time_str, col_start, records):
    """Парсит блок автобусов (строки 6-28)"""
    bus_mapping = [
        (5, 'СВ', 'Бибирево'),
        (6, 'СВ', 'Марьина Роща'),
        (7, 'СВ', 'Дангауэровка'),
        (8, 'СВ', 'Гольяново'),
        (9, 'СВ', 'Всего'),
        (10, 'СЗ', 'Пресня'),
        (11, 'СЗ', 'Мневники'),
        (12, 'СЗ', 'Фили'),
        (13, 'СЗ', 'Ховрино'),
        (14, 'СЗ', 'Тушино'),
        (15, 'СЗ', 'Красногорск'),
        (16, 'СЗ', 'Зеленоградская'),
        (17, 'СЗ', 'Всего'),
        (18, 'Ю', 'Нагатино'),
        (19, 'Ю', 'Андропова'),
        (20, 'Ю', 'Домодедовская'),
        (21, 'Ю', 'Коньково'),
        (22, 'Ю', 'Очаково'),
        (23, 'Ю', 'Ясенево'),
        (24, 'Ю', 'Красная Пахра'),
        (25, 'Ю', 'Чертаново'),
        (26, 'Ю', 'Всего'),
        (27, 'МГТ', 'Всего автобус')
    ]
    for row_idx, filial, platform in bus_mapping:
        if row_idx < len(data):
            plan_value = 0
            fact_value = 0
            under_value = 0
            percent_value = "0,0%"
            if col_start + 0 < len(data[row_idx]):
                plan_value = parse_number(data[row_idx][col_start + 0])
            if col_start + 1 < len(data[row_idx]):
                fact_value = parse_number(data[row_idx][col_start + 1])
            if col_start + 2 < len(data[row_idx]):
                under_value = parse_number(data[row_idx][col_start + 2])
            if col_start + 3 < len(data[row_idx]):
                percent_value = parse_percent(data[row_idx][col_start + 3])
            records.append({
                'Дата': date_str,
                'Время': time_str,
                'Филиал': filial,
                'Площадка': platform,
                'Тип транспорта': 'Автобус',
                'План': plan_value,
                'Факт': fact_value,
                'Недовыпуск': under_value,
                '% выполнения плана': percent_value
            })

def parse_ebus_block(data, date_str, time_str, col_start, records):
    """Парсит блок электробусов (строки 35-50)"""
    ebus_mapping = [
        (34, 'СВ', 'Бибирево'),
        (35, 'СВ', 'В.Лихоборы'),
        (36, 'СВ', 'Останкино'),
        (37, 'СВ', 'Новокосино'),
        (38, 'СВ', 'Салтыковка'),
        (39, 'СВ', 'Всего'),
        (40, 'СЗ', 'Сокол'),
        (41, 'СЗ', 'Фили'),
        (42, 'СЗ', 'Митино'),
        (43, 'СЗ', 'Всего'),
        (44, 'Ю', 'Коньково'),
        (45, 'Ю', 'Красная Пахра'),
        (46, 'Ю', 'Нагорная'),
        (47, 'Ю', 'Чертаново'),
        (48, 'Ю', 'Всего'),
        (49, 'МГТ', 'Всего электробус')
    ]
    for row_idx, filial, platform in ebus_mapping:
        if row_idx < len(data):
            plan_value = 0
            fact_value = 0
            under_value = 0
            percent_value = "0,0%"
            if col_start + 0 < len(data[row_idx]):
                plan_value = parse_number(data[row_idx][col_start + 0])
            if col_start + 1 < len(data[row_idx]):
                fact_value = parse_number(data[row_idx][col_start + 1])
            if col_start + 2 < len(data[row_idx]):
                under_value = parse_number(data[row_idx][col_start + 2])
            if col_start + 3 < len(data[row_idx]):
                percent_value = parse_percent(data[row_idx][col_start + 3])
            records.append({
                'Дата': date_str,
                'Время': time_str,
                'Филиал': filial,
                'Площадка': platform,
                'Тип транспорта': 'Электробус',
                'План': plan_value,
                'Факт': fact_value,
                'Недовыпуск': under_value,
                '% выполнения плана': percent_value
            })

# -------------------------------
# 5. ОСНОВНАЯ ФУНКЦИЯ ПАРСИНГА
# -------------------------------
def parse_last_15_days():
    """Парсит данные за последние 15 дней"""
    print("Парсинг данных за последние 15 дней...")
    all_records = []
    try:
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        worksheets = spreadsheet.worksheets()
        date_pattern = r'^\d{2}\.\d{2}\.\d{4}$'
        date_sheets = []
        for ws in worksheets:
            if re.match(date_pattern, ws.title):
                try:
                    date_obj = datetime.strptime(ws.title, '%d.%m.%Y')
                    date_sheets.append((date_obj, ws))
                except:
                    continue
        date_sheets.sort(key=lambda x: x[0], reverse=True)
        target_sheets = date_sheets[:15]
        if not target_sheets:
            print("Не найдено листов с датами!")
            return pd.DataFrame()
        print(f"Найдено листов с датами: {len(date_sheets)}")
        print(f"Будет обработано: {len(target_sheets)} листов")
        for date_obj, worksheet in target_sheets:
            date_str = date_obj.strftime('%d.%m.%Y')
            print(f"\nОбработка листа: {date_str}")
            try:
                data = worksheet.get_all_values()
                if not data or len(data) < 50:
                    print(f"  Предупреждение: Мало данных в листе ({len(data)} строк)")
                    continue
                sheet_records = parse_sheet_by_date(data, date_str)
                all_records.extend(sheet_records)
                print(f"  Добавлено записей: {len(sheet_records)}")
            except Exception as e:
                print(f"  Ошибка обработки листа {date_str}: {e}")
                import traceback
                traceback.print_exc()
                continue
        print(f"\nВсего собрано записей: {len(all_records)}")
        if all_records:
            df = pd.DataFrame(all_records)
            columns_order = ['Дата', 'Время', 'Филиал', 'Площадка', 'Тип транспорта', 'План', 'Факт', 'Недовыпуск', '% выполнения плана']
            df = df[columns_order]
            return df
        else:
            print("Не удалось собрать данные, создаем демо-данные")
            return create_demo_data()
    except Exception as e:
        print(f"Ошибка при подключении к Google Sheets: {e}")
        print("Создаем демо-данные")
        return create_demo_data()

# -------------------------------
# 6. ДЕМО-ДАННЫЕ
# -------------------------------
def create_demo_data():
    """Создает демо-данные для тестирования"""
    print("Создание демо-данных...")
    records = []
    # Используем московское время для генерации
    now_msk = datetime.utcnow() + timedelta(hours=3)
    dates = [(now_msk - timedelta(days=i)).strftime('%d.%m.%Y') for i in range(15)]
    weekday_times = ['7:15', '8:30', '10:00', '11:00', '12:00', '13:00', '14:00',
                    '15:00', '16:00', '17:00', '18:00', '19:00', '20:00', '21:00']
    weekend_times = ['8:00', '10:00', '13:00', '17:00', '19:00', '21:00']
    bus_platforms = [
        ('СВ', 'Бибирево'), ('СВ', 'Марьина Роща'), ('СВ', 'Дангауэровка'), ('СВ', 'Гольяново'), ('СВ', 'Всего'),
        ('СЗ', 'Пресня'), ('СЗ', 'Мневники'), ('СЗ', 'Фили'), ('СЗ', 'Ховрино'), ('СЗ', 'Тушино'),
        ('СЗ', 'Красногорск'), ('СЗ', 'Зеленоградская'), ('СЗ', 'Всего'),
        ('Ю', 'Нагатино'), ('Ю', 'Андропова'), ('Ю', 'Домодедовская'), ('Ю', 'Коньково'),
        ('Ю', 'Очаково'), ('Ю', 'Ясенево'), ('Ю', 'Красная Пахра'), ('Ю', 'Чертаново'), ('Ю', 'Всего'),
        ('МГТ', 'Всего автобус')
    ]
    ebus_platforms = [
        ('СВ', 'Бибирево'), ('СВ', 'В.Лихоборы'), ('СВ', 'Останкино'), ('СВ', 'Новокосино'), ('СВ', 'Салтыковка'), ('СВ', 'Всего'),
        ('СЗ', 'Сокол'), ('СЗ', 'Фили'), ('СЗ', 'Митино'), ('СЗ', 'Всего'),
        ('Ю', 'Коньково'), ('Ю', 'Красная Пахра'), ('Ю', 'Нагорная'), ('Ю', 'Чертаново'), ('Ю', 'Всего'),
        ('МГТ', 'Всего электробус')
    ]
    for date_str in dates:
        date_obj = datetime.strptime(date_str, '%d.%m.%Y')
        is_weekend = date_obj.weekday() >= 5
        times = weekend_times if is_weekend else weekday_times
        for time_str in times:
            for filial, platform in bus_platforms:
                plan = np.random.randint(80, 150)
                percent_val = np.random.uniform(85, 100)
                fact = round(plan * percent_val / 100)
                under = plan - fact
                percent_str = "100%" if abs(percent_val - 100) < 0.1 else f"{round(percent_val, 1):.1f}".replace('.', ',') + '%'
                records.append({
                    'Дата': date_str,
                    'Время': time_str,
                    'Филиал': filial,
                    'Площадка': platform,
                    'Тип транспорта': 'Автобус',
                    'План': plan,
                    'Факт': fact,
                    'Недовыпуск': under,
                    '% выполнения плана': percent_str
                })
            for filial, platform in ebus_platforms:
                plan = np.random.randint(40, 100)
                percent_val = np.random.uniform(80, 100)
                fact = round(plan * percent_val / 100)
                under = plan - fact
                percent_str = "100%" if abs(percent_val - 100) < 0.1 else f"{round(percent_val, 1):.1f}".replace('.', ',') + '%'
                records.append({
                    'Дата': date_str,
                    'Время': time_str,
                    'Филиал': filial,
                    'Площадка': platform,
                    'Тип транспорта': 'Электробус',
                    'План': plan,
                    'Факт': fact,
                    'Недовыпуск': under,
                    '% выполнения плана': percent_str
                })
    df = pd.DataFrame(records)
    print(f"Создано демо-записей: {len(df)}")
    return df

# -------------------------------
# 7. СОХРАНЕНИЕ ДАННЫХ
# -------------------------------
def save_data_to_files(df, csv_filename='flat_table_correct.csv', xlsx_filename='flat_table_correct.xlsx'):
    """Сохраняет данные в CSV и XLSX форматах"""
    print(f"\nСохранение данных...")
    df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
    print(f"Файл сохранен: {csv_filename}")
    df.to_excel(xlsx_filename, index=False, engine='openpyxl')
    print(f"Файл сохранен: {xlsx_filename}")
    import zipfile
    with zipfile.ZipFile('parsed_data.zip', 'w') as zipf:
        zipf.write(csv_filename)
        zipf.write(xlsx_filename)
    print(f"Архив создан: parsed_data.zip")
    return csv_filename, xlsx_filename

# -------------------------------
# 8. ОСНОВНОЙ БЛОК ПАРСИНГА
# -------------------------------
print("\n" + "="*60)
print("ПАРСИНГ ДАННЫХ ИЗ GOOGLE SHEETS")
print("="*60)
if not DEMO_MODE:
    parsed_df = parse_last_15_days()
else:
    parsed_df = create_demo_data()
#csv_file, xlsx_file = save_data_to_files(parsed_df, 'flat_table_correct.csv', 'flat_table_correct.xlsx')
print(f"\nПример данных (первые 10 строк):")
print(parsed_df.head(10))
print(f"\nСтатистика данных:")
print(f"   Всего записей: {len(parsed_df)}")
print(f"   Уникальных дат: {parsed_df['Дата'].nunique()}")
print(f"   Уникальных времен: {parsed_df['Время'].nunique()}")

# ======================================================================
# ДАШБОРД - Часть 2 (С АВТОМАТИЧЕСКИМ ФИЛЬТРОМ ПО МСК И БЕЗ СВОРАЧИВАНИЯ ТАБЛИЦ)
# ======================================================================
# ШАГ 1: Подготовка данных для дашборда
#print("1. Загрузка flat_table_correct.csv...")
#df = pd.read_csv('flat_table_correct.csv', encoding='utf-8-sig')
df = parsed_df
print(f"   Загружено {len(df)} записей.")
# Преобразуем филиалы (СВ -> ФСВ)
filial_mapping = {'СВ': 'ФСВ', 'СЗ': 'ФСЗ', 'Ю': 'ФЮ'}
df['Филиал_код'] = df['Филиал'].map(filial_mapping).fillna(df['Филиал'])
# Используем готовое поле 'Тип транспорта' из CSV
df.rename(columns={'Тип транспорта': 'Тип'}, inplace=True)
# Создаем поле "Процент" (число) для совместимости с JS
df['Процент'] = df['% выполнения плана'].str.replace('%', '').str.replace(',', '.').astype(float)

# -------------------------------
# ОПРЕДЕЛЕНИЕ ФУНКЦИЙ
# -------------------------------
def calculate_summaries_for_dashboard(time_df):
    """Рассчитывает сводные данные."""
    filial_summary = []
    bus_by_filial = []
    ebus_by_filial = []
    unique_filials = ['ФСВ', 'ФСЗ', 'ФЮ']

    # ИСПРАВЛЕНИЕ: Рассчитываем суммы ПРАВИЛЬНО
    # Общие суммы по автобусам (ВСЕ автобусы, не только итоговые)
    total_bus_plan = time_df[(time_df['Тип'] == 'Автобус') & (~time_df['Площадка'].str.contains('Всего'))]['План'].sum()
    total_bus_fact = time_df[(time_df['Тип'] == 'Автобус') & (~time_df['Площадка'].str.contains('Всего'))]['Факт'].sum()

    # Общие суммы по электробусам (ВСЕ электробусы, не только итоговые)
    total_ebus_plan = time_df[(time_df['Тип'] == 'Электробус') & (~time_df['Площадка'].str.contains('Всего'))]['План'].sum()
    total_ebus_fact = time_df[(time_df['Тип'] == 'Электробус') & (~time_df['Площадка'].str.contains('Всего'))]['Факт'].sum()

    # Остальной код остается без изменений
    total_bus_under = time_df[(time_df['Тип'] == 'Автобус') & (time_df['Площадка'] == 'Всего')]['Недовыпуск'].sum()
    total_ebus_under = time_df[(time_df['Тип'] == 'Электробус') & (time_df['Площадка'] == 'Всего')]['Недовыпуск'].sum()

    total_bus_percent = (total_bus_fact / total_bus_plan * 100) if total_bus_plan > 0 else 0
    total_ebus_percent = (total_ebus_fact / total_ebus_plan * 100) if total_ebus_plan > 0 else 0

    total_mosgortrans_plan = total_bus_plan + total_ebus_plan
    total_mosgortrans_fact = total_bus_fact + total_ebus_fact
    total_mosgortrans_under = total_bus_under + total_ebus_under
    total_mosgortrans_percent = (total_mosgortrans_fact / total_mosgortrans_plan * 100) if total_mosgortrans_plan > 0 else 0

    # Остальной код функции (для филиалов) остается без изменений
    for filial_code in unique_filials:
        filial_data = time_df[time_df['Филиал_код'] == filial_code]
        bus_total_row = filial_data[(filial_data['Тип'] == 'Автобус') & (filial_data['Площадка'] == 'Всего')]
        ebus_total_row = filial_data[(filial_data['Тип'] == 'Электробус') & (filial_data['Площадка'] == 'Всего')]
        total_plan = 0
        total_fact = 0
        total_under = 0
        if not bus_total_row.empty:
            total_plan += bus_total_row.iloc[0]['План']
            total_fact += bus_total_row.iloc[0]['Факт']
            total_under += bus_total_row.iloc[0]['Недовыпуск']
        if not ebus_total_row.empty:
            total_plan += ebus_total_row.iloc[0]['План']
            total_fact += ebus_total_row.iloc[0]['Факт']
            total_under += ebus_total_row.iloc[0]['Недовыпуск']
        total_percent = (total_fact / total_plan * 100) if total_plan > 0 else 0
        filial_summary.append({
            'Дата': time_df.iloc[0]['Дата'],
            'Время': time_df.iloc[0]['Время'],
            'Филиал': filial_code,
            'Тип': 'Итого',
            'Процент': round(total_percent, 1),
            'Недовыпуск': total_under
        })
        if not bus_total_row.empty:
            row = bus_total_row.iloc[0]
            bus_by_filial.append({
                'Дата': row['Дата'],
                'Время': row['Время'],
                'Филиал': filial_code,
                'Тип': 'Автобус',
                'Процент': round(row['Процент'], 1),
                'Недовыпуск': row['Недовыпуск']
            })
        if not ebus_total_row.empty:
            row = ebus_total_row.iloc[0]
            ebus_by_filial.append({
                'Дата': row['Дата'],
                'Время': row['Время'],
                'Филиал': filial_code,
                'Тип': 'Электробус',
                'Процент': round(row['Процент'], 1),
                'Недовыпуск': row['Недовыпуск']
            })

    return {
        'filial_summary': filial_summary,
        'bus_by_filial': bus_by_filial,
        'ebus_by_filial': ebus_by_filial,
        'total_mosgortrans_percent': round(total_mosgortrans_percent, 1),
        'total_mosgortrans_under': int(total_mosgortrans_under),
        'total_bus_percent': round(total_bus_percent, 1),
        'total_bus_under': int(total_bus_under),
        'total_ebus_percent': round(total_ebus_percent, 1),
        'total_ebus_under': int(total_ebus_under),
        'total_mosgortrans_plan': total_mosgortrans_plan,
        'total_mosgortrans_fact': total_mosgortrans_fact
    }

def prepare_dashboard_data(df):
    """Подготавливает данные для дашборда"""
    print("2. Подготовка данных для дашборда...")
    dashboard_data = {}
    dates = sorted(df['Дата'].unique())
    for date_str in dates:
        date_data = {}
        date_df = df[df['Дата'] == date_str]
        times = sorted(date_df['Время'].unique())
        for time_str in times:
            time_df = date_df[date_df['Время'] == time_str]
            bus_sites = time_df[(time_df['Тип'] == 'Автобус') & (~time_df['Площадка'].str.contains('Всего'))].to_dict('records')
            ebus_sites = time_df[(time_df['Тип'] == 'Электробус') & (~time_df['Площадка'].str.contains('Всего'))].to_dict('records')
            summary_data = calculate_summaries_for_dashboard(time_df)
            date_data[time_str] = {
                'bus_sites': bus_sites,
                'ebus_sites': ebus_sites,
                **summary_data
            }
        dashboard_data[date_str] = date_data
    print(f"   Данные подготовлены: {len(dates)} дней")
    return dashboard_data

# -------------------------------
# ОСНОВНАЯ ЛОГИКА
# -------------------------------
dashboard_data = prepare_dashboard_data(df)

# ШАГ 2: ОПРЕДЕЛЕНИЕ АВТОМАТИЧЕСКИХ ФИЛЬТРОВ ПО МОСКОВСКОМУ ВРЕМЕНИ
# Получаем текущее время в MSK (UTC+3)
now_utc = datetime.utcnow()
now_msk = now_utc + timedelta(hours=3)
current_date_str = now_msk.strftime('%d.%m.%Y')
current_time_obj = now_msk.time()

target_date = current_date_str
target_time = ''

# Проверяем, есть ли сегодняшняя дата в dashboard_data
if current_date_str in dashboard_data:
    available_times = set(dashboard_data[current_date_str].keys())
    if '7:15' in available_times:
        day_type = 'weekday'
    elif '8:00' in available_times:
        day_type = 'weekend'
    else:
        day_type = 'weekday'

    if day_type == 'weekday':
        intervals = [
            (time(7,15), time(8,30), '7:15'),
            (time(8,30), time(10,0), '8:30'),
            (time(10,0), time(11,0), '10:00'),
            (time(11,0), time(12,0), '11:00'),
            (time(12,0), time(13,0), '12:00'),
            (time(13,0), time(14,0), '13:00'),
            (time(14,0), time(15,0), '14:00'),
            (time(15,0), time(16,0), '15:00'),
            (time(16,0), time(17,0), '16:00'),
            (time(17,0), time(18,0), '17:00'),
            (time(18,0), time(19,0), '18:00'),
            (time(19,0), time(20,0), '19:00'),
            (time(20,0), time(21,0), '20:00'),
            (time(21,0), time(22,0), '21:00'),
        ]
    else:
        intervals = [
            (time(8,0), time(10,0), '8:00'),
            (time(10,0), time(13,0), '10:00'),
            (time(13,0), time(17,0), '13:00'),
            (time(17,0), time(19,0), '17:00'),
            (time(19,0), time(21,0), '19:00'),
            (time(21,0), time(22,0), '21:00'),
        ]

    for start, end, label in intervals:
        if start <= current_time_obj < end:
            target_time = label
            break

    if not target_time and intervals:
        target_time = intervals[-1][2]

else:
    # Сегодняшней даты нет — используем последнюю доступную
    dates_sorted = sorted(dashboard_data.keys(), key=lambda d: datetime.strptime(d, '%d.%m.%Y'))
    if dates_sorted:
        target_date = dates_sorted[-1]
        all_times = sorted(dashboard_data[target_date].keys(), key=lambda t: list(map(int, t.split(':'))))
        target_time = all_times[0] if all_times else ''
    else:
        target_date = ''
        target_time = ''

# Получаем все даты и времена для выпадающих списков
dates = sorted(dashboard_data.keys(), key=lambda d: datetime.strptime(d, '%d.%m.%Y'))
all_times_set = {t for d in dashboard_data.values() for t in d.keys()}
times = sorted(all_times_set, key=lambda t: list(map(int, t.split(':'))))

# Генерация опций с автоматическим выбором
date_options = ''.join([f'<option value="{d}" {"selected" if d == target_date else ""}>{d}</option>' for d in dates])
time_options = ''.join([f'<option value="{t}" {"selected" if t == target_time else ""}>{t}</option>' for t in times])

filials = ['ФСВ', 'ФСЗ', 'ФЮ']
filial_options = ''.join([f'<option value="{f}">{f}</option>' for f in filials])

json_str = json.dumps(dashboard_data, ensure_ascii=False, default=str)


# ШАГ 3: Генерация HTML путем конкатенации

html_part1 = '''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Выпуск транспорта</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.0.0"></script>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; font-family: 'Arial', sans-serif; }
body { background-color: #f5f5f5; padding: 4px; color: #333; font-size: 11px; overflow-x: hidden; }
.dashboard-container { max-width: 100%; margin: 0 auto; }
.filters-panel { background: white; border-radius: 4px; padding: 8px; margin-bottom: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid #ddd; }
.filters-title { font-size: 12px; font-weight: bold; margin-bottom: 8px; color: #2c3e50; padding-bottom: 4px; border-bottom: 1px solid #eee; }
.filter-group { display: grid; grid-template-columns: 1fr; gap: 6px; }
.filter-item { width: 100%; }
.filter-label { display: block; margin-bottom: 2px; font-weight: bold; color: #555; font-size: 10px; }
.filter-select { width: 100%; padding: 5px 6px; border: 1px solid #ccc; border-radius: 3px; font-size: 11px; background: white; cursor: pointer; height: 28px; }
.tables-container { display: flex; flex-direction: column; gap: 8px; }
.table-section { background: white; border-radius: 4px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid #ddd; margin-bottom: 6px; }
.table-header { background: #2c3e50; color: white; padding: 6px 8px; font-size: 11px; font-weight: bold; text-align: center; }
.table-subtitle { padding: 4px 8px; background: #f8f9fa; border-bottom: 1px solid #eee; font-size: 9px; color: #666; display: flex; flex-wrap: wrap; gap: 8px; }
.date-info { font-weight: bold; color: #2c3e50; font-size: 9px; }
.table-wrapper { overflow-x: auto; padding: 0; -webkit-overflow-scrolling: touch; max-width: 100vw; }
table { width: 100%; border-collapse: collapse; font-size: 10px; min-width: 300px; table-layout: fixed; }
th { background-color: #f8f9fa; padding: 4px 3px; text-align: left; font-weight: bold; color: #495057; border-bottom: 1px solid #dee2e6; white-space: nowrap; font-size: 10px; position: sticky; top: 0; z-index: 10; }
td { padding: 3px 2px; border-bottom: 1px solid #e9ecef; vertical-align: middle; font-size: 10px; line-height: 1.1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
th:nth-child(1), td:nth-child(1) { width: 35px; min-width: 35px; max-width: 35px; padding-left: 3px; }
th:nth-child(2), td:nth-child(2) { width: 90px; min-width: 90px; max-width: 90px; }
th:nth-child(3), td:nth-child(3) { width: 45px; min-width: 45px; max-width: 45px; text-align: center; }
th:nth-child(4), td:nth-child(4) { width: 45px; min-width: 45px; max-width: 45px; text-align: center; }
table[id*="Filial"] th:nth-child(1), table[id*="Filial"] td:nth-child(1) { width: 55px; }
table[id*="Filial"] th:nth-child(2), table[id*="Filial"] td:nth-child(2) { width: 60px; }
table[id="mgtTable"] th:nth-child(1), table[id="mgtTable"] td:nth-child(1) { width: 70px; }
.percent-cell { font-weight: bold; padding: 1px 3px; border-radius: 2px; display: inline-block; min-width: 40px; text-align: center; font-size: 10px; line-height: 1.2; }
.percent-green { background-color: #e8f5e9; color: #2e7d32; border: 1px solid #c8e6c9; }
.percent-light-orange { background-color: #fffde7; color: #f9a825; border: 1px solid #ffa724; }
.percent-medium-red { background-color: #ffcdd2; color: #b71c1c; border: 1px solid #ef9a9a; }
.percent-dark-red { background-color: #b71c1c; color: white; border: 1px solid #8c0000; }
.under-cell { font-weight: bold; color: #c62828; font-size: 10px; text-align: center; }
.total-row { background-color: #f0f7ff; font-weight: bold; }
.total-row td { border-top: 1px solid #dee2e6; padding: 4px 3px; }
.no-data { text-align: center; padding: 15px 10px; color: #999; font-style: italic; font-size: 10px; }
.footer-info { margin-top: 10px; padding: 8px; text-align: center; color: #666; font-size: 9px; background: white; border-radius: 4px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid #ddd; }
@media (max-width: 360px) { body { padding: 2px; font-size: 10px; } .filters-panel { padding: 6px; } .table-header { padding: 5px 6px; font-size: 10px; } table { min-width: 280px; font-size: 9px; } th, td { font-size: 9px; padding: 2px 1px; } .percent-cell { min-width: 35px; font-size: 9px; padding: 1px 2px; } .filter-select { font-size: 10px; padding: 4px 5px; height: 26px; } }
@media (min-width: 768px) { body { padding: 8px; font-size: 12px; } .filters-panel { padding: 12px; border-radius: 6px; } .filter-group { grid-template-columns: repeat(3, 1fr); gap: 12px; } .filter-label { font-size: 11px; } .filter-select { font-size: 12px; height: 32px; padding: 6px 8px; } .table-header { font-size: 13px; padding: 8px 10px; } table { font-size: 11px; min-width: 350px; } th, td { font-size: 11px; padding: 5px 4px; } .percent-cell { min-width: 50px; font-size: 11px; } }
@media (min-width: 1024px) { .dashboard-container { max-width: 1000px; } body { padding: 12px; } .tables-container { display: grid; grid-template-columns: 1fr; gap: 12px; } .top-tables { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px; } .bottom-tables { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; } th:nth-child(1), td:nth-child(1) { width: 45px; } th:nth-child(2), td:nth-child(2) { width: 130px; } .percent-cell { min-width: 55px; } }
.top-tables, .bottom-tables { display: flex; flex-direction: column; gap: 8px; }
@media (min-width: 1024px) { .top-tables, .bottom-tables { flex-direction: row; } }
.chart-canvas { width: 100%; height: 160px; max-height: 40vh; }
</style>
</head>
<body>
<div class="dashboard-container">
<div class="filters-panel">
<div class="filters-title">ФИЛЬТРЫ</div>
<div class="filter-group">
<div class="filter-item">
<div class="filter-label">Дата</div>
<select class="filter-select" id="dateFilter">
'''
html_part2 = '''
</select>
</div>
<div class="filter-item">
<div class="filter-label">Время</div>
<select class="filter-select" id="timeFilter">
'''
html_part3 = '''
</select>
</div>
<div class="filter-item">
<div class="filter-label">Филиал</div>
<select class="filter-select" id="filialFilter">
<option value="all" selected>Все</option>
'''
html_part4 = '''
</select>
</div>
</div>
</div>
<div class="table-section" id="mgtCumulativeSection">
<div class="table-header">МГТ ИТОГО — НАКОПИТЕЛЬНЫЙ ГРАФИК</div>
<div class="table-subtitle">
<span class="date-info" id="mgtCumulativeDate">Дата: ''' + target_date + '''</span>
<span class="date-info" id="mgtCumulativeTime">Время: ''' + target_time + '''</span>
</div>
<div class="table-wrapper">
<canvas id="mgtCumulativeChart" class="chart-canvas"></canvas>
</div>
</div>
<div class="tables-container">
<div class="top-tables">
<div class="table-section" id="totalFilialSection">
<div class="table-header">ИТОГО МГТ</div>
<div class="table-subtitle">
<span class="date-info">Дата: ''' + target_date + '''</span>
<span class="date-info">Время: ''' + target_time + '''</span>
<span class="date-info">Филиал: Все</span>
</div>
<div class="table-wrapper">
<table id="totalFilialTable">
<thead>
<tr>
<th>Филиал</th>
<th>%</th>
<th id="totalTimeHeader">''' + target_time + '''</th>
</tr>
</thead>
<tbody id="totalFilialTableBody"></tbody>
</table>
</div>
</div>
<div class="table-section" id="busFilialSection">
<div class="table-header">ИТОГО АВТОБУСЫ МГТ</div>
<div class="table-subtitle">
<span class="date-info">Дата: ''' + target_date + '''</span>
<span class="date-info">Время: ''' + target_time + '''</span>
<span class="date-info">Филиал: Все</span>
</div>
<div class="table-wrapper">
<table id="busFilialTable">
<thead>
<tr>
<th>Филиал</th>
<th>%</th>
<th id="busFilialTimeHeader">''' + target_time + '''</th>
</tr>
</thead>
<tbody id="busFilialTableBody"></tbody>
</table>
</div>
</div>
<div class="table-section" id="ebusFilialSection">
<div class="table-header">ИТОГО ЭЛЕКТРОБУСЫ МГТ</div>
<div class="table-subtitle">
<span class="date-info">Дата: ''' + target_date + '''</span>
<span class="date-info">Время: ''' + target_time + '''</span>
<span class="date-info">Филиал: Все</span>
</div>
<div class="table-wrapper">
<table id="ebusFilialTable">
<thead>
<tr>
<th>Филиал</th>
<th>%</th>
<th id="ebusFilialTimeHeader">''' + target_time + '''</th>
</tr>
</thead>
<tbody id="ebusFilialTableBody"></tbody>
</table>
</div>
</div>
</div>
<div class="bottom-tables">
<div class="table-section" id="busSection">
<div class="table-header">АВТОБУСЫ</div>
<div class="table-subtitle">
<span class="date-info" id="busDateInfo">Дата: ''' + target_date + '''</span>
<span class="date-info" id="busTimeInfo">Время: ''' + target_time + '''</span>
<span class="date-info" id="busFilialInfo">Фил: Все</span>
</div>
<div class="table-wrapper">
<table id="busTable">
<thead>
<tr>
<th>Фил.</th>
<th>Площадка</th>
<th>%</th>
<th id="busTimeHeader">''' + target_time + '''</th>
</tr>
</thead>
<tbody id="busTableBody"></tbody>
</table>
</div>
</div>
<div class="table-section" id="ebusSection">
<div class="table-header">ЭЛЕКТРОБУСЫ</div>
<div class="table-subtitle">
<span class="date-info" id="ebusDateInfo">Дата: ''' + target_date + '''</span>
<span class="date-info" id="ebusTimeInfo">Время: ''' + target_time + '''</span>
<span class="date-info" id="ebusFilialInfo">Фил: Все</span>
</div>
<div class="table-wrapper">
<table id="ebusTable">
<thead>
<tr>
<th>Фил.</th>
<th>Площадка</th>
<th>%</th>
<th id="ebusTimeHeader">''' + target_time + '''</th>
</tr>
</thead>
<tbody id="ebusTableBody"></tbody>
</table>
</div>
</div>
</div>
</div>
<div class="footer-info">
Выпуск транспорта | Обновлено: ''' + now_msk.strftime('%d.%m.%Y %H:%M') + '''<br>
Данные: ''' + (dates[0] if dates else '') + ''' - ''' + (dates[-1] if dates else '') + '''
</div>
</div>
<script>
const dashboardData = ''' + json_str + ''';
function getPercentColor(percent) {
    if (percent === null || percent === undefined) return '';
    if (percent >= 100.0) return 'percent-green';
    if (percent >= 95.0) return 'percent-light-orange';
    if (percent >= 90.0) return 'percent-medium-red';
    return 'percent-dark-red';
}
function formatPercent(percent) {
    if (percent === null || percent === undefined) return '—';
    return percent.toFixed(1).replace('.', ',') + '%';
}
function formatUnder(under) {
    if (under === null || under === undefined) return '—';
    if (under > 0) return '-' + Math.abs(under);
    return under.toString();
}
let mgtCumulativeChart = null;
function updateAllTables() {
    const selectedDate = document.getElementById('dateFilter').value;
    const selectedTime = document.getElementById('timeFilter').value;
    const selectedFilial = document.getElementById('filialFilter').value;
    const filialDisplayText = selectedFilial === 'all' ? 'Все' : selectedFilial;
    document.getElementById('busDateInfo').textContent = 'Дата: ' + selectedDate;
    document.getElementById('busTimeInfo').textContent = 'Время: ' + selectedTime;
    document.getElementById('busFilialInfo').textContent = 'Фил: ' + filialDisplayText;
    document.getElementById('ebusDateInfo').textContent = 'Дата: ' + selectedDate;
    document.getElementById('ebusTimeInfo').textContent = 'Время: ' + selectedTime;
    document.getElementById('ebusFilialInfo').textContent = 'Фил: ' + filialDisplayText;
    document.querySelectorAll('[id$="TimeHeader"]').forEach(el => el.textContent = selectedTime);
    const sectionHeaders = document.querySelectorAll('.table-section .table-subtitle .date-info');
    for (let i = 0; i < sectionHeaders.length; i += 3) {
        if (i < sectionHeaders.length) sectionHeaders[i].textContent = 'Дата: ' + selectedDate;
        if (i+1 < sectionHeaders.length) sectionHeaders[i+1].textContent = 'Время: ' + selectedTime;
        if (i+2 < sectionHeaders.length) sectionHeaders[i+2].textContent = 'Филиал: ' + filialDisplayText;
    }
    if (!dashboardData[selectedDate] || !dashboardData[selectedDate][selectedTime]) {
        showNoData();
        return;
    }
    const timeData = dashboardData[selectedDate][selectedTime];
    updateTable('busTableBody', timeData.bus_sites, selectedFilial);
    updateTable('ebusTableBody', timeData.ebus_sites, selectedFilial);
    updateSummaryTable('totalFilialTableBody', timeData.filial_summary,
        timeData.total_mosgortrans_percent, timeData.total_mosgortrans_under, selectedFilial);
    updateSummaryTable('busFilialTableBody', timeData.bus_by_filial,
        timeData.total_bus_percent, timeData.total_bus_under, selectedFilial);
    updateSummaryTable('ebusFilialTableBody', timeData.ebus_by_filial,
        timeData.total_ebus_percent, timeData.total_ebus_under, selectedFilial);
    updateMgtCumulativeChart(selectedDate, selectedTime);
}
function updateTable(tableId, data, filialFilter) {
    let filteredData = data;
    if (filialFilter !== 'all') {
        const filialCode = filialFilter;
        filteredData = data.filter(item =>
            item.Филиал_код === filialCode || item.Филиал === filialFilter
        );
    }
    filteredData.sort((a, b) => (a.Процент || 0) - (b.Процент || 0));
    let html = '';
    if (filteredData.length === 0) {
        html = '<tr><td colspan="4" class="no-data">Нет данных</td></tr>';
    } else {
        filteredData.forEach(item => {
            const percentClass = getPercentColor(item.Процент);
            const percentHtml = `<span class="percent-cell ${percentClass}">${formatPercent(item.Процент)}</span>`;
            html += `<tr><td>${item.Филиал_код || '—'}</td><td class="platform-cell">${item.Площадка || '—'}</td><td>${percentHtml}</td><td class="under-cell">${formatUnder(item.Недовыпуск)}</td></tr>`;
        });
    }
    document.getElementById(tableId).innerHTML = html;
}
function updateSummaryTable(tableId, data, totalPercent, totalUnder, filialFilter) {
    let filteredData = data;
    if (filialFilter !== 'all') {
        filteredData = data.filter(item => item.Филиал === filialFilter);
    }
    filteredData.sort((a, b) => (a.Процент || 0) - (b.Процент || 0));
    let html = '';
    if (filteredData.length === 0) {
        html = '<tr><td colspan="3" class="no-data">Нет данных</td></tr>';
    } else {
        filteredData.forEach(item => {
            const percentClass = getPercentColor(item.Процент);
            html += `<tr><td>${item.Филиал || '—'}</td><td><span class="percent-cell ${percentClass}">${formatPercent(item.Процент)}</span></td><td class="under-cell">${formatUnder(item.Недовыпуск)}</td></tr>`;
        });
    }
    let totalLabel = '';
    if (tableId === 'busFilialTableBody') totalLabel = 'МГТ автобус';
    else if (tableId === 'ebusFilialTableBody') totalLabel = 'МГТ электробус';
    else totalLabel = 'МГТ итого';
    html += `<tr class="total-row"><td>${totalLabel}</td><td><span class="percent-cell ${getPercentColor(totalPercent)}">${formatPercent(totalPercent)}</span></td><td class="under-cell">${formatUnder(totalUnder)}</td></tr>`;
    document.getElementById(tableId).innerHTML = html;
}
function showNoData() {
    const tables = ['busTableBody','ebusTableBody','totalFilialTableBody','busFilialTableBody','ebusFilialTableBody'];
    tables.forEach(id => document.getElementById(id).innerHTML = '<tr><td colspan="10" class="no-data">Нет данных</td></tr>');
}
function updateMgtCumulativeChart(selectedDate, selectedTime) {
    const dayData = dashboardData[selectedDate];
    if (!dayData) return;
    const allTimes = Object.keys(dayData).sort((a, b) => {
        const [h1, m1] = a.split(':').map(Number);
        const [h2, m2] = b.split(':').map(Number);
        return h1 * 60 + m1 - (h2 * 60 + m2);
    });
    const isWeekday = allTimes.includes('7:15');
    let prevDate = null;
    const sortedDates = Object.keys(dashboardData).sort((a, b) => {
        const da = a.split('.').reverse().join('-');
        const db = b.split('.').reverse().join('-');
        return new Date(da) - new Date(db);
    });
    const currentIndex = sortedDates.indexOf(selectedDate);
    for (let i = currentIndex - 1; i >= 0; i--) {
        const d = sortedDates[i];
        const dData = dashboardData[d];
        const dTimes = Object.keys(dData);
        const dIsWeekday = dTimes.includes('7:15');
        if (dIsWeekday === isWeekday) {
            prevDate = d;
            break;
        }
    }
    const prevDayData = prevDate ? dashboardData[prevDate] : null;
    const [sh, sm] = selectedTime.split(':').map(Number);
    const selectedMinutes = sh * 60 + sm;
    const labels = [];
    const hourlyValues = [];
    const cumValues = [];
    const deltaValues = [];
    let cumulativeSumHourly = 0;
    let count = 0;
    allTimes.forEach(t => {
        const [h, m] = t.split(':').map(Number);
        const minutes = h * 60 + m;
        if (minutes <= selectedMinutes) {
            const timeData = dayData[t];
            const hourlyP = timeData.total_mosgortrans_percent || 0;
            cumulativeSumHourly += hourlyP;
            count++;
            const cumP = count > 0 ? cumulativeSumHourly / count : 0;
            let delta = 0;
            if (prevDayData && prevDayData[t]) {
                const prevP = prevDayData[t].total_mosgortrans_percent || 0;
                delta = hourlyP - prevP;
            }
            hourlyValues.push(hourlyP);
            cumValues.push(cumP);
            deltaValues.push(delta);
            labels.push(t);
        }
    });
    if (labels.length === 0) return;
    document.getElementById('mgtCumulativeDate').textContent = 'Дата: ' + selectedDate;
    document.getElementById('mgtCumulativeTime').textContent = 'Время: ' + labels[labels.length - 1];
    const ctx = document.getElementById('mgtCumulativeChart').getContext('2d');
    const maxValue = Math.max(...hourlyValues, ...cumValues);
    const yMax = maxValue + (maxValue * 0.06);
    const minDelta = Math.min(...deltaValues);
    const maxDelta = Math.max(...deltaValues);
    const yMaxDelta = maxDelta + 5;
    const yMinDelta = minDelta - 5;
    Chart.register(ChartDataLabels);
    if (mgtCumulativeChart) {
        mgtCumulativeChart.data.labels = labels;
        mgtCumulativeChart.data.datasets[0].data = hourlyValues;
        mgtCumulativeChart.data.datasets[1].data = cumValues;
        mgtCumulativeChart.data.datasets[2].data = deltaValues;
        mgtCumulativeChart.data.datasets[2].pointBackgroundColor = deltaValues.map(v => {if (v == null || isNaN(v)) return 'rgba(150,150,150,0.5)'; return v < 0 ? 'rgba(255, 99, 132, 0.5)' : 'rgba(23, 229, 44, 0.5)';});
        mgtCumulativeChart.options.scales.y.max = yMax;
        mgtCumulativeChart.options.scales.y1.min = yMinDelta;
        mgtCumulativeChart.options.scales.y1.max = yMaxDelta;
        mgtCumulativeChart.update();
    } else {
        mgtCumulativeChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [
                    {
                        type: 'bar',
                        label: 'Выпуск на час МГТ, %',
                        data: hourlyValues,
                        backgroundColor: 'rgba(160,160,160,0.6)',
                        borderColor: 'rgba(160,160,160,1)',
                        borderWidth: 1,
                        yAxisID: 'y',
                        datalabels: {
                            color: '#333',
                            anchor: 'end',
                            align: 'end',
                            offset: -6,
                            clamp: true,
                            font: {
                                size: window.innerWidth < 768 ? 7 : 8,
                                weight: 'bold'
                            },
                            rotation: (ctx) => (ctx.chart.data.labels.length > 7 && window.innerWidth < 768) ? -85 : 0,
                            formatter: (v) => v.toFixed(1).replace('.', ',') + '%'
                        }
                    },
                    {
                        type: 'bar',
                        label: 'Итог МГТ, %',
                        data: cumValues,
                        backgroundColor: 'rgba(54,162,235,0.6)',
                        borderColor: 'rgba(54,162,235,1)',
                        borderWidth: 1,
                        yAxisID: 'y',
                        datalabels: {
                            color: '#333',
                            anchor: 'end',
                            align: 'end',
                            offset: -6,
                            clamp: true,
                            font: {
                                size: window.innerWidth < 768 ? 7 : 8,
                                weight: 'bold'
                            },
                            rotation: (ctx) => (ctx.chart.data.labels.length > 7 && window.innerWidth < 768) ? -85 : 0,
                            formatter: (v) => v.toFixed(1).replace('.', ',') + '%'
                        }
                    },
                    {
                        type: 'line',
                        label: 'Динамика % vs вчера',
                        data: deltaValues,
                        borderColor: 'rgba(153,102,255,0.6)',
                        pointBackgroundColor: (ctx) => {
                            const v = ctx.parsed.y;
                            return (v !== null && !isNaN(v) && v < 0)
                                ? 'rgba(255, 99, 132, 1)'
                                : 'rgba(23, 229, 44, 1)';
                        },
                        backgroundColor: 'rgba(153,102,255,0.3)',
                        borderWidth: 1.5,
                        fill: false,
                        tension: 0.2,
                        pointRadius: 3,
                        yAxisID: 'y1',
                        datalabels: {
                            color: '#333',
                            anchor: 'end',
                            align: 'end',
                            offset: -6,
                            clamp: true,
                            font: {
                                size: window.innerWidth < 768 ? 7 : 8,
                                weight: 'bold'
                            },
                            formatter: (v) => v.toFixed(1).replace('.', ',') + '%'
                        }
                    }
                ]
            },
            options: {
                responsive: true,
                layout: { padding: { top: 25 } },
                plugins: {
                    legend: {
                        position: 'top',
                        labels: { font: { size: window.innerWidth < 768 ? 9 : 11 } }
                    }
                },
                scales: {
                    y: {
                        position: 'left',
                        max: yMax,
                        title: { display: true, text: '% выполнения плана', font: { size: window.innerWidth < 768 ? 9 : 11 } },
                        ticks: { font: { size: window.innerWidth < 768 ? 8 : 10 } }
                    },
                    y1: {
                        position: 'right',
                        min: yMinDelta,
                        max: yMaxDelta,
                        grid: { drawOnChartArea: false },
                        title: { display: true, text: 'Δ % vs вчера', font: { size: window.innerWidth < 768 ? 8 : 10 } },
                        ticks: { font: { size: window.innerWidth < 768 ? 7 : 9 } }
                    },
                    x: {
                        title: { display: true, text: 'Время', font: { size: window.innerWidth < 768 ? 9 : 11 } },
                        ticks: {
                            autoSkip: true,
                            maxTicksLimit: window.innerWidth < 768 ? 100 : 12,
                            font: { size: window.innerWidth < 768 ? 8 : 10 }
                        }
                    }
                }
            }
        });
    }
}
document.addEventListener('DOMContentLoaded', function() {
    document.getElementById('dateFilter').addEventListener('change', updateAllTables);
    document.getElementById('timeFilter').addEventListener('change', updateAllTables);
    document.getElementById('filialFilter').addEventListener('change', updateAllTables);
    updateAllTables();
});
</script>
</body>
</html>'''


final_html = (
    html_part1 + date_options + html_part2 + time_options + html_part3 +
    filial_options + html_part4
)



with open('index.html', 'w', encoding='utf-8') as f:
    f.write(final_html)

print("Файл создан.")
print("Текущее время: " + now_msk.strftime('%d.%m.%Y %H:%M'))
  
