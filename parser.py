import struct
import sqlite3
import logging
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(stream=sys.stdout)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def parse_sensor_data(hex_data):
    hex_pairs = [hex_data[i:i + 8] for i in range(0, len(hex_data), 8)]
    logger.info('подключение к БД...')
    conn = sqlite3.connect('sensor_data.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sensor_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            current_value_counter INTEGER,
            pressure_value REAL,
            status INTEGER
        )
    ''')
    for hex_pair in hex_pairs:
        try:
            status, current_value_counter, pressure_value = struct.unpack('>BBH', bytes.fromhex(hex_pair))
            if status == 0x80:
                current_value_counter &= 0x7F
                if 0 <= current_value_counter <= 0x7F:
                    cursor.execute(
                        'INSERT INTO sensor_data (current_value_counter, pressure_value, status) VALUES (?, ?, ?)',
                        (current_value_counter,
                        pressure_value/ 100.0, status))
                    logger.info('успешно выполнена запись данных в БД')
        except Exception as e:
            logger.error('получены неверные данные')
    conn.commit()
    conn.close()


if __name__ == '__main__':
    try:
        with open('data.txt', 'r', encoding='utf-8') as file:
            hex_data = file.readline()
            logger.info('данные прочитаны из файла')

    except Exception:
        hex_data = '34ffffff80490000804a0000804b0000804c0000804d000079f3ffff'
        logger.error('не получилось загрузить данные из файла, программа выполнится на тестовом примере')
    parse_sensor_data(hex_data)
