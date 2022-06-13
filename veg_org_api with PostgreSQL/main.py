import traceback

from flask import Flask, request
import jsonschema
from jsonschema import validate
import psycopg2
from psycopg2 import Error
import re
from datetime import datetime

app = Flask(__name__)

# Connect to sc database

connection = psycopg2.connect(
    host="192.168.0.104",
    database="veg_org_prac",
    user="postgres",
    password="postgres",
    port=32345
)


def test_connection():
    try:
        valid = "cannot connect"
        # Create a cursor to perform database operations
        cursor = connection.cursor()
        # Print PostgreSQL details
        print("PostgreSQL server information")
        print(connection.get_dsn_parameters(), "\n")
        # Executing a SQL query
        cursor.execute("SELECT version();")
        # Fetch result
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")
        valid = "connected"

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
        valid = "cannot connect"
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
    return valid


# POST sample input
#input_veg = {"veg_type":"葉菜類", "veg_seasonal":"冬季", "veg_name": "西蘭花", "veg_pic_url":"https://www.vmo.org/uploads/vegetables/big/8.jpg"}
#input_price = {"veg_name":"豆角乾(2兩)", "veg_date":"2022-04-27", "veg_temperature":27, "veg_rainfall":0.4, "avg_px_per_catty":0.5, "avg_px_per_KG":0.6}

# regex
regex_url = '(http)?s?:?(\\/\\/[^"\']*\\.(?:png|jpg))'
regex_date = '^\\d{4}\\-(0[1-9]|1[012])\\-(0[1-9]|[12][0-9]|3[01])$'
regex_chinese = r'^[\u4E00-\u9FFF_0-9_()]+$'
regex_float = '([0-9]+\\.?[0-9]*|\\.[0-9]+)$'

# wholesale_veg schema
wholesale_veg_schema = {
    "type": "object",
    "properties": {
        'veg_type': {'type': 'string', 'pattern': regex_chinese},
        'veg_seasonal': {'type': 'string', 'pattern': regex_chinese},
        'veg_name': {'type': 'string', 'pattern': regex_chinese},
        'veg_pic_url': {'type': 'string', 'format': 'uri', 'pattern': regex_url},
    },
    "required": ['veg_name']
}

# price schema
price_schema = {
    "type": "object",
    "properties": {
        'veg_name': {'type': 'string', 'pattern': regex_chinese},
        'veg_date': {'type': 'string', 'format': 'date', 'pattern': regex_date},
        'veg_temperature': {'type': 'integer'},
        'veg_rainfall': {'type': 'number'},
        'avg_px_per_catty': {'type': 'number'},
        'avg_px_per_KG': {'type': 'number'}
    },
    "required": ['veg_name', 'veg_date', 'avg_px_per_catty', 'avg_px_per_KG']
}

# json schema validation
def validateJson(jsonData, schema):
    try:
        validate(instance=jsonData, schema=schema)
    except jsonschema.exceptions.ValidationError as err:
        return False
    return True


@app.route('/')
def test():
    check_conn = test_connection()
    return f"Veg_Marketing_Org_Practice: {check_conn}"

# POST each wholesale_veg
@app.route('/wholesale_veg', methods=['POST'])
def create_veg():
    req_data = request.get_json()
    is_valid = validateJson(req_data, wholesale_veg_schema)
    if is_valid:
        new_veg = {
            'veg_type': req_data['veg_type'],
            'veg_seasonal': req_data['veg_seasonal'],
            'veg_name': req_data['veg_name'],
            'veg_pic_url': req_data['veg_pic_url'],
        }
        return new_veg

# find the veg_name with veg_id
def get_veg_id(cursor, veg_name):
    try:
        select_veg_name_query = f"SELECT veg_id from wholesale_veg wv WHERE veg_name = \'{veg_name}\';"
        cursor.execute(select_veg_name_query)
        selected_veg_id = cursor.fetchone()[0]
        return selected_veg_id
    except Exception as error:
        print(
            f'get_veg_id method error: Cannot found veg_id from veg_name({veg_name}) in the wholesale_veg table, {error}')
        logging_error()
        return

# checking the prices_test table for row existence, true for nothing
# found, false for exist
def track_exists_row(cursor, table_to_check,
                     veg_id_check, veg_date_check, row):
    try:
        check_exist_query = f"SELECT EXISTS(SELECT 1 FROM {table_to_check} where veg_id = {veg_id_check} and veg_date = \'{veg_date_check}\'); "
        cursor.execute(check_exist_query)
        check_exist = cursor.fetchone()[0]
        return check_exist
    except Exception as error:
        logging_error()
        return print(f'track_exists_row method error: {error}')


# find the next price_table_id
def next_price_table_id(cursor, input_table_name, input_column_name):
    select_price_table_id_query = f'SELECT MAX({input_table_name}.{input_column_name}) from {input_table_name};'
    try:
        cursor.execute(select_price_table_id_query)
    except Exception as error:
        print(f'next_price_table_id method error: {error}')
        logging_error()
        return
    else:
        #print(cursor.fetchall())
        max_price_table_id = cursor.fetchall()[0][0]

    if max_price_table_id is None:
        max_price_table_id = 0
    return max_price_table_id + 1


@app.route('/price_day', methods=['POST'])
def input_price_day():
    # check if it can get json
    try:
        req_data = request.get_json()
        all_data_list = req_data['data']
    except Exception as error:
        print(f'json input error: {error}')
        logging_error()
        return

    with connection.cursor() as cursor:
        # choose the table to be input
        input_table_name = 'price'
        input_column_name = 'price_table_id'

        all_data_list_range = len(all_data_list) - 1

        try:
            for i in range(all_data_list_range):

                # loop through all data and input into db
                # the number of row of the data got
                nth_row_data_list = all_data_list[i]
                veg_name = nth_row_data_list[1]
                veg_date = nth_row_data_list[0]
                # veg_temperature = req_data['veg_temperature']
                # veg_rainfall = req_data['veg_rainfall']
                avg_px_per_catty_get = nth_row_data_list[2]
                avg_px_per_KG_get = nth_row_data_list[3]
                # print(type(veg_name), type(veg_date), type(avg_px_per_catty), type(avg_px_per_KG))
                avg_px_per_catty = avg_px_per_catty_get.replace('$', '')
                avg_px_per_KG = avg_px_per_KG_get.replace('$', '')
                if veg_name[:2] == '枇杷':
                    veg_name = '枇杷'

                # format assertion
                assert re.match(regex_chinese, veg_name), f"Row {i} error: veg_name should be in Chinese, veg_name: {veg_name}"
                assert re.match(regex_date, veg_date), f"Row {i} error: veg_date should be in YYYY-MM-DD format, veg_date: {veg_date}"
                assert re.match(regex_float, avg_px_per_catty), f'Row {i} error: avg_px_per_catty should be in \'$XX.X\' format, avg_px_per_catty: {avg_px_per_catty}'
                assert re.match(regex_float, avg_px_per_KG), f'Row {i} error: avg_px_per_KG should be in \'$XX.X\' format, avg_px_per_KG: {avg_px_per_KG}'

                selected_veg_id = get_veg_id(cursor, veg_name)
                assert selected_veg_id is not None, f'veg_id cannot be found, check the veg_name'

                timezone = 'SET timezone = +8;'
                cursor.execute(timezone)

                status = 'waiting'

                if track_exists_row(cursor, input_table_name,
                                    selected_veg_id, veg_date, i) is False:

                    # if required row not exist
                    new_price_table_id = next_price_table_id(
                            cursor, input_table_name, input_column_name)

                    insert_query = f'INSERT INTO {input_table_name}' \
                        f'({input_column_name}, veg_id, veg_date, avg_px_per_catty, avg_px_per_kg, last_updated_time)' \
                        f'VALUES({new_price_table_id}, {selected_veg_id}, \'{veg_date}\', {avg_px_per_catty}, {avg_px_per_KG}, CURRENT_TIMESTAMP);'

                    status = 'insert'

                else:
                    # if required row exist
                    insert_query = f'UPDATE {input_table_name} ' \
                        f'SET avg_px_per_catty = {avg_px_per_catty}, avg_px_per_kg = {avg_px_per_KG}, last_updated_time = CURRENT_TIMESTAMP ' \
                        f'WHERE veg_id = {selected_veg_id} and veg_date = \'{veg_date}\';'

                    status = 'update'

                cursor.execute(insert_query)
                connection.commit()
                print(insert_query)
                db_log = f"$$success, row: {i}, status: {status}, veg_id: {selected_veg_id}, veg_date:{veg_date}, avg_px_per_catty: {avg_px_per_catty}, avg_px_per_kg: {avg_px_per_KG}, python_time: {datetime.now()}$$"
                print(db_log)
                data_log(cursor, db_log)

        except Exception as error:
            connection.rollback()
            db_log = f"$$failed, row: {i}, python_time: {datetime.now()}, error: {error}$$"

            print(db_log)
            data_log(cursor, db_log)
            logging_error()


    return 'finished'


def logging_error():
    errlog = open('error_log.txt', 'a')
    errlog.write(traceback.format_exc())
    errlog.close()
    print('written into error log: error_log.txt')

def data_log(cursor, print_data=''):
    try:
        # log all input data, both succeed or failed
        new_data_log_id = next_price_table_id(cursor, 'data_log', 'data_log_id')
        insert_query = f'INSERT INTO data_log (data_log_id, status_and_data, last_updated_time)' \
        f'VALUES ({new_data_log_id}, {print_data}, CURRENT_TIMESTAMP);'
        cursor.execute(insert_query)
        connection.commit();

    except Exception as error:
        print(f'data_log error: {error}')
        logging_error()

#print(next_price_table_id(connection.cursor(), 'data_log', 'data_log_id'))
#print(data_log(connection.cursor()))


if __name__ == "__main__":
    app.run(debug=True)
