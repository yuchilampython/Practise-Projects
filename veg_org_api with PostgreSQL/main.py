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


# Connect to localhost database
'''connection = psycopg2.connect(
    host="localhost",
    database="vege_org",
    user="postgres",
    password="DataEngineer123",
    port=5432
)'''


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
def input_json():
    input_json_price = {"data": [["2022-05-05",
                              "白苦瓜",
                              "$13.5",
                              "$13.5"],
                             ["2022-05-05",
                              "菜芯仔",
                              "$8.5",
                              "$7.2"],
                             ["2022-05-05",
                              "超甜粟",
                              "$11.9",
                              "$12.7"],
                             ["2022-05-05",
                              "南瓜",
                              "$6.3",
                              "$6.7"],
                             ["2022-05-05",
                              "粉刈",
                              "$10.8",
                              "$11.0"],
                             ["2022-05-05",
                              "皇帝菜",
                              "$13.0",
                              "$10.5"],
                             ["2022-05-05",
                              "蒜頭",
                              "$12.9",
                              "$13.4"],
                             ["2022-05-05",
                              "菜苗",
                              "$9.2",
                              "$7.5"],
                             ["2022-05-05",
                              "紅蘿蔔仔",
                              "$5.3",
                              "$5.9"],
                             ["2022-05-05",
                              "豆杯",
                              "$33.0",
                              "$33.4"],
                             ["2022-05-05",
                              "芋仔",
                              "$9.2",
                              "$9.4"],
                             ["2022-05-05",
                              "芥菜",
                              "$4.0",
                              "$4.1"],
                             ["2022-05-05",
                              "蘿蔔苗",
                              "$3.5",
                              "$3.5"],
                             ["2022-05-05",
                              "干蔥肉",
                              "$7.8",
                              "$7.8"],
                             ["2022-05-05",
                              "溫室青瓜",
                              "$13.0",
                              "$11.5"],
                             ["2022-05-05",
                              "薑",
                              "$9.6",
                              "$9.0"],
                             ["2022-05-05",
                              "金針菇",
                              "$12.0",
                              "$9.0"],
                             ["2022-05-05",
                              "馬尾仔",
                              "$2.0",
                              "$2.0"],
                             ["2022-05-05",
                              "人參",
                              "$83.3",
                              "$83.3"],
                             ["2022-05-05",
                              "苦瓜",
                              "$10.1",
                              "$10.8"],
                             ["2022-05-05",
                              "海鮮菇",
                              "$8.5",
                              "$13.6"],
                             ["2022-05-05",
                              "香水瓜",
                              "$6.3",
                              "$6.1"],
                             ["2022-05-05",
                              "王瓜",
                              "$5.4",
                              "$6.1"],
                             ["2022-05-05",
                              "菜芯",
                              "$8.1",
                              "$7.3"],
                             ["2022-05-05",
                              "甜椒",
                              "$15.4",
                              "$13.3"],
                             ["2022-05-05",
                              "日本小南瓜",
                              "$7.5",
                              "$7.5"],
                             ["2022-05-05",
                              "珍珠筍",
                              "$8.0",
                              "$10.7"],
                             ["2022-05-05",
                              "蘆薈",
                              "$4.1",
                              "$2.4"],
                             ["2022-05-05",
                              "青白芯",
                              "$7.2",
                              "$7.3"],
                             ["2022-05-05",
                              "火炭毛",
                              "$1.0",
                              "$1.0"],
                             ["2022-05-05",
                              "京蔥",
                              "$10.9",
                              "$10.9"],
                             ["2022-05-05",
                              "枸杞",
                              "$6.4",
                              "$12.5"],
                             ["2022-05-05",
                              "蛇舌草",
                              "$17.2",
                              "$16.6"],
                             ["2022-05-05",
                              "蓮葉",
                              "$20.0",
                              "$18.0"],
                             ["2022-05-05",
                              "馬尾白",
                              "$7.0",
                              "$7.0"],
                             ["2022-05-05",
                              "牛蒡",
                              "$26.5",
                              "$14.1"],
                             ["2022-05-05",
                              "紫蘇葉",
                              "$7.2",
                              "$8.2"],
                             ["2022-05-05",
                              "珍珠菜",
                              "$7.9",
                              "$6.8"],
                             ["2022-05-05",
                              "荷荳",
                              "$10.4",
                              "$11.5"],
                             ["2022-05-05",
                              "木薯",
                              "$3.0",
                              "$2.8"],
                             ["2022-05-05",
                              "肉豆",
                              "$10.4",
                              "$10.5"],
                             ["2022-05-05",
                              "番薯",
                              "$7.5",
                              "$8.0"],
                             ["2022-05-05",
                              "春菜",
                              "$4.4",
                              "$4.4"],
                             ["2022-05-05",
                              "韭菜",
                              "$7.8",
                              "$7.7"],
                             ["2022-05-05",
                              "香茅",
                              "$22.0",
                              "$19.2"],
                             ["2022-05-05",
                              "紅洋蔥",
                              "$4.6",
                              "$4.7"],
                             ["2022-05-05",
                              "菠菜苗",
                              "$21.6",
                              "$20.9"],
                             ["2022-05-05",
                              "油麥頭",
                              "$6.5",
                              "$6.5"],
                             ["2022-05-05",
                              "銀絲菜",
                              "$4.0",
                              "$4.0"],
                             ["2022-05-05",
                              "馬蹄",
                              "$12.0",
                              "$12.0"],
                             ["2022-05-05",
                              "西蒜",
                              "$3.0",
                              "$3.0"],
                             ["2022-05-05",
                              "君達菜",
                              "$12.5",
                              "$12.5"],
                             ["2022-05-05",
                              "蘆筍",
                              "$29.6",
                              "$20.1"],
                             ["2022-05-05",
                              "紅蘿蔔",
                              "$5.7",
                              "$5.4"],
                             ["2022-05-05",
                              "梅菜",
                              "$12.0",
                              "$12.0"],
                             ["2022-05-05",
                              "竹蔗根",
                              "$9.6",
                              "$9.6"],
                             ["2022-05-05",
                              "紅椰菜",
                              "$6.0",
                              "$5.8"],
                             ["2022-05-05",
                              "西椒",
                              "$9.2",
                              "$9.4"],
                             ["2022-05-05",
                              "南薑",
                              "$15.0",
                              "$15.0"],
                             ["2022-05-05",
                              "鮑魚菇",
                              "$50.0",
                              "$50.0"],
                             ["2022-05-05",
                              "白茄",
                              "$11.0",
                              "$11.6"],
                             ["2022-05-05",
                              "紅尖椒",
                              "$15.4",
                              "$17.0"],
                             ["2022-05-05",
                              "菱角",
                              "$4.0",
                              "$4.0"],
                             ["2022-05-05",
                              "芫茜",
                              "$16.2",
                              "$13.3"],
                             ["2022-05-05",
                              "鶴藪仔",
                              "$7.9",
                              "$8.9"],
                             ["2022-05-05",
                              "土芹",
                              "$9.2",
                              "$9.4"],
                             ["2022-05-05",
                              "蒜心",
                              "$21.6",
                              "$20.5"],
                             ["2022-05-05",
                              "蒲瓜",
                              "$5.4",
                              "$3.9"],
                             ["2022-05-05",
                              "青白仔",
                              "$6.5",
                              "$6.6"],
                             ["2022-05-05",
                              "茄瓜",
                              "$6.6",
                              "$6.8"],
                             ["2022-05-05",
                              "白通菜",
                              "$7.1",
                              "$7.1"],
                             ["2022-05-05",
                              "江門芯",
                              "$9.4",
                              "$9.4"],
                             ["2022-05-05",
                              "雞腿菇",
                              "$6.2",
                              "$19.4"],
                             ["2022-05-05",
                              "白椰",
                              "$2.9",
                              "$2.9"],
                             ["2022-05-05",
                              "貢菜",
                              "$25.0",
                              "$25.0"],
                             ["2022-05-05",
                              "蓮子",
                              "$4.8",
                              "$4.8"],
                             ["2022-05-05",
                              "紅菜頭",
                              "$6.4",
                              "$6.8"],
                             ["2022-05-05",
                              "夜香花",
                              "$11.5",
                              "$15.3"],
                             ["2022-05-05",
                              "白豆",
                              "$10.9",
                              "$11.3"],
                             ["2022-05-05",
                              "薄荷葉",
                              "$11.2",
                              "$11.1"],
                             ["2022-05-05",
                              "臭草",
                              "$31.2",
                              "$28.1"],
                             ["2022-05-05",
                              "芥菜仔",
                              "$1.7",
                              "$2.3"],
                             ["2022-05-05",
                              "草菇",
                              "$32.0",
                              "$32.0"],
                             ["2022-05-05",
                              "塔菜",
                              "$23.0",
                              "$23.0"],
                             ["2022-05-05",
                              "江門仔",
                              "$9.0",
                              "$9.0"],
                             ["2022-05-05",
                              "小青瓜",
                              "$12.0",
                              "$11.9"],
                             ["2022-05-05",
                              "番茄",
                              "$7.2",
                              "$7.4"],
                             ["2022-05-05",
                              "江門白",
                              "$7.0",
                              "$7.0"],
                             ["2022-05-05",
                              "馬莧菜",
                              "$6.9",
                              "$7.8"],
                             ["2022-05-05",
                              "田菜",
                              "$6.9",
                              "$6.2"],
                             ["2022-05-05",
                              "潺茄",
                              "$13.9",
                              "$14.7"],
                             ["2022-05-05",
                              "半邊蓮",
                              "$15.0",
                              "$11.7"],
                             ["2022-05-05",
                              "青通菜",
                              "$5.5",
                              "$6.1"],
                             ["2022-05-05",
                              "鮮冬菇",
                              "$23.0",
                              "$23.0"],
                             ["2022-05-05",
                              "沙薑",
                              "$20.0",
                              "$18.2"],
                             ["2022-05-05",
                              "白瓜",
                              "$6.1",
                              "$6.8"],
                             ["2022-05-05",
                              "茶樹菇",
                              "$180.0",
                              "$180.0"],
                             ["2022-05-05",
                              "紅生菜",
                              "$4.0",
                              "$4.0"],
                             ["2022-05-05",
                              "淮山",
                              "$11.8",
                              "$11.2"],
                             ["2022-05-05",
                              "指天椒",
                              "$44.6",
                              "$31.3"],
                             ["2022-05-05",
                              "益母草",
                              "$9.7",
                              "$6.8"],
                             ["2022-05-05",
                              "毛瓜",
                              "$6.2",
                              "$6.0"],
                             ["2022-05-05",
                              "冬瓜",
                              "$5.8",
                              "$5.9"],
                             ["2022-05-05",
                              "真姬菇",
                              "$5.5",
                              "$5.5"],
                             ["2022-05-05",
                              "豬肚菇",
                              "$9.0",
                              "$9.0"],
                             ["2022-05-05",
                              "粟鬚",
                              "$6.7",
                              "$6.8"],
                             ["2022-05-05",
                              "木瓜",
                              "$4.4",
                              "$4.4"],
                             ["2022-05-05",
                              "南瓜苗",
                              "$5.0",
                              "$4.0"],
                             ["2022-05-05",
                              "芽菜",
                              "$7.3",
                              "$7.7"],
                             ["2022-05-05",
                              "黑粟米",
                              "$10.0",
                              "$10.0"],
                             ["2022-05-05",
                              "淮山薯",
                              "$2.0",
                              "$2.0"],
                             ["2022-05-05",
                              "韭王",
                              "$13.5",
                              "$13.5"],
                             ["2022-05-05",
                              "薯仔",
                              "$5.2",
                              "$4.9"],
                             ["2022-05-05",
                              "甜荳",
                              "$12.2",
                              "$12.8"],
                             ["2022-05-05",
                              "青蘿蔔",
                              "$6.1",
                              "$6.1"],
                             ["2022-05-05",
                              "九牙生菜",
                              "$3.0",
                              "$3.0"],
                             ["2022-05-05",
                              "蚌花",
                              "$12.0",
                              "$12.0"],
                             ["2022-05-05",
                              "魚香",
                              "$2.0",
                              "$2.0"],
                             ["2022-05-05",
                              "鶴芯仔",
                              "$2.5",
                              "$5.3"],
                             ["2022-05-05",
                              "生菜",
                              "$4.9",
                              "$5.0"],
                             ["2022-05-05",
                              "枇杷",
                              "$3.0",
                              "$4.0"],
                             ["2022-05-05",
                              "青豆",
                              "$16.6",
                              "$16.1"],
                             ["2022-05-05",
                              "菊花",
                              "$14.0",
                              "$14.0"],
                             ["2022-05-05",
                              "芥芯",
                              "$6.0",
                              "$6.0"],
                             ["2022-05-05",
                              "芋頭",
                              "$15.0",
                              "$12.5"],
                             ["2022-05-05",
                              "白菜",
                              "$6.1",
                              "$5.6"],
                             ["2022-05-05",
                              "蘑菇",
                              "$47.0",
                              "$86.0"],
                             ["2022-05-05",
                              "胡蘆瓜",
                              "$5.0",
                              "$6.0"],
                             ["2022-05-05",
                              "干蔥頭",
                              "$7.3",
                              "$7.4"],
                             ["2022-05-05",
                              "魚翅瓜",
                              "$8.0",
                              "$8.0"],
                             ["2022-05-05",
                              "青瓜",
                              "$5.1",
                              "$5.0"],
                             ["2022-05-05",
                              "王菜",
                              "$5.8",
                              "$5.4"],
                             ["2022-05-05",
                              "筍",
                              "$6.1",
                              "$5.7"],
                             ["2022-05-05",
                              "包芥",
                              "$7.3",
                              "$7.3"],
                             ["2022-05-05",
                              "蔥",
                              "$8.3",
                              "$8.2"],
                             ["2022-05-05",
                              "白果",
                              "$26.0",
                              "$26.0"],
                             ["2022-05-05",
                              "意生菜",
                              "$5.1",
                              "$5.1"],
                             ["2022-05-05",
                              "王菜芯",
                              "$7.0",
                              "$7.0"],
                             ["2022-05-05",
                              "潺菜",
                              "$4.6",
                              "$4.8"],
                             ["2022-05-05",
                              "栗子",
                              "$35.0",
                              "$33.6"],
                             ["2022-05-05",
                              "西生菜",
                              "$6.3",
                              "$5.4"],
                             ["2022-05-05",
                              "紅棗",
                              "$45.0",
                              "$45.0"],
                             ["2022-05-05",
                              "羊角豆",
                              "$28.0",
                              "$28.0"],
                             ["2022-05-05",
                              "竹蔗",
                              "$6.2",
                              "$6.4"],
                             ["2022-05-05",
                              "苦瓜苗",
                              "$2.0",
                              "$2.0"],
                             ["2022-05-05",
                              "洋沖",
                              "$5.1",
                              "$4.9"],
                             ["2022-05-05",
                              "鶴藪白",
                              "$7.2",
                              "$7.9"],
                             ["2022-05-05",
                              "豆角乾(2兩)",
                              "$9.2",
                              "$9.2"],
                             ["2022-05-05",
                              "白芯",
                              "$5.0",
                              "$6.8"],
                             ["2022-05-05",
                              "茅根",
                              "$26.0",
                              "$25.8"],
                             ["2022-05-05",
                              "絲瓜",
                              "$8.2",
                              "$8.2"],
                             ["2022-05-05",
                              "鶴藪芯",
                              "$11.9",
                              "$8.7"],
                             ["2022-05-05",
                              "莧菜",
                              "$6.2",
                              "$5.5"],
                             ["2022-05-05",
                              "芯薯",
                              "$12.0",
                              "$8.0"],
                             ["2022-05-05",
                              "反茜",
                              "$14.7",
                              "$13.9"],
                             ["2022-05-05",
                              "田草",
                              "$7.9",
                              "$6.6"],
                             ["2022-05-05",
                              "合掌瓜",
                              "$5.8",
                              "$6.0"],
                             ["2022-05-05",
                              "秀珍菇",
                              "$19.0",
                              "$18.5"],
                             ["2022-05-05",
                              "靈芝菇",
                              "$28.3",
                              "$28.5"],
                             ["2022-05-05",
                              "紅莧菜",
                              "$6.5",
                              "$5.7"],
                             ["2022-05-05",
                              "蓮藕",
                              "$12.3",
                              "$11.8"],
                             ["2022-05-05",
                              "毛豆",
                              "$11.7",
                              "$11.7"],
                             ["2022-05-05",
                              "檸葉",
                              "$2.0",
                              "$2.0"],
                             ["2022-05-05",
                              "麻葉",
                              "$11.0",
                              "$14.0"],
                             ["2022-05-05",
                              "蕉蕾",
                              "$3.5",
                              "$3.1"],
                             ["2022-05-05",
                              "茨苗",
                              "$5.5",
                              "$5.8"],
                             ["2022-05-05",
                              "西蘭花",
                              "$13.4",
                              "$12.8"],
                             ["2022-05-05",
                              "茼蒿",
                              "$7.0",
                              "$7.0"],
                             ["2022-05-05",
                              "娃娃菜",
                              "$8.7",
                              "$8.8"],
                             ["2022-05-05",
                              "鮮百合",
                              "$80.0",
                              "$42.5"],
                             ["2022-05-05",
                              "油麥菜",
                              "$5.1",
                              "$4.5"],
                             ["2022-05-05",
                              "青白菜",
                              "$6.6",
                              "$5.4"],
                             ["2022-05-05",
                              "青椰",
                              "$4.4",
                              "$3.9"],
                             ["2022-05-05",
                              "羅馬生菜",
                              "$5.0",
                              "$4.7"],
                             ["2022-05-05",
                              "芥蘭",
                              "$6.4",
                              "$6.7"],
                             ["2022-05-05",
                              "白蘿蔔",
                              "$4.0",
                              "$4.0"],
                             ["2022-05-05",
                              "椰菜花",
                              "$6.1",
                              "$6.1"],
                             ["2022-05-05",
                              "芥蘭仔",
                              "$6.0",
                              "$7.2"],
                             ["2022-05-05",
                              "青尖椒",
                              "$7.1",
                              "$7.0"],
                             ["2022-05-05",
                              "津菜",
                              "$5.9",
                              "$6.2"],
                             ["2022-05-05",
                              "四季豆",
                              "$15.9",
                              "$16.6"],
                             ["2022-05-05",
                              "白菜仔",
                              "$7.0",
                              "$7.4"],
                             ["2022-05-05",
                              "韭花",
                              "$23.0",
                              "$21.7"],
                             ["2022-05-05",
                              "沙刈",
                              "$16.2",
                              "$10.6"],
                             ["2022-05-05",
                              "豆苗",
                              "$21.0",
                              "$23.2"],
                             ["2022-05-05",
                              "車厘茄",
                              "$11.0",
                              "$7.8"],
                             ["2022-05-05",
                              "蘿蔔葉",
                              "$5.2",
                              "$5.2"],
                             ["2022-05-05",
                              "西洋菜",
                              "$6.5",
                              "$6.2"],
                             ["2022-05-05",
                              "西芹",
                              "$8.7",
                              "$8.4"],
                             ["2022-05-05",
                              "菠菜",
                              "$12.2",
                              "$9.7"]]}

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
