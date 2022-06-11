from dotenv import load_dotenv
import os


class Config:
    load_dotenv()
    dbase = os.getenv('DATA_BASE')
    user = os.getenv('USER_NAME')
    password = os.getenv('PASSWORD')
    host = os.getenv('IP')
    sheet_day = os.getenv('SHEET_DAY')
    sheet_week = os.getenv('SHEET_WEEK')
    sheet_month = os.getenv('SHEET_MONTH')
    table_ptf = os.getenv('TABLE_PTF')
    table_pyt = os.getenv('TABLE_PYT')
    table_msk = os.getenv('TABLE_MSK')
