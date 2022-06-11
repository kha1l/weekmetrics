import pandas as pd


class ReadFile:

    def __init__(self, name_rest: str, tps: str):
        self.name = name_rest
        self.tps = tps

    def open_file(self, order: str, rows: int):
        if order != 'rating':
            try:
                df = pd.read_excel(f'./orders/export/{order}_{self.name}_{self.tps}.xlsx', skiprows=rows)
            except ValueError:
                print(f'Неккоректный отчет в {self.name}:{order}')
                df = pd.DataFrame()
        else:
            try:
                df = pd.read_excel(f'./orders/export/{order}_{self.name}_{self.tps}.xlsx',
                                   sheet_name='Рейтинг курьеров по доставкам', skiprows=rows)
            except ValueError:
                print(f'Неккоректный отчет в {self.name}:{order}')
                df = pd.DataFrame()
        return df

    def open_txt(self):
        with open(f'./orders/export/staff_{self.name}_{self.tps}.txt', 'r') as file:
            st = file.read()
        return st


class Reader(ReadFile):
    df_rev = None
    df_prod = None
    df_del = None
    df_stop = None
    df_stop_ing = None
    df_stop_prod = None
    df_hand_del = None
    df_hand_stat = None
    df_actual = None
    df_extra = None
    df_driving = None
    df_refusal = None
    df_activity = None
    df_salary = None
    df_lunch = None
    df_check = None
    df_rating_client = None
    df_rating_couriers = None
    staff = None

    def read_df(self):
        self.df_rev = self.open_file('revenue', 17)
        self.df_prod = self.open_file('productivity', 4)
        self.df_del = self.open_file('del_statistic', 5)
        self.df_stop = self.open_file('being_stop', 5)
        self.df_stop_ing = self.open_file('being_stop_ingredient', 6)
        self.df_stop_prod = self.open_file('being_stop_product', 6)
        self.df_hand_del = self.open_file('handover_delivery', 6)
        self.df_hand_stat = self.open_file('handover_stationary', 6)
        self.df_actual = self.open_file('time_work', 4)
        self.df_extra = self.open_file('time_work_extra', 4)
        self.df_driving = self.open_file('driving_couriers', 4)
        self.df_refusal = self.open_file('orders', 7)
        self.df_activity = self.open_file('activity_bonus', 4)
        self.df_salary = self.open_file('salary', 4)
        self.df_lunch = self.open_file('lunch', 5)
        self.df_check = self.open_file('average_check', 7)
        self.df_rating_couriers = self.open_file('rating', 4)
        self.df_rating_client = pd.read_json('https://publicapi.dodois.io/ru/api/v1/ratings')
        self.staff = self.open_txt()
