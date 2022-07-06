from application.reader import Reader
from datetime import timedelta, time
import pandas as pd
from postgres.psql import Database
from collections import Counter
import operator


class Changer:

    def __init__(self, obj: Reader) -> None:
        self.obj = obj

    @staticmethod
    def change_time(t):
        try:
            t = timedelta(hours=t.hour, minutes=t.minute, seconds=t.second, microseconds=t.microsecond)
        except AttributeError:
            t = timedelta(0)
        return t

    @staticmethod
    def change_string(t):
        t = str(t) + ':00'
        return t

    @staticmethod
    def df_handover(df):
        def change_time(t):
            try:
                t = timedelta(hours=t.hour, minutes=t.minute, seconds=t.second, microseconds=t.microsecond)
            except AttributeError:
                t = timedelta(0)
            return t
        try:
            time_handover = pd.to_timedelta(
                df['Ожидание'].apply(change_time).mean() + df['Приготовление'].apply(change_time).mean())
        except KeyError:
            time_handover = '0:00:00'

        if issubclass(type(time_handover), pd._libs.tslibs.nattype.NaTType):
            time_handover = pd.to_timedelta(0)

        return time_handover

    def change_revenue(self):
        df = self.obj.df_rev

        try:
            df = df.loc[df['Дата'] == 'Итого']
        except KeyError:
            df = pd.DataFrame()

        try:
            revenue = df.iloc[0]['Итого']
        except IndexError:
            revenue = 0
        except KeyError:
            revenue = 0

        try:
            revenue_rest = df.iloc[0]['Ресторан']
        except IndexError:
            revenue_rest = 0
        except KeyError:
            revenue_rest = 0

        try:
            revenue_delivery = df.iloc[0]['Доставка']
        except IndexError:
            revenue_delivery = 0
        except KeyError:
            revenue_delivery = 0

        try:
            revenue_pickup = df.iloc[0]['Самовывоз']
        except IndexError:
            revenue_pickup = 0
        except KeyError:
            revenue_pickup = 0

        try:
            delivery_orders = df.iloc[0]['Доставка.1']
        except IndexError:
            delivery_orders = 0
        except KeyError:
            delivery_orders = 0

        return int(revenue), int(revenue_rest), int(revenue_delivery), int(revenue_pickup), int(delivery_orders)

    def change_extra_time(self):
        df = self.obj.df_extra

        try:
            df_kitchen = df.drop(
                df[(df['Категория сотрудника'] == 'Автомобильный') | (df['Категория сотрудника'] == 'Велосипедный') | (
                    df['Категория сотрудника'] == 'Пеший')].index)
        except KeyError:
            df_kitchen = pd.DataFrame()

        try:
            extra = pd.to_timedelta(df_kitchen['Количество часов'].apply(self.change_time).sum())
            extra = extra.total_seconds() / 3600
        except KeyError:
            extra = 0

        return extra

    def change_actual_time(self):
        df = self.obj.df_actual

        try:
            df_auto = df.loc[df['Категория'] == 'Автомобильный']
            df_bike = df.loc[(df['Категория'] == 'Велосипедный') | (df['Категория'] == 'Пеший')]
            df_manager = df.loc[(df['Категория'] == 'Менеджер') | (df['Категория'] == 'Стажер-менеджер')]
            df_kitchen = df.drop(df[(df['Категория'] == 'Автомобильный') | (df['Категория'] == 'Велосипедный') | (
                    df['Категория'] == 'Пеший')].index)
            bike_id = df_bike['Id сотрудника'].to_list()

            count_manager = df_manager['Id сотрудника'].nunique(dropna=True)
            count_kitchen = df_kitchen['Id сотрудника'].nunique(dropna=True)

            auto = pd.to_timedelta(df_auto['Итого'].apply(self.change_string)).sum()
            bike = pd.to_timedelta(df_bike['Итого'].apply(self.change_string)).sum()
            manager = pd.to_timedelta(df_manager['Итого'].apply(self.change_string)).sum()
            kitchen = pd.to_timedelta(df_kitchen['Итого'].apply(self.change_string)).sum()
            df_man = pd.to_timedelta(df_manager['Итого'].apply(self.change_string))
            df_manager.insert(0, 'result', df_man)
            max_manager = (df_manager.groupby('Id сотрудника')['result'].sum()).max()

        except KeyError:
            count_kitchen = 0
            count_manager = 0
            auto = timedelta(0)
            bike = timedelta(0)
            manager = timedelta(0)
            kitchen = timedelta(0)
            max_manager = timedelta(0)
            bike_id = list()

        try:
            avg_manager = manager / count_manager
            avg_kitchen = kitchen / count_kitchen
        except ZeroDivisionError:
            avg_manager = timedelta(0)
            avg_kitchen = timedelta(0)

        auto = round(auto.total_seconds() / 3600, 2)
        bike = round(bike.total_seconds() / 3600, 2)

        manager = round(manager.total_seconds() / 3600, 2)
        kitchen = round(kitchen.total_seconds() / 3600, 2)
        avg_manager = round(avg_manager.total_seconds() / 3600, 2)
        max_manager = round(max_manager.total_seconds() / 3600, 2)
        avg_kitchen = round(avg_kitchen.total_seconds() / 3600, 2)

        return auto, bike, manager, kitchen, avg_manager, avg_kitchen, max_manager, bike_id

    def change_productivity(self):
        df = self.obj.df_prod

        try:
            productivity = int(round(df['Выручка'].sum() / df['Кол-во отработанных часов'].sum(), 2))
        except ZeroDivisionError:
            productivity = 0
        except KeyError:
            productivity = 0
        except ValueError:
            productivity = 0

        try:
            order_per_hour = float(round(df['Кол-во заказов на курьера в час'].mean(), 2))
        except KeyError:
            order_per_hour = 0
        except ValueError:
            order_per_hour = 0

        try:
            product_on_hour = float(round(df['Продуктов на человека в час'].mean(), 2))
        except KeyError:
            product_on_hour = 0
        except ValueError:
            product_on_hour = 0

        return productivity, product_on_hour, order_per_hour

    def change_handover_delivery(self):
        df = self.obj.df_hand_del

        try:
            time_shelf = pd.to_timedelta(df['Ожидание на полке'].apply(self.change_time).mean())
        except KeyError:
            time_shelf = timedelta(0)

        if issubclass(type(time_shelf), pd._libs.tslibs.nattype.NaTType):
            time_shelf = pd.to_timedelta(0)

        handover = self.df_handover(df)

        return handover, time_shelf

    def change_handover_stationary(self):
        df = self.obj.df_hand_stat

        try:
            df_app = df.loc[df['Сборка заказов из приложения в ресторане'] != time(0, 0, 0)]
            time_app = pd.to_timedelta(df_app['Сборка заказов из приложения в ресторане'].apply(self.change_time).mean())
        except KeyError:
            time_app = timedelta(0)

        if issubclass(type(time_app), pd._libs.tslibs.nattype.NaTType):
            time_app = pd.to_timedelta(0)

        handover = self.df_handover(df)

        return handover, time_app

    def couriers_time(self, bike_id):
        df = self.obj.df_driving

        def flt_time(t):
            h = t.days * 24 + t.seconds / 3600
            return h

        try:
            df['Id сотрудника'] = df['Id сотрудника'].apply(lambda x: 1 if x in bike_id else 0)
            df_bike = df.loc[df['Id сотрудника'] == 1]
            df_auto = df.loc[df['Id сотрудника'] == 0]
        except KeyError:
            df_bike = pd.DataFrame()
            df_auto = pd.DataFrame()

        try:
            bike_duration = pd.to_timedelta(df_bike['Длительность'].apply(self.change_time).sum())
            auto_duration = pd.to_timedelta(df_auto['Длительность'].apply(self.change_time).sum())
        except KeyError:
            bike_duration = timedelta(0)
            auto_duration = timedelta(0)

        try:
            bike_ord = df_bike['Количество заказов'].sum()
            auto_ord = df_auto['Количество заказов'].sum()
        except KeyError:
            bike_ord = 0
            auto_ord = 0

        bike_duration = flt_time(bike_duration)
        auto_duration = flt_time(auto_duration)

        return auto_duration, bike_duration, int(auto_ord), int(bike_ord)

    @staticmethod
    def change_workload(auto, bike, orders, auto_dur, bike_dur, auto_ord, bike_ord):
        couriers = auto + bike
        try:
            workload_all = round((auto_dur + bike_dur) / couriers, 2)
        except ZeroDivisionError:
            workload_all = 0
        try:
            workload_auto = round(auto_dur / auto, 2)
        except ZeroDivisionError:
            workload_auto = 0
        try:
            workload_bike = round(bike_dur / bike, 2)
        except ZeroDivisionError:
            workload_bike = 0
        try:
            perc_auto = round(auto_ord / orders, 2)
        except ZeroDivisionError:
            perc_auto = 0
        try:
            perc_bike = round(bike_ord / orders, 2)
        except ZeroDivisionError:
            perc_bike = 0
        try:
            prod_auto = round(auto_ord / auto, 2)
        except ZeroDivisionError:
            prod_auto = 0
        try:
            prod_bike = round(bike_ord / bike, 2)
        except ZeroDivisionError:
            prod_bike = 0
        return workload_all, workload_auto, workload_bike, perc_auto, perc_bike, prod_auto, prod_bike

    def change_delivery_statistic(self, orders):
        df = self.obj.df_del

        try:
            avg_del = df.iloc[0]['Среднее время доставки*']
        except IndexError:
            avg_del = timedelta(0)
        except KeyError:
            avg_del = timedelta(0)

        try:
            cert = df.iloc[0]['Количество просроченных заказов']
        except IndexError:
            cert = 0
        except KeyError:
            cert = 0

        try:
            perc_later = round(cert / orders * 100, 2)
        except ZeroDivisionError:
            perc_later = 0

        return avg_del, int(cert), perc_later

    def change_refusal(self, revenue):
        df = self.obj.df_refusal

        try:
            df = df.loc[df['Статус заказа'] == 'Отказ']
            refusal = float(df['Сумма заказа'].sum())
        except (IndexError, KeyError):
            refusal = 0

        try:
            perc_refusal = round(refusal / revenue, 2)
        except ZeroDivisionError:
            perc_refusal = 0

        return refusal, perc_refusal

    def change_activity(self):
        df = self.obj.df_activity

        try:
            df_staff50 = df.loc[df['Название акции'] == 'Скидка сотрудникам 50%']
        except KeyError:
            df_staff50 = pd.DataFrame()

        try:
            df_staff60 = df.loc[df['Название акции'] == 'Скидка контрагентам 60%']
        except KeyError:
            df_staff60 = pd.DataFrame()

        try:
            df_staff49 = df.loc[df['Название акции'] == 'Скидка контрагентам 49%']
        except KeyError:
            df_staff49 = pd.DataFrame()

        try:
            df_cert = df.loc[df['Категория'] == 'Сертификаты за опоздание']
        except KeyError:
            df_cert = pd.DataFrame()

        try:
            app_cert = df_cert.iloc[0]['Количество срабатываний']
        except IndexError:
            app_cert = 0

        try:
            total_50 = df_staff50['Сумма заказов по акции'].sum()
        except IndexError:
            total_50 = 0

        try:
            total_60 = df_staff60['Сумма заказов по акции'].sum()
        except IndexError:
            total_60 = 0

        try:
            total_49 = df_staff49['Сумма заказов по акции'].sum()
        except IndexError:
            total_49 = 0

        return float(total_50), float(total_60), float(total_49), int(app_cert)

    def change_salary(self, name, revenue, revenue_del, orders):
        db = Database()
        tax = db.get_tax(name)
        df = self.obj.df_salary

        try:
            df_couriers = df.loc[(df['Категория'] == 'Автомобильный') | (df['Категория'] == 'Велосипедный') |
                                 (df['Категория'] == 'Пеший')]
            df_kitchen = df.drop(df[(df['Категория'] == 'Автомобильный') | (df['Категория'] == 'Велосипедный') |
                                    (df['Категория'] == 'Пеший')].index)
            df_trainee = df_kitchen.loc[(df_kitchen['Категория'] == 'Стажер-менеджер') |
                                        (df_kitchen['Категория'] == 'Стажер-пиццамейкер') |
                                        (df_kitchen['Категория'] == 'Стажер-кассир')]
        except KeyError:
            df_couriers = pd.DataFrame()
            df_trainee = pd.DataFrame()
            df_kitchen = pd.DataFrame()

        try:
            salary_c = float(df_couriers['Итого'].sum())
            salary_k = float(df_kitchen['Итого'].sum())
            salary_t = float(df_trainee['Итого'].sum())
            salary_pc = float(df_couriers['ЗП, премия'].sum() + df_couriers['ЗП, премия за стаж'].sum())
            salary_pk = float(df_kitchen['ЗП, премия'].sum() + df_kitchen['ЗП, премия за стаж'].sum())
        except KeyError:
            salary_c = 0
            salary_k = 0
            salary_t = 0
            salary_pk = 0
            salary_pc = 0

        try:
            del_cost = float(round((salary_c / revenue_del) * 100, 2))
        except ZeroDivisionError:
            del_cost = 0

        try:
            total_one = float(round((salary_c / orders), 2))
        except ZeroDivisionError:
            total_one = 0

        try:
            kit_cost = float(round((salary_k * tax[0] / revenue) * 100, 2))
        except ZeroDivisionError:
            kit_cost = 0

        return salary_c, salary_pc, salary_k, salary_t, salary_pk, del_cost, total_one, kit_cost

    def change_being_stop(self):
        df = self.obj.df_stop

        try:
            df = df.drop_duplicates(subset=['Дата остановки'], keep='first')
        except KeyError:
            df = 0

        try:
            stop_duration = pd.to_timedelta(df['Длительность за отчетный период'].apply(self.change_time).sum())
        except KeyError:
            stop_duration = 0

        try:
            df = df.drop_duplicates(subset=['Причина остановки'], keep='first')
        except KeyError:
            df = 0

        try:
            cause_stop = df['Причина остановки'].tolist()
        except KeyError:
            cause_stop = 0

        if len(cause_stop) != 0:
            stop_cause = ','.join(cause_stop)
        else:
            stop_cause = '-'

        return stop_duration, stop_cause

    @staticmethod
    def result_list(result_ingredient_list):
        result = dict(Counter(result_ingredient_list))
        line = ''
        sum_values = 0
        sorted_tuples = sorted(result.items(), key=operator.itemgetter(1), reverse=True)
        sorted_dict = {k: v for k, v in sorted_tuples}
        for j in range(len(sorted_dict)):
            key, value = list(sorted_dict.items())[j]
            line += key + ' - ' + str(value) + ';\n'
            sum_values += value
        return sum_values, line

    def change_stop_ing(self):
        df = self.obj.df_stop_ing
        result_ingredient_list = list()

        if df.shape[0] != 0:
            result_ingredient_list.append(df['Название продукта'][0])
            for i in range(1, len(df['Название продукта'])):
                if (df['Название продукта'][i] == df['Название продукта'][
                    i - 1]) and (
                        df['Дата остановки'][i] == df['Дата остановки'][i - 1]):
                    continue
                else:
                    if 'персонал' in df['Название продукта'][i]:
                        continue
                    else:
                        result_ingredient_list.append(df['Название продукта'][i])
        sum_values, line_ingredient = self.result_list(result_ingredient_list)
        return sum_values, line_ingredient

    def change_stop_prod(self):
        df = self.obj.df_stop_prod
        result_product_list = list()
        if df.shape[0] != 0:
            result_product_list.append(df['Ингредиент'][0])
            for i in range(1, len(df['Ингредиент'])):
                if (df['Ингредиент'][i] == df['Ингредиент'][i - 1]) and (
                        df['Дата остановки'][i] == df['Дата остановки'][i - 1]):
                    continue
                else:
                    if 'персонал' in df['Ингредиент'][i]:
                        continue
                    else:
                        result_product_list.append(df['Ингредиент'][i])
        sum_values, line_product = self.result_list(result_product_list)
        return sum_values, line_product

    def change_lunch(self, revenue):
        df = self.obj.df_lunch
        try:
            lunch = float(df['Сумма, ₽'].sum())
        except KeyError:
            lunch = 0

        try:
            perc_lunch = round(lunch / revenue, 2)
        except ZeroDivisionError:
            perc_lunch = 0

        return lunch, perc_lunch

    def change_average_check(self):
        df = self.obj.df_check
        try:
            check = int(df['Средний чек'].mean())
        except KeyError:
            check = 0
        except ValueError:
            check = 0
        return check

    def change_rating_client(self, uuid):
        df = self.obj.df_rating_client
        df_rat = df.loc[df['UnitUUId'] == uuid].reset_index()
        try:
            rating = round(df_rat.iloc[0]['AvgRating'], 2)
        except IndexError:
            rating = 0
        return float(rating)

    def change_rating_couriers(self):
        df = self.obj.df_rating_couriers
        try:
            avg_rating = float(round(df['Rating'].mean(), 2))
        except KeyError:
            avg_rating = 0
        return avg_rating

    def change_staff(self):
        staff = self.obj.staff
        stf = staff.split('-')
        kitchen = stf[0]
        courier = stf[1]
        form = stf[2]
        return kitchen, courier, form

    def change_rest_orders(self):
        df = self.obj.df_rest_orders
        try:
            case = int(df.iloc[0]['Заказов на кассе'])
            app = int(df.iloc[0]['Заказов в моб. приложении'])
        except KeyError:
            case = 0
            app = 0
        except IndexError:
            case = 0
            app = 0

        all_orders = case + app
        try:
            perc_app = round(float(app / all_orders * 100), 2)
        except ZeroDivisionError:
            perc_app = 0
        return all_orders, app, perc_app
