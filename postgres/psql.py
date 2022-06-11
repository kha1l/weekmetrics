import psycopg2
from config.conf import Config
from datetime import timedelta, date


class Database:
    @property
    def connection(self):
        cfg = Config()
        return psycopg2.connect(
            database=cfg.dbase,
            user=cfg.user,
            password=cfg.password,
            host=cfg.host,
            port='5432'
        )

    def execute(self, sql: str, parameters: tuple = None, fetchone=False, fetchall=False, commit=False):
        if not parameters:
            parameters = tuple()
        connection = self.connection
        cursor = connection.cursor()
        data = None
        cursor.execute(sql, parameters)
        if commit:
            connection.commit()
        if fetchone:
            data = cursor.fetchone()
        if fetchall:
            data = cursor.fetchall()
        connection.close()
        return data

    def get_data(self, name: str):
        sql = '''
            SELECT restId, uuId, restLogin, 
                    restPassword, countryCode 
            FROM settings 
            WHERE restName=%s;
        '''
        parameters = (name,)
        return self.execute(sql, parameters=parameters, fetchone=True)

    def get_users(self, group: str):
        sql = '''
            SELECT restName, restId , uuId
            FROM settings 
            WHERE restGroup=%s 
            ORDER BY restId;
        '''
        parameters = (group,)
        return self.execute(sql, parameters=parameters, fetchall=True)

    def get_tax(self, name: str):
        sql = '''
            SELECT restTax 
            FROM settings 
            WHERE restName=%s;
        '''
        parameters = (name,)
        return self.execute(sql, parameters=parameters, fetchone=True)

    def get_line(self, dt: str, rest_id: int):
        sql = '''
            SELECT restName 
            FROM week 
            WHERE ordersDay=%s and restId=%s;
        '''
        parameters = dt, rest_id
        return self.execute(sql, parameters=parameters, fetchall=True)

    def add_metrics(self, dt: date, rest_id: int, rest_name: str, rev: int, rev_r: int,
                    rev_d: int, rev_p: int, ord_del: int, act_time: float, ex_time: float,
                    avg_m: float, max_m: float, avg_k: float, prod: int, pr: float,
                    t_rest: timedelta, t_del: timedelta, t_app: timedelta, ord_hour: float,
                    prod_auto: float, prod_bike: float, wk_all: float, wk_auto: float,
                    wk_bike: float, pr_auto: float, pr_bike: float, t_shelf: timedelta,
                    avg_del: timedelta, cert: int, app_cert: int, rat: float,
                    pr_later: float, client: float, k_cost: float, s_kitchen: float,
                    prem_kitchen: float, train_s: float, d_cost: float, s_couriers: float,
                    prem_couriers: float, one_del: float, order_ref: float, proc_ref: float,
                    action50: float, action60: float, action49: float, lunch: float,
                    proc_lunch: float, stop: timedelta, stop_cause: str, stop_prod: int,
                    list_prod: str, stop_ing: int, list_ing: str, staff: str,
                    couriers: str, form: str, check: int):
        sql = '''
            INSERT INTO week (ordersDay, restId, restName, revenue, revenueRest, 
                revenueDelivery, revenuePickup, ordersDelivery, workKitchen, workExtra, 
                workloadManager, workloadManagerMax, workloadKitchen, productivity, 
                productHour, timeRest, timeDelivery, timeAssembly, orderHour, 
                productivityAuto, productivityBike, workloadCouriers, workloadAuto, 
                workloadBike, percentAuto, percentBike, timeShelf, speedDelivery,
                certificates, certificatesApply, ratingCouriers, percentLater, 
                ratingClients, kitchenCost, salaryKitchen, awardsKitchen, salaryTrainee, 
                deliveryCost, salaryCouriers, awardsCouriers, oneDelivery, refusal, 
                percentRefusal, action50, action60, action49, lunch, percentLunch, 
                stopSelling, stopCause, stopProduct, stopCauseProduct, stopIngredient, 
                stopCauseIngredient, staffKitchen, staffCouriers, formResponse, 
                checkDelivery) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        parameters = (dt, rest_id, rest_name, rev, rev_r, rev_d, rev_p, ord_del,
                      act_time, ex_time, avg_m, max_m, avg_k, prod, pr, t_rest,
                      t_del, t_app, ord_hour, prod_auto, prod_bike, wk_all,
                      wk_auto, wk_bike, pr_auto, pr_bike, t_shelf, avg_del, cert,
                      app_cert, rat, pr_later, client, k_cost, s_kitchen,
                      prem_kitchen, train_s, d_cost, s_couriers, prem_couriers, one_del,
                      order_ref, proc_ref, action50, action60, action49, lunch,
                      proc_lunch, stop, stop_cause, stop_prod, list_prod, stop_ing,
                      list_ing, staff, couriers, form, check)
        return self.execute(sql, parameters=parameters, commit=True)

    def update_metrics(self, dt: date, rest_id: int, rest_name: str, rev: int, rev_r: int,
                       rev_d: int, rev_p: int, ord_del: int, act_time: float, ex_time: float,
                       avg_m: float, max_m: float, avg_k: float, prod: int, pr: float,
                       t_rest: timedelta, t_del: timedelta, t_app: timedelta, ord_hour: float,
                       prod_auto: float, prod_bike: float, wk_all: float, wk_auto: float,
                       wk_bike: float, pr_auto: float, pr_bike: float, t_shelf: timedelta,
                       avg_del: timedelta, cert: int, app_cert: int, rat: float,
                       pr_later: float, client: float, k_cost: float, s_kitchen: float,
                       prem_kitchen: float, train_s: float, d_cost: float, s_couriers: float,
                       prem_couriers: float, one_del: float, order_ref: float, proc_ref: float,
                       action50: float, action60: float, action49: float, lunch: float,
                       proc_lunch: float, stop: timedelta, stop_cause: str, stop_prod: int,
                       list_prod: str, stop_ing: int, list_ing: str, staff: str,
                       couriers: str, form: str, check: int):
        sql = '''
            UPDATE week SET ordersDay=%s, restId=%s, restName=%s, revenue=%s, revenueRest=%s, 
                revenueDelivery=%s, revenuePickup=%s, ordersDelivery=%s, workKitchen=%s, 
                workExtra=%s, workloadManager=%s, workloadManagerMax=%s, workloadKitchen=%s, 
                productivity=%s, productHour=%s, timeRest=%s, timeDelivery=%s, 
                timeAssembly=%s, orderHour=%s, productivityAuto=%s, productivityBike=%s, 
                workloadCouriers=%s, workloadAuto=%s, workloadBike=%s, percentAuto=%s, 
                percentBike=%s, timeShelf=%s, speedDelivery=%s, certificates=%s, 
                certificatesApply=%s, ratingCouriers=%s, percentLater=%s, ratingClients=%s, 
                kitchenCost=%s, salaryKitchen=%s, awardsKitchen=%s, salaryTrainee=%s, 
                deliveryCost=%s, salaryCouriers=%s, awardsCouriers=%s, oneDelivery=%s, 
                refusal=%s, percentRefusal=%s, action50=%s, action60=%s, action49=%s, 
                lunch=%s, percentLunch=%s, stopSelling=%s, stopCause=%s, stopProduct=%s, 
                stopCauseProduct=%s, stopIngredient=%s, stopCauseIngredient=%s, 
                staffKitchen=%s, staffCouriers=%s, formResponse=%s, checkDelivery=%s)
            WHERE ordersDay=%s and restId=%s;
        '''
        parameters = (dt, rest_id, rest_name, rev, rev_r, rev_d, rev_p, ord_del,
                      act_time, ex_time, avg_m, max_m, avg_k, prod, pr, t_rest,
                      t_del, t_app, ord_hour, prod_auto, prod_bike, wk_all,
                      wk_auto, wk_bike, pr_auto, pr_bike, t_shelf, avg_del, cert,
                      app_cert, rat, pr_later, client, k_cost, s_kitchen,
                      prem_kitchen, train_s, d_cost, s_couriers, prem_couriers, one_del,
                      order_ref, proc_ref, action50, action60, action49, lunch,
                      proc_lunch, stop, stop_cause, stop_prod, list_prod, stop_ing,
                      list_ing, staff, couriers, form, check, dt, rest_id)
        return self.execute(sql, parameters=parameters, commit=True)
