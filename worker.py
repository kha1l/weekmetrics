from postgres.psql import Database
from application.reader import Reader
from application.changer import Changer
from date_work import DataWork
from datetime import date


def work(group: str, tps: str):
    db = Database()
    users = db.get_users(group)
    dt = DataWork(date(2022, 6, 5)).set_date()
    for user in users:
        line = db.get_line(dt, user[1])
        cls_df = Reader(user[0], tps)
        cls_df.read_df()
        change = Changer(cls_df)
        revenue, revenue_rest, revenue_delivery, revenue_pickup, delivery_orders = change.change_revenue()
        extra = change.change_extra_time()
        time_auto, time_bike, time_manager, time_kitchen, avg_manager, avg_kitchen, max_manager, bike_id = change.change_actual_time()
        productivity, product, orders_per_hour = change.change_productivity()
        time_in_delivery, time_in_shelf = change.change_handover_delivery()
        time_in_rest, time_app = change.change_handover_stationary()
        auto_duration, bike_duration, auto_ord, bike_ord = change.couriers_time(bike_id)
        workload_all, workload_auto, workload_bike, perc_auto, perc_bike, prod_auto, prod_bike = change.change_workload(
            time_auto, time_bike, delivery_orders, auto_duration, bike_duration, auto_ord, bike_ord)
        delivery_time, certificates, perc_later = change.change_delivery_statistic(delivery_orders)
        refusal, perc_refusal = change.change_refusal(revenue)
        act50, act60, act49, app_cert = change.change_activity()
        salary_c, salary_pc, salary_k, salary_t, salary_pk, del_cost, total_one, kit_cost = change.change_salary(
            user[0], revenue, revenue_delivery, delivery_orders)
        stop_duration, stop_cause = change.change_being_stop()
        ingredient, line_ingredient = change.change_stop_ing()
        product_stop, line_product = change.change_stop_prod()
        lunch, perc_lunch = change.change_lunch(revenue)
        check = change.change_average_check()
        rating_client = change.change_rating_client(user[2])
        rating_couriers = change.change_rating_couriers()
        kitchen, courier, form = change.change_staff()
        if len(line) == 0:
            db.add_metrics(dt, user[1], user[0], revenue, revenue_rest, revenue_delivery, revenue_pickup,
                           delivery_orders, time_kitchen, extra, avg_manager, max_manager, avg_kitchen,
                           productivity, product, time_in_rest, time_in_delivery, time_app, orders_per_hour,
                           prod_auto, prod_bike, workload_all, workload_auto, workload_bike, perc_auto, perc_bike,
                           time_in_shelf, delivery_time, certificates, app_cert, rating_couriers, perc_later,
                           rating_client, kit_cost, salary_k, salary_pk, salary_t, del_cost, salary_c, salary_pc,
                           total_one, refusal, perc_refusal, act50, act60, act49, lunch, perc_lunch, stop_duration,
                           stop_cause, product_stop, line_product, ingredient, line_ingredient, kitchen, courier,
                           form, check)
        else:
            db.update_metrics(dt, user[1], user[0], revenue, revenue_rest, revenue_delivery, revenue_pickup,
                              delivery_orders, time_kitchen, extra, avg_manager, max_manager, avg_kitchen,
                              productivity, product, time_in_rest, time_in_delivery, time_app, orders_per_hour,
                              prod_auto, prod_bike, workload_all, workload_auto, workload_bike, perc_auto, perc_bike,
                              time_in_shelf, delivery_time, certificates, app_cert, rating_couriers, perc_later,
                              rating_client, kit_cost, salary_k, salary_pk, salary_t, del_cost, salary_c, salary_pc,
                              total_one, refusal, perc_refusal, act50, act60, act49, lunch, perc_lunch, stop_duration,
                              stop_cause, product_stop, line_product, ingredient, line_ingredient, kitchen, courier,
                              form, check)
