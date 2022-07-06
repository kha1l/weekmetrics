from postgres.psql import Database
from orders.export import DataExportDay
from date_work import DataWork
import time
from datetime import date


def exporter(group: str, tps: str):
    db = Database()
    dt = DataWork(date_end=date(2022, 7, 3)).set_date()
    users = db.get_users(group)
    for user in users:
        data = DataExportDay(dt, user[0], tps)
        data.revenue()
        data.productivity()
        data.delivery_statistic()
        data.orders()
        data.time_work()
        data.handover_stationary()
        data.handover_delivery()
        data.being_stop()
        data.activity_bonus()
        data.being_stop_ingredient()
        data.being_stop_product()
        data.employee_foods()
        data.salary()
        data.time_work_extra()
        data.driving_couriers()
        data.lunch()
        data.average_check()
        data.staff()
        data.rating()
        data.statistic_rest()
        time.sleep(10)
