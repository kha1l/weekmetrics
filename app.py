import time
import schedule
from application.exporter import exporter
from worker import work


@schedule.repeat(schedule.every().monday.at('03:15'))
def get_metrics_week():
    exporter('msk-sch', 'week')


@schedule.repeat(schedule.every().monday.at('05:10'))
def get_metrics_week():
    exporter('sergpas', 'week')


@schedule.repeat(schedule.every().day.at('13:51'))
def get_metrics_week():
    exporter('zelen', 'week')


@schedule.repeat(schedule.every().monday.at('04:15'))
def get_metrics_week():
    exporter('south', 'week')


@schedule.repeat(schedule.every().monday.at('14:08'))
def get_metrics_week():
    exporter('vkus', 'week')


@schedule.repeat(schedule.every().monday.at('01:15'))
def get_metrics_week():
    exporter('omsk', 'week')


@schedule.repeat(schedule.every().monday.at('12:28'))
def get_metrics_week():
    exporter('korsakov', 'week')


@schedule.repeat(schedule.every().monday.at('10:50'))
def get_metrics_week():
    exporter('vkz', 'week')


@schedule.repeat(schedule.every().monday.at('06:10'))
def get_metrics_week():
    exporter('veris', 'week')


@schedule.repeat(schedule.every().monday.at('03:30'))
def get_metrics_day():
    work('msk-sch', 'week')


@schedule.repeat(schedule.every().monday.at('05:15'))
def get_metrics_day():
    work('sergpas', 'week')


@schedule.repeat(schedule.every().day.at('13:52'))
def get_metrics_day():
    work('zelen', 'week')


@schedule.repeat(schedule.every().monday.at('04:30'))
def get_metrics_day():
    work('south', 'week')


@schedule.repeat(schedule.every().monday.at('14:24'))
def get_metrics_day():
    work('vkus', 'week')


@schedule.repeat(schedule.every().monday.at('01:30'))
def get_metrics_day():
    work('omsk', 'week')


@schedule.repeat(schedule.every().monday.at('06:15'))
def get_metrics_day():
    work('veris', 'week')


@schedule.repeat(schedule.every().monday.at('12:55'))
def get_metrics_day():
    work('korsakov', 'week')


@schedule.repeat(schedule.every().monday.at('11:28'))
def get_metrics_day():
    work('vkz', 'week')


if __name__ == '__main__':
    while True:
        schedule.run_pending()
        time.sleep(1)
