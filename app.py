import time
import schedule
from application.exporter import exporter
from worker import work


@schedule.repeat(schedule.every().day.at('12:25'))
def get_metrics_week():
    exporter('vkus', 'week')
    exporter('south', 'week')


@schedule.repeat(schedule.every().day.at('12:35'))
def get_metrics_day():
    work('vkus', 'week')
    work('south', 'week')


if __name__ == '__main__':
    while True:
        schedule.run_pending()
        time.sleep(1)
