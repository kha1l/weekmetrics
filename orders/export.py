import requests
import fake_useragent
from datetime import date, timedelta
from postgres.psql import Database
from bs4 import BeautifulSoup


class DataExportDay:

    def __init__(self, date_end: date, name: str, tps: str):
        db = Database()
        data = db.get_data(name)
        self.name = name
        self.rest = data[0]
        self.uuid = data[1]
        self.date_end = date_end
        self.login = data[2]
        self.password = data[3]
        self.code = data[4]
        self.session = None
        self.user = None
        self.header = None
        self.tps = tps
        self.auth()

    def auth(self):
        self.session = requests.Session()
        self.user = fake_useragent.UserAgent().random
        log_data = {
            'CountryCode': self.code,
            'login': self.login,
            'password': self.password
        }
        self.header = {
            'user-agent': self.user
        }
        log_link = f'https://auth.dodopizza.{self.code}/Authenticate/LogOn'
        self.session.post(log_link, data=log_data, headers=self.header)

    def save(self, orders_data):
        for order in orders_data:
            response = self.session.post(orders_data[order]['link'], data=orders_data[order]['data'],
                                         headers=self.header)
            with open(f'./orders/export/{order}_{self.name}_{self.tps}.xlsx', 'wb') as file:
                file.write(response.content)
                file.close()

    def staff(self):
        data1_link = f'https://officemanager.dodopizza.{self.code}/OfficeManager/EmployeeStatistics/PartialEmployeeStatistics?unitIds={self.rest}'
        response1 = self.session.get(data1_link, headers=self.header)

        data2_link = f'https://officemanager.dodopizza.{self.code}/OfficeManager/Applicants/InboxPartial?tabName=InboxTabCall&pageIndex=1&unitId={self.rest}'
        response2 = self.session.get(data2_link, headers=self.header)

        result = []

        soup1 = BeautifulSoup(response1.text, 'html.parser')
        finds1 = soup1.find_all("div", class_="dodoClearfix band-wrap")
        for find1 in finds1:
            finds2 = find1.find_all("div", class_="band band_green")
            for find2 in finds2:
                result.append(find2.text)
        soup2 = BeautifulSoup(response2.text, 'html.parser')
        finds3 = soup2.find_all("div", class_="bubble__number")
        try:
            find3 = finds3[-1].text.replace('\xa0', ' ')
        except IndexError:
            find3 = ''
        find_list = find3.split(' ')
        if len(find_list) == 2:
            find3 = find_list[0]
        else:
            if int(find_list[2]) > 30:
                find3 = int(find_list[0]) + 1
            else:
                find3 = find_list[0]
        result.append(find3)
        try:
            staff = str(result[0])
            couriers = str(result[1])
            form = str(result[2])
        except IndexError:
            staff = '0'
            couriers = '0'
            form = '0'
        res = '-'.join([staff, couriers, form])
        with open(f'./orders/export/staff_{self.name}_{self.tps}.txt', 'w') as file:
            file.write(res)
            file.close()

    def productivity(self):
        delta_days = {
            'day': 0,
            'week': 6,
        }
        orders_data = {
            'productivity': {
                'link': f'https://officemanager.dodopizza.{self.code}/Reports/Productivity/Export',
                'data': {
                    "unitId": self.rest,
                    "beginDate": self.date_end - timedelta(days=delta_days[self.tps]),
                    "endDate": self.date_end,
                    "Interval": "24"
                }
            }
        }
        self.save(orders_data)

    def revenue(self):
        delta_days = {
            'day': 0,
            'week': 6,
        }
        orders_data = {
            'revenue': {
                'link': f'https://officemanager.dodopizza.{self.code}/Reports/Revenue/Export',
                'data': {
                    "unitsIds": self.rest,
                    "OrderSources": [
                        "Telephone",
                        "Site",
                        "Restaurant",
                        "Mobile",
                        "Pizzeria",
                        "Aggregator"
                    ],
                    "ReportType": "ByDates",
                    "reportType": "",
                    "pseudoBeginTime": "",
                    "pseudoBeginDate": self.date_end - timedelta(days=delta_days[self.tps]),
                    "pseudoEndTime": "",
                    "pseudoEndDate": self.date_end,
                    "beginDate": self.date_end - timedelta(days=delta_days[self.tps]),
                    "endDate": self.date_end,
                    "beginTime": "",
                    "endTime": "",
                    "date": self.date_end,
                    "IsVatIncluded": [
                        "true",
                        "false"
                    ],
                    "Export": "Экспорт+в+Excel"
                }
            }
        }
        self.save(orders_data)

    def delivery_statistic(self):
        delta_days = {
            'day': 0,
            'week': 6,
        }
        orders_data = {
            'del_statistic': {
                'link': f'https://officemanager.dodopizza.{self.code}/Reports/DeliveryStatistic/Export',
                'data': {
                    "unitsIds": self.rest,
                    "beginDate": self.date_end - timedelta(days=delta_days[self.tps]),
                    "endDate": self.date_end
                }
            }
        }
        self.save(orders_data)

    def being_stop(self):
        delta_days = {
            'day': 0,
            'week': 6,
        }
        orders_data = {
            'being_stop': {
                'link': f'https://officemanager.dodopizza.{self.code}/Reports/StopSaleStatistic/Export',
                'data': {
                    "UnitsIds": self.rest,
                    "stopType": "0",
                    "beginDate": self.date_end - timedelta(days=delta_days[self.tps]),
                    "endDate": self.date_end
                }
            }
        }
        self.save(orders_data)

    def handover_delivery(self):
        delta_days = {
            'day': 0,
            'week': 6,
        }
        orders_data = {
            'handover_delivery': {
                'link': f'https://officemanager.dodopizza.{self.code}/Reports/OrderHandoverTime/Export',
                'data': {
                    "unitsIds": self.uuid,
                    "beginDate": self.date_end - timedelta(days=delta_days[self.tps]),
                    "endDate": self.date_end,
                    "orderTypes": "Delivery",
                    "Export": "Экспорт+в+Excel"
                }
            }
        }
        self.save(orders_data)

    def handover_stationary(self):
        delta_days = {
            'day': 0,
            'week': 6,
        }
        orders_data = {
            'handover_stationary': {
                'link': f'https://officemanager.dodopizza.{self.code}/Reports/OrderHandoverTime/Export',
                'data': {
                    "unitsIds": self.uuid,
                    "beginDate": self.date_end - timedelta(days=delta_days[self.tps]),
                    "endDate": self.date_end,
                    "orderTypes": "Stationary",
                    "Export": "Экспорт+в+Excel"
                }
            }
        }
        self.save(orders_data)

    def time_work_extra(self):
        delta_days = {
            'day': 0,
            'week': 6,
        }
        orders_data = {
            'time_work_extra': {
                'link': f'https://officemanager.dodopizza.{self.code}/Reports/TimeReport/Export',
                'data': {
                    "PageIndex": "0",
                    "unitId": self.rest,
                    "beginDate": self.date_end - timedelta(days=delta_days[self.tps]),
                    "endDate": self.date_end,
                    "EmployeeTypes": [
                        "Operator",
                        "Kitchen",
                        "Courier",
                        "Cashier",
                        "PersonalManager",
                        "Undefined"
                    ]
                }
            }
        }
        self.save(orders_data)

    def time_work(self):
        delta_days = {
            'day': 0,
            'week': 6,
        }
        orders_data = {
            'time_work': {
                'link': f'https://officemanager.dodopizza.{self.code}/Reports/ActualTime/Export',
                'data': {
                    "PageIndex": "1",
                    "unitId": self.rest,
                    "EmployeeName": "",
                    "isGroupingByEmployee": [
                        "true",
                        "false"
                    ],
                    "beginDate": self.date_end - timedelta(days=delta_days[self.tps]),
                    "endDate": self.date_end
                }
            }
        }
        self.save(orders_data)

    def rating(self):
        delta_days = {
            'day': 0,
            'week': 6,
        }
        orders_data = {
            'rating': {
                'link': f'https://officemanager.dodopizza.{self.code}/Reports/CourierRating/Export',
                'data': {
                    "unitId": self.rest,
                    "beginDate": self.date_end - timedelta(days=delta_days[self.tps]),
                    "endDate": self.date_end
                }
            }
        }
        self.save(orders_data)

    def activity_bonus(self):
        delta_days = {
            'day': 0,
            'week': 6,
        }
        orders_data = {
            'activity_bonus': {
                'link': f'https://officemanager.dodopizza.{self.code}/Reports/BonusActionActivity/Export',
                'data': {
                    "unitsIds": self.rest,
                    "beginDate": self.date_end - timedelta(days=delta_days[self.tps]),
                    "endDate": self.date_end
                }
            }
        }
        self.save(orders_data)

    def employee_foods(self):
        delta_days = {
            'day': 0,
            'week': 6,
        }
        orders_data = {
            'employee_foods': {
                'link': f'https://officemanager.dodopizza.{self.code}/Reports/EmployeeFoods/Export',
                'data': {
                    "unitsIds": self.rest,
                    "SearchText": "",
                    "beginDate": self.date_end - timedelta(days=delta_days[self.tps]),
                    "endDate": self.date_end,
                    "OnlyPeriod": "False",
                    "ExcludeDismissed": "false"
                }
            }
        }
        self.save(orders_data)

    def orders(self):
        delta_days = {
            'day': 0,
            'week': 6,
        }
        orders_data = {
            'orders': {
                'link': f'https://officemanager.dodopizza.{self.code}/Reports/Orders/Export',
                'data': {
                    "filterType": "AllOrders",
                    "unitsIds": self.rest,
                    "OrderSources": [
                        "Telephone",
                        "Site",
                        "Restaurant",
                        "DefectOrder",
                        "Mobile",
                        "Pizzeria",
                        "Aggregator"
                    ],
                    "beginDate": self.date_end - timedelta(days=delta_days[self.tps]),
                    "endDate": self.date_end,
                    "orderTypes": [
                        "Delivery",
                        "Pickup",
                        "Stationary"
                    ]
                }
            }
        }
        self.save(orders_data)

    def salary(self):
        delta_days = {
            'day': 0,
            'week': 6,
        }
        orders_data = {
            'salary': {
                'link': f'https://officemanager.dodopizza.{self.code}/Reports/Salary/Export',
                'data': {
                    "beginDate": self.date_end - timedelta(days=delta_days[self.tps]),
                    "endDate": self.date_end + timedelta(days=1),
                    "unitsIds": self.rest,
                    "EmployeeName": "",
                    "GroupedByEmployee": [
                        "true",
                        "false"
                    ],
                    "beginDatePicker": self.date_end - timedelta(days=delta_days[self.tps]),
                    "beginTimePicker": "",
                    "endDatePicker": self.date_end + timedelta(days=1),
                    "endTimePicker": "",
                    "EmployeeTypes": [
                        "Kitchen",
                        "Courier",
                        "Cashier"
                    ]
                }
            }
        }
        self.save(orders_data)

    def being_stop_product(self):
        delta_days = {
            'day': 0,
            'week': 6,
        }
        orders_data = {
            'being_stop_product': {
                'link': f'https://officemanager.dodopizza.{self.code}/Reports/StopSaleStatistic/Export',
                'data': {
                    "UnitsIds": self.rest,
                    "stopType": "1",
                    "productOrIngredientStopReasons": [
                        "0",
                        "1",
                        "2",
                        "3",
                        "4",
                        "5",
                        "6"
                    ],
                    "beginDate": self.date_end - timedelta(days=delta_days[self.tps]),
                    "endDate": self.date_end
                }
            }
        }
        self.save(orders_data)

    def being_stop_ingredient(self):
        delta_days = {
            'day': 0,
            'week': 6,
        }
        orders_data = {
            'being_stop_ingredient': {
                'link': f'https://officemanager.dodopizza.{self.code}/Reports/StopSaleStatistic/Export',
                'data': {
                    "UnitsIds": self.rest,
                    "stopType": "2",
                    "productOrIngredientStopReasons": [
                        "0",
                        "1",
                        "2",
                        "3",
                        "4",
                        "5",
                        "6"
                    ],
                    "beginDate": self.date_end - timedelta(days=delta_days[self.tps]),
                    "endDate": self.date_end
                }
            }
        }
        self.save(orders_data)

    def driving_couriers(self):
        delta_days = {
            'day': 0,
            'week': 6,
        }
        orders_data = {
            'driving_couriers': {
                'link': f'https://officemanager.dodopizza.{self.code}/Reports/CourierTasks/Export',
                'data': {
                    "unitId": self.rest,
                    "beginDate": self.date_end - timedelta(days=delta_days[self.tps]),
                    "endDate": self.date_end,
                    "statuses": [
                        "Ordering"
                    ]
                }
            }
        }
        self.save(orders_data)

    def lunch(self):
        delta_days = {
            'day': 0,
            'week': 6,
        }
        orders_data = {
            'lunch': {
                'link': f'https://officemanager.dodopizza.{self.code}/OfficeManager/MaterialConsumption/ExportReportByDays/Export',
                'data': {
                    "SelectedUnitIds": self.rest,
                    "ReportType": "20",
                    "CurrentViewType": "ByDay",
                    "DatePeriodStart": self.date_end - timedelta(days=delta_days[self.tps]),
                    "DatePeriodEnd": self.date_end,
                    "SelectedWeekDays": [
                        "Monday",
                        "Tuesday",
                        "Wednesday",
                        "Thursday",
                        "Friday",
                        "Saturday",
                        "Sunday"
                    ],
                    "IsVatIncluded": [
                        "true",
                        "false"
                    ]
                }
            }
        }
        self.save(orders_data)

    def average_check(self):
        delta_days = {
            'day': 0,
            'week': 6,
        }
        orders_data = {
            'average_check': {
                'link': f'https://officemanager.dodopizza.{self.code}/Reports/AverageCheck/Export',
                'data': {
                    "unitsIds": self.rest,
                    "orderSources": [
                        "Telephone",
                        "Site",
                        "Restaurant",
                        "Mobile",
                        "Pizzeria",
                        "Aggregator"
                    ],
                    "beginDate": self.date_end - timedelta(days=delta_days[self.tps]),
                    "endDate": self.date_end,
                    "orderTypes": "Delivery"
                }
            }
        }
        self.save(orders_data)

    def statistic_rest(self):
        delta_days = {
            'day': 0,
            'week': 6,
        }
        orders_data = {
            'app_share': {
                'link': f'https://officemanager.dodopizza.{self.code}/Reports/RestaurantAppShare/Export',
                'data': {
                    "unitsIds": self.rest,
                    "beginDate": self.date_end - timedelta(days=delta_days[self.tps]),
                    "endDate": self.date_end
                }
            }
        }
        self.save(orders_data)
