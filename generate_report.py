from datetime import datetime, timezone, timedelta
from sql_service import execute_sql, DB_CONN
import os


class GenerateReport:
    store_result = {}

    def get_report(self, report_id: str):
        if os.path.exists(report_id):
            return True
        else:
            return False

    def calculate_hrs(self, query: str, active: str, inactive: str):
        """
        Execute the given query and extract the values from the result
        """
        try:
            result = execute_sql(query)

            for item in result.fetchall():
                active_hrs = 0
                inactive_hrs = 0
                if active == "uptime_last_week":
                    store_id, day, start_time, end_time, count = item
                else:
                    store_id, start_time, end_time, count = item

                if not self.store_result.get(store_id, ""):
                    self.store_result[store_id] = {}

                hours_open = int(end_time[0:2]) - int(start_time[0:2])
                active_hrs = count

                if hours_open > 0:
                    if hours_open != count:
                        inactive_hrs = abs(hours_open - count)
                else:
                    hours_open = 1

                if self.store_result.get(store_id, {}):
                    if self.store_result.get(store_id, {}).get(active):
                        self.store_result[store_id][active] += active_hrs
                        self.store_result[store_id][inactive] += inactive_hrs
                    else:
                        self.store_result[store_id][active] = active_hrs
                        self.store_result[store_id][inactive] = inactive_hrs
                else:
                    self.store_result[store_id] = {
                        active: active_hrs,
                        inactive: inactive_hrs,
                    }

        except Exception as e:
            print(item)
            raise e

    def gen_report(self):
        """
        Taking the latest date from store status so that we can calcualte the required data before latest date.
        Not taking the current date because the latest date in the given csv is 2023-1-25.
        Using SQL query to complete the calculations insteadd of just querying the data and processing in python as it's more efficient.
        """
        api_call_date = datetime.now().date()
        try:
            if os.path.exists(f"{api_call_date}.csv"):
                store_result = {}
                return {
                    "success": "True",
                    "message": "File already exisits",
                    "output_file": f"{api_call_date}.csv",
                }
        except Exception as e:
            print(e)
        output_file = open(f"{api_call_date}.csv", "a")
        max_date = execute_sql("""SELECT max(poll_date_utc) from store_status; """)
        for item in max_date:
            latest_date = item[0]
            break
        latest_date = datetime.strptime(latest_date, "%Y-%m-%d %H:%M:%S")
        weekday = latest_date.weekday()
        if weekday > 0:
            last_week_end = latest_date - timedelta(weekday)
            last_week_start = last_week_end - timedelta(7)
        else:
            last_week_end = latest_date
            last_week_start = last_week_end - timedelta(7)

        last_day_start = latest_date.replace(hour=00, minute=00, second=00)
        last_day_end = latest_date.replace(hour=23, minute=59, second=59)

        last_hr_start = latest_date - timedelta(hours=2)
        last_hr_end = latest_date

        last_week_query = f"""select store_id, day, start_time_local, end_time_local, count(*) from (select st_hr.store_id, st_hr.day, st_hr.start_time_local, st_hr.end_time_local from store_status as st_status join (select * from store_hours) as st_hr on st_hr.store_id = st_status.store_id and st_hr.day = st_status.day where st_status.status = "active" and st_status.poll_time between st_hr.start_time_local and st_hr.end_time_local and st_status.poll_date_utc between '{last_week_start}' and '{last_week_end}') group by store_id, day;"""
        last_day_query = f"""select store_id, start_time_local, end_time_local, count(*) from (select st_hr.store_id, st_hr.day, st_hr.start_time_local, st_hr.end_time_local from store_status as st_status join (select * from store_hours) as st_hr on st_hr.store_id = st_status.store_id and st_hr.day = st_status.day where st_status.status = "active" and st_status.poll_time between st_hr.start_time_local and st_hr.end_time_local and st_status.poll_date_utc between '{last_day_start}' and '{last_day_end}') group by store_id, day;"""
        last_hr_query = f"""select store_id, start_time_local, end_time_local, count(*) from (select st_hr.store_id, st_hr.day, st_hr.start_time_local, st_hr.end_time_local from store_status as st_status join (select * from store_hours) as st_hr on st_hr.store_id = st_status.store_id and st_hr.day = st_status.day where st_status.status = "active" and st_status.poll_time between st_hr.start_time_local and st_hr.end_time_local and st_status.poll_date_utc between '{last_hr_start}' and '{last_hr_end}') group by store_id, day;"""

        self.calculate_hrs(last_week_query, "uptime_last_week", "downtime_last_week")
        self.calculate_hrs(last_day_query, "uptime_last_day", "downtime_last_day")
        self.calculate_hrs(last_hr_query, "uptime_last_hour", "downtime_last_hour")

        columns = "store_id, uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day, downtime_last_week"

        output_file.write(columns + "\n")
        for status_key, status_val in self.store_result.items():
            output_file.write(
                f"{status_key}, {status_val.get('uptime_last_hour', 0)}, {status_val.get('uptime_last_day', 0)}, {status_val.get('uptime_last_week', 0)}, {status_val.get('downtime_last_hour', 0)}, {status_val.get('downtime_last_day', 0)}, {status_val.get('downtime_last_week', 0)} \n"
            )
        self.store_result = {}

        return {
            "success": "True",
            "message": "Report File Generated",
            "output_file": f"{api_call_date}.csv",
        }


if __name__ == "__main__":
    GenerateReport().gen_report()
