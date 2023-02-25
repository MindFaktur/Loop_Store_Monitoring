from datetime import datetime
from pytz import timezone
from sql_service import execute_sql, DB_CONN, CURSOR


def load_business_hours(file_path: str):
    table_name = "store_hours"
    create_query = f"""CREATE TABLE {table_name} (store_id TEXT,
                        day INTEGER,
                        start_time_local text,
                        end_time_local text,
                        PRIMARY KEY (store_id, day)
                    );"""
    execute_sql(create_query)
    insert_query = f"""INSERT or IGNORE INTO {table_name} VALUES (?,?,?,?);"""
    values = []
    try:
        with open(file_path, "r") as file_obj:
            for line in file_obj:
                line = line.split(",")
                if "store_id" in line:
                    continue
                line_items = [item.strip() for item in line]
                if line_items[-2] == "00:00:00" and line_items[-1] == "00:00:00":
                    print(line_items)
                    line_items[-1] = "24:00:00"
                values.append(tuple(line_items))
                if len(values) == 100:
                    execute_sql(insert_query, values, True)
            execute_sql(insert_query, values, True)
    except Exception as e:
        print(e)


def load_timezones(file_path: str):
    table_name = "store_timezones"
    create_query = f"""CREATE TABLE {table_name}(store_id TEXT,
                        timezone text DEFAULT "America/Chicago"
                    );"""
    execute_sql(create_query)

    insert_query = f"""INSERT INTO {table_name} VALUES (?,?);"""
    values = []
    try:
        with open(file_path, "r") as file_obj:
            for line in file_obj:
                line = line.split(",")
                if "store_id" in line:
                    continue
                line_items = [item.strip() for item in line]
                values.append(tuple(line_items))
                if len(values) == 100:
                    execute_sql(insert_query, values, True)

            execute_sql(insert_query, values, True)
    except Exception as e:
        print(e)


def load_store_status(file_path: str):
    table_name = "store_status"
    create_query = f"""CREATE TABLE {table_name}(store_id TEXT,
                        status TEXT,
                        poll_date_utc DATE,
                        poll_time,
                        day INTEGER
                    );"""
    execute_sql(create_query)
    tz_data = {}
    insert_query = f"""INSERT INTO {table_name} VALUES (?,?,?,?,?);"""
    values = []
    try:
        with open("timezone.csv", "r") as tz_file:
            for line in tz_file:
                line = line.split(",")
                if "store_id" in line:
                    continue
                line_items = [item.strip() for item in line]
                tz_data[line_items[0]] = line_items[1]

        with open(file_path, "r") as file_obj:
            for line in file_obj:
                line = line.split(",")
                if "store_id" in line:
                    continue
                line_items = [item.strip() for item in line]
                str_date = line_items[-1].strip("UTC").split(".")
                date_val = datetime.strptime(str_date[0].strip(), "%Y-%m-%d %H:%M:%S")
                line_items[-1] = date_val
                tz_val = tz_data.get(line_items[0])
                if not tz_val:
                    tz_val = "America/Chicago"
                tz_change_date = date_val.astimezone(timezone(tz_val))
                line_items.append(tz_change_date.strftime("%H:%M:%S"))
                line_items.append(tz_change_date.weekday())
                values.append(tuple(line_items))
                if len(values) == 100:
                    execute_sql(insert_query, values, True)

            execute_sql(insert_query, values, True)

    except Exception as e:
        print(e)


if __name__ == "__main__":
    # load_business_hours("business_hours.csv")
    # load_store_status("store_status.csv")
    # load_timezones("timezone.csv")
    DB_CONN.close()
