last_week_query = f"""
                    SELECT store_id, day, start_time_local, end_time_local, count(*) FROM 
                        (SELECT st_hr.store_id, st_hr.day, st_hr.start_time_local, st_hr.end_time_local FROM store_status as st_status 
                        JOIN 
                        (SELECT * FROM store_hours) AS st_hr 
                        ON
                            st_hr.store_id = st_status.store_id and st_hr.day = st_status.day 
                        WHERE 
                            st_status.status = "active" and st_status.poll_time between st_hr.start_time_local and st_hr.end_time_local 
                            and 
                            st_status.poll_date_utc between '{last_week_start}' and '{last_week_end}')
                        GROUP BY store_id, day;
                    """
last_day_query = f"""
                    SELECT store_id, start_time_local, end_time_local, count(*) FROM 
                        (SELECT st_hr.store_id, st_hr.day, st_hr.start_time_local, st_hr.end_time_local FROM store_status AS st_status 
                        JOIN 
                        (SELECT * FROM store_hours) AS st_hr 
                        ON 
                            st_hr.store_id = st_status.store_id and st_hr.day = st_status.day 
                        WHERE 
                            st_status.status = "active" and st_status.poll_time between st_hr.start_time_local and st_hr.end_time_local 
                            and 
                            st_status.poll_date_utc between '{last_day_start}' and '{last_day_end}') 
                        GROUP BY store_id, day;
                """
last_hr_query = f"""
                    SELECT store_id, start_time_local, end_time_local, count(*) FROM 
                        (SELECT st_hr.store_id, st_hr.day, st_hr.start_time_local, st_hr.end_time_local FROM store_status AS st_status 
                        JOIN 
                        (SELECT * FROM store_hours) AS st_hr 
                        ON 
                            st_hr.store_id = st_status.store_id and st_hr.day = st_status.day 
                        WHERE 
                            st_status.status = "active" and st_status.poll_time between st_hr.start_time_local and st_hr.end_time_local 
                            and 
                            st_status.poll_date_utc between '{last_hr_start}' and '{last_hr_end}') 
                        GROUP BY store_id, day;
                """
