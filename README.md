# Loop_Store_Monitoring
Backend functionality to monitor the restaurant business hours and generate report

How to Install?
1) pip install requirements.txt
2) Run load_to_db.py file by providing the csv files
3) uvicorn main:app --reload
This will load the data and start the service.

Visit "http://127.0.0.1:8000/trigger_report" to trigger the report generation
Visit "http://127.0.0.1:8000/get_report/{report_id got from trigger_report}" : This will send the output csv itself.
