from fastapi import FastAPI
from fastapi.responses import FileResponse

from generate_report import gen_report

app = FastAPI()


@app.get("/trigger_report")
def trigger_report():
    return gen_report()


@app.get("/get_report/{report_id}")
def get_report(report_id: str):
    return FileResponse(report_id)
