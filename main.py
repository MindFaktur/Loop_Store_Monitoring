from fastapi import FastAPI
from fastapi.responses import FileResponse

from generate_report import GenerateReport

app = FastAPI()


@app.get("/trigger_report")
def trigger_report():
    return GenerateReport().gen_report()


@app.get("/get_report/{report_id}")
def get_report(report_id: str):
    if GenerateReport().get_report(report_id):
        return FileResponse(report_id)
    else:
        return {
            "success": "False",
            "message": "Report id does not exist, please generate new",
        }
