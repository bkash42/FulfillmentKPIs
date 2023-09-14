import datetime
import logging
import os
import azure.functions as func
from dotenv import load_dotenv
from FulfillmentKPIsV1 import calculate_kpi

app = func.FunctionApp()

@app.schedule(schedule="0 */5 * * * *", arg_name="test_timer", run_on_startup=False) 
def test_function(test_timer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    calculate_kpi()

    if test_timer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
