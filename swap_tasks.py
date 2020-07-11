# swap_tasks.py
"""
To start the app:
    celery -A swap_tasks worker -B -l info
"""
from apis import Telegram, Liquid
from celery import Celery, chain
from celery.schedules import crontab
from celery.utils.log import get_task_logger
from celery.signals import after_setup_task_logger
from dotenv import load_dotenv
import os
import psycopg2
import time
from datetime import datetime
import logging
import traceback
import sys

logger = get_task_logger(__name__)

# Constants
load_dotenv()
HIGH_RATE = os.getenv('HIGH_RATE')
PERIOD = int(os.getenv('PERIOD'))

# Initialize objects
swap_app = Celery('tasks', backend='redis://localhost:6379/0', broker='pyamqp://guest@localhost//')
lq = Liquid(api_key=os.getenv('API_KEY_LIQUID'), api_secret=os.getenv('SECRET_KEY_LIQUID'))
tg = Telegram(chat_id=os.getenv('CHAT_ID'), bot_token=os.getenv('BOT_TOKEN'))

# Initialize database
try:
    con = psycopg2.connect(database=os.getenv('DATABASE'), host=os.getenv('PSQL_HOST'),
                           user=os.getenv('PSQL_USER'), password=os.getenv('PSQL_PASS'))
    cursorObj = con.cursor()
except Exception as err:
    """
    stop the app if it can't connect to the database
    """
    logging.error('Can\'t connect to the database')
    logging.error(err)
    logging.error(traceback.format_exc())
    sys.exit()
cursorObj.execute(
    'CREATE TABLE IF NOT EXISTS hourly_average (id serial PRIMARY KEY, time_stamp text, funding_rate real, start_time real, end_time real)')
con.commit()

# Schedule tasks
swap_app.conf.beat_schedule = {
    'get-swap-data': {
        'task': 'swap_task.workflow',
        'schedule': crontab(minute='0', hour='*', day_of_week='*'),
    },
}


# Add file logging
@after_setup_task_logger.connect
def setup_task_logger(logger, *args, **kwargs):
    log_format = '%(asctime)s:%(levelname)s:%(name)s: %(message)s'
    fh = logging.FileHandler('celery_logs.log')
    fh.setFormatter(logging.Formatter(log_format))
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)
    logger.setLevel(logging.INFO)


@swap_app.task(name='swap_task.collect_data')
def collect_data():
    data = []
    start = time.time()
    while time.time() - start < 60:
        now = time.time()
        try:
            market_info = lq.get_product(product_id=604).json()
        except Exception as err:
            logger.error('Couldn\'t get liquid market data')
            logger.error(err)
            logger.error(traceback.format_exc())
            continue
        timestamp = float(market_info['timestamp'])
        funding_rate = float(market_info['funding_rate'])
        data.append({'timestamp': timestamp, 'funding_rate': funding_rate})
        logger.debug(f'time: {time.time()}  timestamp: {timestamp}  funding_rate: {funding_rate}')
        execution_time = time.time() - now
        time.sleep(max((PERIOD - execution_time), 0))
    logger.info(f'data: {data[0]} ... {data[-1]}')
    return data


@swap_app.task(name='swap_task.swap_average')
def swap_average(data):
    ts_start = data[0]['timestamp']
    ts_end = data[-1]['timestamp']
    weighted = 0
    """
    get time weighted average of data points
    """
    prev_ts = ts_start
    for i in data[1:]:
        weighted += i['funding_rate'] * (i['timestamp'] - prev_ts)
        prev_ts = i['timestamp']
    average = weighted / (ts_end - ts_start)
    try:
        timestamp_readable =datetime.fromtimestamp(ts_end).strftime("%b %d %H:%M:%S")
        cursorObj.execute(
            "INSERT INTO hourly_average (time_stamp, funding_rate, start_time, end_time) VALUES (%s,%s,%s,%s)",
            (timestamp_readable, average, ts_start, ts_end))
        con.commit()
    except Exception as err:
        logger.error('Couldn\'t log average in database')
        logger.error(err)
        logger.error(traceback.format_exc())

    """
    send a telegram message with the swap rate
    """
    msg = f'Average hourly swap rate is {average:.2%}'
    tg.send_message(msg)
    logger.info(f'sent message: {msg}')


@swap_app.task(name='swap_task.workflow')
def workflow():
    chain(collect_data.s(), swap_average.s())()
    logger.info('new time period starting')
