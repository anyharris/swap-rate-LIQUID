# swap_tasks.py
"""
To start worker:
nohup celery -A swap_tasks worker -B -l info -Q swaps -n swap_worker@%h > celery.log &

to show celery queue:
sudo rabbitmqctl list_queues

to purge celery queue:
celery -A tasks purge -Q swaps
"""
from apis import Telegram, Liquid
from celery import Celery, chain
from celery.schedules import crontab
from celery.utils.log import get_task_logger
from celery.signals import after_setup_task_logger, worker_process_init, worker_process_shutdown
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
swap_app.conf.update({
    'task_routes': {
        'swap_tasks.workflow': {'queue': 'swaps'},
        'swap_tasks.collect_data': {'queue': 'swaps'},
        'swap_tasks.swap_average': {'queue': 'swaps'},
    }})
lq = Liquid()
tg = Telegram(chat_id=os.getenv('CHAT_ID'), bot_token=os.getenv('BOT_TOKEN'))

# Initialize database
try:
    con = psycopg2.connect(database=os.getenv('DATABASE'), host=os.getenv('PSQL_HOST'),
                           user=os.getenv('PSQL_USER'), password=os.getenv('PSQL_PASS'))
    con.autocommit = True
except Exception as err:
    """
    stop the app if it can't connect to the database
    """
    logging.error('Can\'t connect to the database')
    logging.error(err)
    logging.error(traceback.format_exc())
    sys.exit()
cursorObj = con.cursor()
cursorObj.execute(
    'CREATE TABLE IF NOT EXISTS hourly_average (id serial PRIMARY KEY, time_stamp text, funding_rate real, start_time real, end_time real)')
con.close()

# Schedule tasks
swap_app.conf.beat_schedule = {
    'get-swap-data': {
        'task': 'swap_tasks.workflow',
        'schedule': crontab(minute='0', hour='*', day_of_week='*'),
    },
}


# Have each worker connect to the database
@worker_process_init.connect
def init_worker(**kwargs):
    global con
    print('Initializing database connection for worker.')
    con = psycopg2.connect(database=os.getenv('DATABASE'), host=os.getenv('PSQL_HOST'),
                           user=os.getenv('PSQL_USER'), password=os.getenv('PSQL_PASS'))
    con.autocommit = True


# Close the connection when the worker shuts down
@worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    global con
    if con:
        print('Closing database connectionn for worker.')
        con.close()


# Add file logging
@after_setup_task_logger.connect
def setup_task_logger(logger, *args, **kwargs):
    log_format = '%(asctime)s:%(levelname)s:%(name)s: %(message)s'
    fh = logging.FileHandler('celery_logs.log')
    fh.setFormatter(logging.Formatter(log_format))
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)
    logger.setLevel(logging.INFO)


@swap_app.task(name='swap_tasks.collect_data')
def collect_data():
    data = []
    start = time.time()
    while time.time() - start < 60*60:
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


@swap_app.task(name='swap_tasks.swap_average')
def swap_average(data):
    now = time.time()
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
        timestamp_readable = datetime.fromtimestamp(now).strftime("%b %d %H:%M:%S")
        cur = con.cursor()
        cur.execute(
            "INSERT INTO hourly_average (time_stamp, funding_rate, start_time, end_time) VALUES (%s,%s,%s,%s)",
            (timestamp_readable, average, ts_start, ts_end))
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


@swap_app.task(name='swap_tasks.workflow')
def workflow():
    chain(collect_data.s(), swap_average.s())()
    logger.info('new time period starting')
