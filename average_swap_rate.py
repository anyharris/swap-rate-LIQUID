# average_swap_rate.py
from apis import Telegram, Liquid
from dotenv import load_dotenv
import os
import psycopg2
import time
from datetime import datetime
import logging
import traceback
import sys

# Logging
FORMAT = '%(asctime)s:%(levelname)s:%(name)s: %(message)s'
logging.basicConfig(format=FORMAT, filename='logging.log', level=logging.INFO)

# Constants
load_dotenv()
SAMPLING_PERIOD = int(os.getenv('SAMPLING_PERIOD'))
AVERAGE_PERIOD = int(os.getenv('AVERAGE_PERIOD'))

# Initialize objects
lq = Liquid()
tg = Telegram(chat_id=os.getenv('CHAT_ID'), bot_token=os.getenv('BOT_TOKEN'))

# Initialize database connection and table
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
cur = con.cursor()
cur.execute(
    'CREATE TABLE IF NOT EXISTS hourly_average (id serial PRIMARY KEY, time_stamp text, funding_rate real, start_time real, end_time real)')

# Collect data
data = []
start = time.time()
while time.time() - start < AVERAGE_PERIOD:
    now = time.time()
    try:
        market_info = lq.get_product(product_id=604).json()
    except Exception as err:
        logging.error('Couldn\'t get liquid market data')
        logging.error(err)
        logging.error(traceback.format_exc())
        continue
    timestamp = float(market_info['timestamp'])
    funding_rate = float(market_info['funding_rate'])
    data.append({'timestamp': timestamp, 'funding_rate': funding_rate})
    logging.debug(f'time: {time.time()}  timestamp: {timestamp}  funding_rate: {funding_rate}')
    execution_time = time.time() - now
    time.sleep(max((SAMPLING_PERIOD - execution_time), 0))
logging.info(f'data: {data[0]} ... {data[-1]}')

# Process data
now = time.time()
ts_start = data[0]['timestamp']
ts_end = data[-1]['timestamp']
weighted = 0
prev_ts = ts_start
for i in data[1:]:
    weighted += i['funding_rate'] * (i['timestamp'] - prev_ts)
    prev_ts = i['timestamp']
average = weighted / (ts_end - ts_start)

# Store in database
try:
    timestamp_readable = datetime.fromtimestamp(now).strftime("%b %d %H:%M:%S")
    cur.execute(
        "INSERT INTO hourly_average (time_stamp, funding_rate, start_time, end_time) VALUES (%s,%s,%s,%s)",
        (timestamp_readable, average, ts_start, ts_end))
except Exception as err:
    logging.error('Couldn\'t log average in database')
    logging.error(err)
    logging.error(traceback.format_exc())

# Send a telegram message
msg = f'Average hourly swap rate is {average:.3%}'
tg.send_message(msg)
logging.info(f'sent message: {msg}')

# Close database connection
con.close()
