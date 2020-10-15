import os
from pathlib import Path

import sys
import csv
from variables import instruments_list
root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(''))))
sys.path.append(root + '/python')
from time import sleep
import ccxt


def retry_fetch_ohlcv(exchange, max_retries, symbol, timeframe, since, limit):
	num_retries = 0
	while num_retries < max_retries :
		try:
			num_retries += 1
			ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since, limit)
			return ohlcv
		except Exception:
			sleep(2)
			print("retry")
			if num_retries > max_retries:
				raise Exception


def scrape_ohlcv(exchange, max_retries, symbol, timeframe, since, end, limit):
	timeframe_duration_in_seconds = exchange.parse_timeframe(timeframe)
	timeframe_duration_in_ms = timeframe_duration_in_seconds * 1000
	timedelta = limit * timeframe_duration_in_ms
	all_ohlcv = []
	while True:
		fetch_since = end - timedelta
		ohlcv = retry_fetch_ohlcv(exchange, max_retries, symbol, timeframe, fetch_since, limit)
		# if we have reached the beginning of history
		if not ohlcv:
			break
		if len(ohlcv)<1:
			break
		if ohlcv[0][0] >= end:
			break
		end = ohlcv[0][0]
		all_ohlcv = ohlcv + all_ohlcv
		print(len(all_ohlcv), symbol, 'candles in total from', exchange.iso8601(all_ohlcv[0][0]), 'to', exchange.iso8601(all_ohlcv[-1][0]))
		# if we have reached the checkpoint
		if fetch_since < since:
			break
	return all_ohlcv


def write_to_csv(filename, symbol, data):
	p = Path("./data/", str(symbol.replace("/","_")))
	p.mkdir(parents=True, exist_ok=True)
	full_path = p / str(filename)
	with Path(full_path).open('w+', newline='') as output_file:
		csv_writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
		csv_writer.writerows(data)

def parse_ohlc(row,exchange):
	val=row.copy()
	val[0]=exchange.iso8601(row[0])
	return val 

def scrape_candles_to_csv(filename, exchange_id, max_retries, symbol, timeframe, start_str, end_str, limit):
	# instantiate the exchange by id
	exchange = getattr(ccxt, exchange_id)({
		'enableRateLimit': True,  # required by the Manual
	})
	# convert since from string to milliseconds integer if needed
	if isinstance(start_str, str):
		start = exchange.parse8601(start_str)
	# preload all markets from the exchange
	exchange.load_markets()
	# fetch all candles
	end = exchange.parse8601(end_str)
	ohlcv = scrape_ohlcv(exchange, max_retries, symbol, timeframe, start, end, limit)
	if start in [row[0] for row in ohlcv]:
		ohlcv = ohlcv[[row[0] for row in ohlcv].index(start):]
	ohlcv_parsed= [parse_ohlc(row,exchange) for row in ohlcv]
	# save them to csv file
	write_to_csv(filename, symbol, ohlcv_parsed)
	print('Saved', len(ohlcv), 'candles from', exchange.iso8601(ohlcv[0][0]), 'to', exchange.iso8601(ohlcv[-1][0]), 'to', filename)



def scrape_candles(symbol, timeframe, start,end):
	filename=timeframe+".csv"
	exchange_id='binance'
	max_retries=10
	limit=1000
	scrape_candles_to_csv(filename, exchange_id, max_retries, symbol, timeframe, start, end, limit)
def fetch_token_data(symbol,timeframe):
	start='2015-01-0100:00:00Z'
	end='2020-09-0100:00:00Z'
	scrape_candles(symbol, timeframe, start,end)

def main():
	for instrument in instruments_list:
		try:
			fetch_token_data(instrument, '15m')
		except Exception as e:
			print("failed fetch for token", instrument)
	# scrape_candles_to_csv('1m.csv', 'binance', 3, 'BTC/USDT', '1m', '2020-09-0100:00:00Z', 1000)
	# scrape_candles('BTC/USDT', '1m', '2020-09-0100:00:00Z','2020-09-1000:00:00Z')

if __name__ == '__main__':
	main()
