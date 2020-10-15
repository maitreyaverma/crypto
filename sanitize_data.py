import pandas as pd
import os
timeframes=["15m"]
save_path="15m"
def main():
	instruments=os.listdir("data")
	for instrument in instruments:
		for timeframe in timeframes:
			path=os.path.join("data",instrument,timeframe+".csv")
			print(path)
			if os.path.exists(path):
				try:
					df=pd.read_csv(path)
					df.columns=["time","open","high","low","close","volume"]
					df=df.set_index("time").drop_duplicates()
					df.index=pd.to_datetime(df.index,format="%Y-%m-%dT%H:%M:%S.%fZ")
					df=df.sort_index()
					df.to_csv(os.path.join(save_path,instrument+".csv"))
				except Exception as e:
					print("failed for", path)

def sanitize_pair(timeframe,first,second):
	first_path = os.path.join(timeframe, str(first.replace("/", "_")) + ".csv")
	second_path = os.path.join(timeframe, str(second.replace("/", "_")) + ".csv")
	first_df=pd.read_csv(first_path)
	second_df=pd.read_csv(second_path)
	full = pd.merge(first_df,second_df,on='time',suffixes=('_1','_2'),how='outer')
	full['time'] = pd.to_datetime(full['time'], format="%Y-%m-%d %H:%M:%S.%f")
	full.sort_values(by='time', inplace=True)

	first_df = full[['time','open_1','high_1','low_1','close_1','volume_1']]
	second_df = full[['time','open_2','high_2','low_2','close_2','volume_2']]

	first_df.columns = ["time","open","high","low","close","volume"]
	second_df.columns = ["time","open","high","low","close","volume"]

	first_df = first_df.set_index("time")
	second_df = second_df.set_index("time")

	first_df["volume"]=first_df["volume"].fillna(0)
	first_df["close"]=first_df["close"].ffill()
	first_df=first_df.bfill(axis=1)

	second_df["volume"] = second_df["volume"].fillna(0)
	second_df["close"] = second_df["close"].ffill()
	second_df = second_df.bfill(axis=1)

	first_df.to_csv(first_path)
	second_df.to_csv(second_path)

if __name__ == '__main__':
	main()