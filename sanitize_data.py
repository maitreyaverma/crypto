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


if __name__ == '__main__':
	main()