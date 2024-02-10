import pandas as pd
import redis
import json
import debugpy
# debugpy.listen(5678)
debugpy.wait_for_client()

def produce_to_redis(df, r):
    try:
        for _, row in df.head(10).iterrows():
            r.xadd("flight_data", {"data": row.to_json()})

    except Exception as e:
        print(f"Error producing to redis: {str(e)}")

with open('./states_2019-07-22-23_1000.json', 'r') as file:
    data = json.load(file)
    df = pd.json_normalize(data)

    r = redis.StrictRedis(host='redis', port=6379, decode_responses=True)

    produce_to_redis(df, r)
