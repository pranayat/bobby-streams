import pandas as pd
import redis
import json
# import debugpy

# debugpy.wait_for_client()

def produce_to_redis(df, r):
    try:
        for index, row in df.head(1000).iterrows():
            if index % 3 == 0:
                stream = "stream_1"
            elif index % 3 == 1:
                stream = "stream_2"
            elif index % 3 == 2:
                stream = "stream_3"                

            r.xadd(stream, {"data": row.to_json()}, maxlen=1000, approximate=True)

    except Exception as e:
        print(f"Error producing to redis: {str(e)}")

with open('./states_2019-07-22-23_1000.json', 'r') as file:
    data = json.load(file)
    df = pd.json_normalize(data)

    r = redis.StrictRedis(host='redis', port=6379, decode_responses=True)

    while True:
        produce_to_redis(df, r)
