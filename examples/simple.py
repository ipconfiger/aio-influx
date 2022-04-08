
import sys
sys.path.append('../')
import time
import asyncio
import random


async def test_main():
    from aioinflux3 import InfluxClient, Measurement, Query, Field
    token = 'iXQLxmz6VjNIHQNjFicwAdzk6wriDomL8HH8B4m00ilDrfxWQDBPvFWmlskzeX5NtM9OLFM55ID8MPyniU6qLQ=='
    async with InfluxClient(bucket='server-logs', org='mm-tech', token=token) as client:
        for i in range(30):
            ts = int(time.time()*1000)
            data = Measurement.new('tokens',
                                   ts, 
                                   tag=[Field(key="coin",
                                              val="BTC"), ],
                                   fields=[
                                       Field(key="price",
                                             val=random.randint(100, 130)),
                                       Field(key="open",
                                             val=random.randint(100, 150)),
                                       Field(key="close",
                                             val=random.randint(10, 20)),
                                   ]
                                   )
            await client.write(data)
            print('write data:', data)
            time.sleep(2)
        resp = await client.query(Query('server-logs', measurement='tokens').\
                range(start='-4h').\
                    filter('price', coin="BTC").\
                        window(every='5s', fn='last').\
                            do_yield(name='last'), numpy=True)
        print('resp:', resp['price'])

        

def main():
    asyncio.run(test_main())


if __name__ == "__main__":
    main()
