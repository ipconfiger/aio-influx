# aio-influx
influxdb python client working with asyncio

## Install

    pip install aioinflux3

## Initialize asynchronous client

    from aioinflux3 import InfluxClient, Measurement, Query, Field

    async with InfluxClient(host=..., port=..., org=..., token=...) as client:
        # write code here


##  Write data

    async with InfluxClient(host=..., port=..., org=..., token=...) as client:
        await client.write(Measurement.new('measurement',
                                   timestamp, 
                                   tag=[Field(key=tag key,
                                              val=tag value), ],
                                   fields=[
                                       Field(key=filed key,
                                             val=field value),
                                   ]
                            )
                          )


## Make Query

    async with InfluxClient(host=..., port=..., org=..., token=...) as client:
        query = Query(bucket name, measurement=measurement name)
                  .range(start='-4h', end='-1s')
                  .filter('_field name', tag name= tag value)
                  .window(every='5s', fn='func name') # func name in next list
                  .do_yield(name='func name') # name if optional
        resp = await client.query(query) # return json table
        resp = await client.query(query, numpy=True) # return a numpy Structured Array


func list:
1. mean
2. median
3. max
4. min
5. sum
6. derivative
7. distinct
8. count
9. increase
10. skew
11. spread
12. stddev
13. first
14. last
15. unique
16. sort
17. nonnegative derivative1


## Notice
This project started for own useage. My goal is make it simple and easy to use, it's not full functional for InfluxDB v2 API. If you like it, Pls star, and tell me yourneeds in issuse, I will try my best to make it happen. 