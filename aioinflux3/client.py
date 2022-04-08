import logging
import httpx
import urllib
import time
import datetime
import numpy as np
from typing import List

from sqlalchemy import column
from aioinflux3.line_protocal import Measurement, QueryBody, Query

UTC_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
UTF_SHORT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

class InfluxClient:
    def __init__(self, host:str = '127.0.0.1', port: int = 8086, token: str = '', bucket: str = '', org: str = ''):
        self.url_base = f'http://{host}:{port}/api/v2'
        self.headers = {
            "Accept": "application/json",
            "Authorization": f"Token {token}"
        }
        self.bucket = bucket
        self.org = org

    def query_string(self):
        return urllib.parse.urlencode({
            'bucket': self.bucket,
            'org': self.org,
            'precision': 'ms'
        })

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    def url_for(self, path: str) -> str:
        return f"{self.url_base}{path}?{self.query_string()}"

    async def write(self, data: Measurement):
        post_url = self.url_for('/write')
        logging.error('will post to:%s', post_url)
        async with httpx.AsyncClient() as client:
            resp = await client.post(post_url, content=data.serialize(), headers=self.headers)
            logging.error(resp.text)

    def table_response(self, lines: List[str]) -> dict:
        header = lines.pop(0).split(',')[2:]
        tbody = []
        for row in lines:
            cols = [col.strip() for col in row.split(',')[2:]]
            tbody.append(cols)
        return dict(header=header, tbody=tbody)

    def _trans_value(self, val, idx, dtype):
        tp = dtype[idx]
        if tp == 'int64':
            return int(val)
        if tp == 'float64' or tp == 'float32':
            return float(val)
        return val

    def numpy_response(self, lines: List[str]) -> np.ndarray:
        lines.pop(0)
        columns = {
            'ts':[],
        }
        columns_type = {}
        dtype = [('ts', 'float32')]
        for row in lines:
            cols = [col.strip() for col in row.split(',')]
            if len(cols)<2:
                continue
            _tb = cols[2]
            _dt = datetime.datetime.strptime(cols[5], UTC_FORMAT) if len(
                cols[5]) > 20 else datetime.datetime.strptime(cols[5], UTF_SHORT_FORMAT)
            _ts = time.mktime(_dt.timetuple()) + float("0.%s" % _dt.microsecond)
            _v = cols[6]
            _f = cols[7]
            columns['ts'].append(_ts)
            if _f in columns:
                columns[_f].append(_v)
            else:
                tp = columns_type.get(_tb)
                if not tp:
                    # 没有检测类型，开始检测类型
                    if _v.isdigit():
                        tp = 'int64'
                    elif len([1 for _n in _v.split('.') if _n.isdigit()]) == 2:
                        tp = 'float64'
                    else:
                        tp = 'S16'
                    columns_type[_tb] = tp
                if  _f in columns:
                    columns[_f].append(_v)
                else:
                    dtype.append((_f, tp))
                    columns[_f] = [_v, ]
        columns['ts'] = columns['ts'][:len(columns[_f])]
        data_columns = []
        for c_name, c_tp in dtype:
            data_columns.append(tuple(columns[c_name]))
        arr = np.rot90(np.array(data_columns))
        return np.array([tuple([self._trans_value(col, _idx, dtype) for _idx, col in enumerate(row)]) for row in arr.tolist()], dtype=dtype)


    async def query(self, query: Query, numpy=False):
        post_url = self.url_for('/query')
        body = QueryBody(params={}, query=query.to_query())
        async with httpx.AsyncClient() as client:
            resp = await client.post(post_url, json=body.serialize(), headers=self.headers)
            lines = resp.text.split('\r\n')
            if numpy:
                return self.numpy_response(lines)
            else:
                return self.table_response(lines)
