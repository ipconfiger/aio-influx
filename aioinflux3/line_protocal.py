from dataclasses import dataclass
import logging
from typing import Any, List, Optional

from aioinflux3.exceptions import LogicException


@dataclass
class Field:
    key: str
    val: Any

    def serialize(self, force_noquot=False):
        if force_noquot:
            return f'{self.key}={self.val}'
        elif isinstance(self.val, str):
            return f'{self.key}="{self.val}"'
        else:
            return f'{self.key}={self.val}'

@dataclass
class Measurement:
    name: str
    tag: Optional[List[Field]]
    fields: Optional[List[Field]]
    ts: int
    
    @classmethod
    def new(cls, name, ts, tag: List[Field] = None, fields: List[Field] = None):
        return cls(name=name, tag=tag, fields=fields, ts=ts)
    
    def serialize(self):
        tag_str = ",".join([field.serialize(force_noquot=True)
                           for field in self.tag]) if self.tag else ""
        filed_set = ",".join([field.serialize() for field in self.fields])
        if tag_str:
            return f'{self.name},{tag_str} {filed_set} {self.ts}\n'
        else:
            return f'{self.name} {filed_set} {self.ts}\n'


@dataclass
class QueryBody:
    params: dict
    query: str

    def serialize(self):
        return {
            'params': self.params,
            'query': self.query
        }


class Query:
    def __init__(self, bucket, measurement=''):
        self.lines = []
        self.measurement = measurement
        self.lines.append(f'from(bucket: "{bucket}")')
        self.apply_range = False
        self.apply_filter = False
        self.apply_window = False
        self.apply_yield = False
        self.params = {}

    def range(self, start: str = '', end: str ='') -> 'Query':
        # self.params['start'] = start
        # self.params['end'] = end
        if not end:
            self.lines.append(f"  |> range(start: {start} )")
        else:
            self.lines.append(f"  |> range(start: {start}, stop: {end})")
        self.lines.append(
            '  |> filter(fn: (r)=> r["_measurement"] == "tokens")')
        self.apply_range = True
        return self
    
    def filter(self, col: str, **conditions) -> 'Query':
        if not self.apply_range:
            raise LogicException('Must apply range first')
        self.lines.append(f'  |> filter(fn: (r) => r["_field"] == "{col}")')
        if conditions:
            for k, v in conditions.items():
                self.lines.append(f'  |> filter(fn: (r) => r["{k}"] == "{v}")')
        self.apply_filter = True
        return self

    def window(self, every='', fn='', createEmpty=False) -> 'Query':
        if not self.apply_range:
            raise LogicException('Must apply range first')
        if not self.apply_filter:
            raise LogicException('Must apply filter first')
        self.lines.append(
            f'  |> aggregateWindow(every: {every}, fn: {fn}, createEmpty: {"true" if createEmpty else "false"})')
        self.apply_window = True
        return self
    
    def do_yield(self, name='') -> 'Query':
        if not self.apply_range:
            raise LogicException('Must apply range first')
        if not self.apply_filter:
            raise LogicException('Must apply filter first')
        if name:
            self.lines.append(f'  |> yield(name: "{name}")')
        else:
            self.lines.append(f'  |> yield()')
        self.apply_yield = True
        return self

    def to_query(self) -> str:
        if not self.apply_range:
            raise LogicException('Must apply range first')
        if not self.apply_filter:
            raise LogicException('Must apply filter first')
        if not self.apply_yield:
            raise LogicException('Must yield')
        query = "\n".join(self.lines)
        return query


    


    
        
        

