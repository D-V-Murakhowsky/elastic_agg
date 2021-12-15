from elasticsearch import Elasticsearch
from elasticsearch_dsl import (Search, A)
import logging
import pandas as pd
from pandas import DataFrame
from random import choices
from dataclasses import dataclass, asdict
from typing import (Dict, Any)

CONTENT_FEATURES = ['Wine', 'Producer', 'Denomination']

logger: logging.Logger = logging.getLogger('main_logger')


@dataclass
class ContentElement:

    Denomination: str = ''
    Producer: str = ''
    Wine: str = ''

    def __str__(self):
        return f'Wine: {self.Wine}, Producer: {self.Producer}, Denomination: {self.Denomination}'


class ESOperations:
    try:
        es: Elasticsearch = Elasticsearch(hosts='localhost:9200', timeout=300)
        logger.info('Elastic search connected')
    except Exception as ex:
        es = None
        logger.critical('Elastic search not connected')
        logger.critical(f'{ex}')

    def __init__(self):
        self.sample_content = []
        df: DataFrame = pd.read_pickle('sample_content.pickle')
        for col in df.columns:
            self.sample_content.extend(list(df[col]))

    def make_and_fill_sample_index(self):
        # self.create_sample_index()
        for _ in range(50):
            cont: list = choices(self.sample_content, k=3)
            doc: ContentElement = ContentElement(Wine=cont[0],
                                                 Producer=cont[1],
                                                 Denomination=cont[2])
            self.save_to_elastic(doc_to_save=doc)

    @classmethod
    def create_sample_index(cls):
        try:
            logger.info('Check index existence')
            indices: list = list(cls.es.indices.get_alias().keys())
            if 'content_index' not in indices:
                cls.es.indices.create(index='content_index')
        except Exception as ex:
            logger.error('Error while initializing index')
            logger.critical(f'{ex}')

    @classmethod
    def save_to_elastic(cls, doc_to_save: ContentElement):
        try:
            doc: dict = asdict(doc_to_save)
            res: Dict[str, Any] = cls.es.index(index='content_index', document=doc)
            print(res)
        except Exception as ex:
            logger.error(f'Error while saving to Elastic. Saving {doc_to_save} failed.')
            logger.error(f'Raised error: {ex}')
            logger.info(doc_to_save)

    @classmethod
    def get_counts(cls, feature):
        s = Search(using=cls.es, index='content_index')
        a = A("terms", field=f'{feature}.keyword', size=100000)
        s.aggs.metric('wines', a)
        res = s.execute().aggregations.wines.buckets
        freqs = {item['key']: item['doc_count'] for item in res}
        return freqs


if __name__ == '__main__':
    eso = ESOperations()
    print(eso.get_counts('Wine'))