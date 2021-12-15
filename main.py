from elasticsearch import Elasticsearch
import logging
import pandas as pd
from random import choices
from dataclasses import dataclass, asdict

CONTENT_FEATURES = ['Wine', 'Producer', 'Denomination']

logger = logging.getLogger('main_logger')


@dataclass
class ContentElement:

    Denomination: str = ''
    Producer: str = ''
    Wine: str = ''

    def __str__(self):
        return f'Wine: {self.Wine}, Producer: {self.Producer}, Denomination: {self.Denomination}'


class ESOperations:
    try:
        es = Elasticsearch(hosts='localhost:9200', timeout=300)
        logger.info('Elastic search connected')
    except Exception as ex:
        es = None
        logger.critical('Elastic search not connected')
        logger.critical(f'{ex}')

    def __init__(self):
        self.sample_content = []
        df = pd.read_pickle('sample_content.pickle')
        for col in df.columns:
            self.sample_content.extend(list(df[col]))

    def make_and_fill_sample_index(self):
        self.create_sample_index()
        for _ in range(50):
            cont = choices(self.sample_content, k=3)
            doc = ContentElement(Wine=cont[0],
                                 Producer=cont[1],
                                 Denomination=cont[2])
            self.save_to_elastic(doc_to_save=doc)

    @classmethod
    def create_sample_index(cls):
        try:
            logger.info('Check index existence')
            indices = list(cls.es.indices.get_alias().keys())
            if 'content_index' not in indices:
                cls.es.indices.create(index='content_index')
        except Exception as ex:
            logger.error('Error while initializing index')
            logger.critical(f'{ex}')

    @classmethod
    def save_to_elastic(cls, doc_to_save: ContentElement):
        try:
            doc = asdict(doc_to_save)
            res = cls.es.index(index='content_index', document=doc)
            return res
        except Exception as ex:
            logger.error(f'Error while saving to Elastic. Saving {doc_to_save} failed.')
            logger.error(f'Raised error: {ex}')
            logger.info(doc_to_save)


if __name__ == '__main__':
    eso = ESOperations()
    eso.make_and_fill_sample_index()