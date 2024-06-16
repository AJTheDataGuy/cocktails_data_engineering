from sqlalchemy import create_engine
from dataclasses import dataclass

@dataclass
class SpiderIndexSQLSetup:
    def __init__(self,**kwargs):
        """Placeholder
        """
        property_defaults = {
            'sql_engine_str':'postgresql+psycopg2',
            'host':'localhost',
            'database':'web_spider_data',
            'user':'postgres',
            'password':'postgres',
            'port':'5432',
            'db_index_schema':'raw',
            'db_index_table_name':'indexing',
            'unique_links_schema':'raw',
            'unique_links_table_name':'unique_links'
        }
        for (prop, default) in property_defaults.items():
            setattr(self,prop,kwargs.get(prop,default))
        self.bind_sql_engine()
        


def bind_sql_engine(self):
    """placeholder
    """

    engine = create_engine(
        f"{self.sql_engine_str}://{self.user}:{self.password}\
            @{self.host}:{self.port}/{self.database}"
    )
    self.sql_engine = engine