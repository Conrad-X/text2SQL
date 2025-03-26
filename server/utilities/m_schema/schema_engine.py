import json, os
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, select, text
import pandas as pd
from sqlalchemy.engine import Engine
from llama_index.core import SQLDatabase
from utilities.constants.database_enums import DatasetType
from utilities.config import PATH_CONFIG
from utilities.m_schema.utils import read_json, write_json, save_raw_text, examples_to_str
from utilities.m_schema.m_schema import MSchema
from utilities.logging_utils import setup_logger

logger = setup_logger(__name__)


class SchemaEngine(SQLDatabase):
    def __init__(self, engine: Engine, schema: Optional[str] = None, metadata: Optional[MetaData] = None,
                 ignore_tables: Optional[List[str]] = None, include_tables: Optional[List[str]] = None,
                 sample_rows_in_table_info: int = 3, indexes_in_table_info: bool = False,
                 custom_table_info: Optional[dict] = None, view_support: bool = False, max_string_length: int = 300,
                 mschema: Optional[MSchema] = None, db_name: Optional[str] = '', dataset_type: Optional[DatasetType] = None, matches=None, db_path = None):
        super().__init__(engine, schema, metadata, ignore_tables, include_tables, sample_rows_in_table_info,
                         indexes_in_table_info, custom_table_info, view_support, max_string_length)

        self._db_name = db_name
        self._dataset_type = dataset_type
        self._usable_tables = [table_name for table_name in self._usable_tables if self._inspector.has_table(table_name, schema)]
        self._dialect = engine.dialect.name
        self.table_descriptions = None
        self.column_descriptions = None
        if mschema is not None:
            self._mschema = mschema
        else:
            self._mschema = MSchema(db_id=db_name, schema=schema)
            if db_path:
                self.load_descriptions(db_path)
            self.init_mschema(matches)

    @property
    def mschema(self) -> MSchema:
        """Return M-Schema"""
        return self._mschema

    def get_pk_constraint(self, table_name: str) -> Dict:
        return self._inspector.get_pk_constraint(table_name, self._schema)['constrained_columns']

    def get_table_comment(self, table_name: str):
        try:
            return self._inspector.get_table_comment(table_name, self._schema)['text']
        except: # sqlite does not support adding comments
            return ''

    def default_schema_name(self) -> Optional[str]:
        return self._inspector.default_schema_name

    def get_schema_names(self) -> List[str]:
        return self._inspector.get_schema_names()

    def get_foreign_keys(self, table_name: str):
        return self._inspector.get_foreign_keys(table_name, self._schema)

    def get_unique_constraints(self, table_name: str):
        return self._inspector.get_unique_constraints(table_name, self._schema)

    def fetch_distinct_values(self, table_name: str, column_name: str, max_num: int = 5):
        table = Table(table_name, self.metadata_obj, autoload_with=self._engine)
        # Build SELECT DISTINCT query
        query = select(table.c[column_name]).distinct().limit(max_num)
        values = []
        with self._engine.connect() as connection:
            result = connection.execute(query)
            distinct_values = result.fetchall()
            for value in distinct_values:
                if value[0] is not None and value[0] != '':
                    values.append(value[0])
        return values

    def load_descriptions(self, db_path):
        description_dir = PATH_CONFIG.description_dir(database_name=self._db_name, dataset_type=self._dataset_type)
        df = pd.read_csv(f"{description_dir}/{self._db_name}_tables.csv")
        self.table_descriptions = dict(zip(df["table_name"].str.lower().str.strip(), df["table_description"]))
        self.column_descriptions = {}
        for table in self.table_descriptions.keys():
            try:
                df = pd.read_csv(f"{description_dir}/{table}.csv")
                col_desc = dict(
                    zip(
                        df["original_column_name"].str.lower().str.strip(),
                        df.apply(
                            lambda row: max(
                                str(row.get('improved_column_description', '')),
                                str(row.get('column_description', '')),
                                key=len
                            ).strip("\n"),
                            axis=1
                        )
                    )
                )

                self.column_descriptions[table] = col_desc
            except FileNotFoundError:
                continue

    def init_mschema(self, matches=None):
        if matches:
            matches = {key.lower(): [item.lower() for item in value] for key, value in matches.items()}
        for table_name in self._usable_tables:
            if (matches and table_name.lower() in list(matches.keys())) or not matches:
                table_comment = self.get_table_comment(table_name)
                table_comment = '' if table_comment is None else table_comment.strip()
                if self.table_descriptions:
                    try:
                        table_comment = self.table_descriptions[table_name.lower()]
                    except KeyError:
                        logger.warning(f"Description not found for table {table_name}")
                        table_comment = ''

                self._mschema.add_table(table_name, fields={}, comment=table_comment)
                pks = self.get_pk_constraint(table_name)

                fks = self.get_foreign_keys(table_name)
                for fk in fks:
                    referred_schema = fk['referred_schema']
                    for c, r in zip(fk['constrained_columns'], fk['referred_columns']):
                        self._mschema.add_foreign_key(table_name, c, referred_schema, fk['referred_table'], r)

                fields = self._inspector.get_columns(table_name, schema=self._schema)
                for field in fields:
                    if (matches and field['name'].lower() in matches[table_name.lower()]) or not matches:
                        field_type = f"{field['type']!s}"
                        field_name = field['name']
                        if field_name in pks:
                            primary_key = True
                        else:
                            primary_key = False

                        field_comment = field.get("comment", None)
                        field_comment = "" if field_comment is None else field_comment.strip()
                        if self.column_descriptions:
                            try:
                                field_comment = self.column_descriptions[table_name.lower()][field_name.lower()]
                            except KeyError:
                                logger.warning(f"Description not found for table: {table_name}, column: {field_name}")
                                field_comment = ''

                        autoincrement = field.get('autoincrement', False)
                        default = field.get('default', None)
                        if default is not None:
                            default = f'{default}'

                        try:
                            examples = self.fetch_distinct_values(table_name, field_name, 5)
                        except:
                            examples = []
                        examples = examples_to_str(examples)

                        self._mschema.add_field(table_name, field_name, field_type=field_type, primary_key=primary_key,
                            nullable=field['nullable'], default=default, autoincrement=autoincrement,
                            comment=field_comment, examples=examples)
