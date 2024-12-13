import collections
import itertools
import json
import os

import attr
import numpy as np
import torch

from scripts.masking.linking_utils import abstract_preproc, corenlp
from scripts.masking.linking_utils.utils import (
    compute_schema_linking,
    compute_cell_value_linking,
    match_shift
)

@attr.s
class PreprocessedSchema:
    column_names = attr.ib(factory=list)
    table_names = attr.ib(factory=list)
    table_bounds = attr.ib(factory=list)
    column_to_table = attr.ib(factory=dict)     # column index in column_names list : table_index in table_names list
    table_to_columns = attr.ib(factory=dict)    # table index in table_names list : column index in column_names list
    foreign_keys = attr.ib(factory=dict) #
    foreign_keys_tables = attr.ib(factory=lambda: collections.defaultdict(set))
    primary_keys = attr.ib(factory=list)

    # only for bert version
    normalized_column_names = attr.ib(factory=list)
    normalized_table_names = attr.ib(factory=list)

def to_dict_with_sorted_values(d, key=None):
    return {k: sorted(v, key=key) for k, v in d.items()}

def preprocess_schema_uncached(schema,
                               tokenize_func,
                               include_table_name_in_column,
                               fix_issue_16_primary_keys,
                               bert=False):
    """If it's bert, we also cache the normalized version of
    question/column/table for schema linking"""




    r = PreprocessedSchema()

    if bert: assert not include_table_name_in_column

    last_table_id = None
    for i, column in enumerate(schema.columns):
        col_toks = tokenize_func(
            column.name, column.unsplit_name)

        # assert column.type in ["text", "number", "time", "boolean", "others"]
        type_tok = f'<type: {column.type}>'
        if bert:
            # for bert, we take the representation of the first word
            column_name = col_toks + [type_tok]
            r.normalized_column_names.append(Bertokens(col_toks))
        else:
            column_name = [type_tok] + col_toks

        if include_table_name_in_column:
            if column.table is None:
                table_name = ['<any-table>']
            else:
                table_name = tokenize_func(
                    column.table.name, column.table.unsplit_name)
            column_name += ['<table-sep>'] + table_name
        r.column_names.append(column_name)

        table_id = None if column.table is None else column.table.id
        r.column_to_table[str(i)] = table_id
        if table_id is not None:
            columns = r.table_to_columns.setdefault(str(table_id), [])
            columns.append(i)
        if last_table_id != table_id:
            r.table_bounds.append(i)
            last_table_id = table_id

        if column.foreign_key_for is not None:
            r.foreign_keys[str(column.id)] = column.foreign_key_for.id
            r.foreign_keys_tables[str(column.table.id)].add(column.foreign_key_for.table.id)

    r.table_bounds.append(len(schema.columns))
    assert len(r.table_bounds) == len(schema.tables) + 1

    for i, table in enumerate(schema.tables):
        table_toks = tokenize_func(
            table.name, table.unsplit_name)
        r.table_names.append(table_toks)
        if bert:
            r.normalized_table_names.append(Bertokens(table_toks))
    last_table = schema.tables[-1]

    r.foreign_keys_tables = to_dict_with_sorted_values(r.foreign_keys_tables)
    r.primary_keys = [
        column.id
        for table in schema.tables
        for column in table.primary_keys
    ] if fix_issue_16_primary_keys else [
        column.id
        for column in last_table.primary_keys
        for table in schema.tables
    ]

    return r


class SpiderEncoderV2Preproc(abstract_preproc.AbstractPreproc):

    def __init__(
            self,
            dataset_dir,
            min_freq=3,
            max_count=5000,
            include_table_name_in_column=True,
            word_emb=None,
            fix_issue_16_primary_keys=False,
            compute_sc_link=False,
            compute_cv_link=False):
        if word_emb is None:
            self.word_emb = None
        else:
            self.word_emb = word_emb

        self.include_table_name_in_column = include_table_name_in_column
        self.fix_issue_16_primary_keys = fix_issue_16_primary_keys
        self.compute_sc_link = compute_sc_link
        self.compute_cv_link = compute_cv_link
        self.texts = []
        self.preprocessed_schemas = {}
        self.cv_partial_cache={}
        self.cv_exact_cache={}

    def validate_item(self, item, schema, section):
        return True, None

    def add_item(self, item, schema, connection):
        preprocessed = self.preprocess_item(item, schema, connection)
        self.texts.append(preprocessed)

    def clear_items(self):
        self.texts = collections.defaultdict(list)

    def preprocess_item(self, item, schema, connection):
        question, question_for_copying = self._tokenize_for_copying(item['question_toks'], item['question'])
        tokenize_result=self.word_emb.tokenize(item['question'])

        preproc_schema = self._preprocess_schema(schema)

        if self.compute_sc_link:
            assert preproc_schema.column_names[0][0].startswith("<type:")
            column_names_without_types = [col[1:] for col in preproc_schema.column_names]
            # key is {question index, col/table index} value is FLAG for table/column partial/exact match
            sc_link = compute_schema_linking(question, column_names_without_types, preproc_schema.table_names)
        else:
            sc_link = {"q_col_match": {}, "q_tab_match": {}}

        if self.compute_cv_link:
            # returns {"num_date_match": num_date_match, "cell_match": cell_match} 
            # num_date_match: dict([question_index, col_index]: column_type)
            # cell_match: dict ([question_index, col_index]: cell exact/partial match)
            cv_link = compute_cell_value_linking(question, schema, connection, self.cv_partial_cache, self.cv_exact_cache)
        else:
            cv_link = {"num_date_match": {}, "cell_match": {}}
        return {
            'id': item['id'],
            'raw_question': item['question'],
            'db_id': schema.db_id,
            'question': question,
            'question_for_copying': question_for_copying,
            'sc_link': sc_link,
            'cv_link': cv_link,
            'columns': preproc_schema.column_names,
            'tables': preproc_schema.table_names,
            'table_bounds': preproc_schema.table_bounds,
            'column_to_table': preproc_schema.column_to_table,
            'table_to_columns': preproc_schema.table_to_columns,
            'foreign_keys': preproc_schema.foreign_keys,
            'foreign_keys_tables': preproc_schema.foreign_keys_tables,
            'primary_keys': preproc_schema.primary_keys,
        }

    def _preprocess_schema(self, schema):
        if schema.db_id in self.preprocessed_schemas:
            return self.preprocessed_schemas[schema.db_id]
        result = preprocess_schema_uncached(schema, self._tokenize,
                                            self.include_table_name_in_column, self.fix_issue_16_primary_keys)
        self.preprocessed_schemas[schema.db_id] = result
        return result

    def _tokenize(self, presplit, unsplit):
        if self.word_emb:
            return self.word_emb.tokenize(unsplit)
        return presplit

    def _tokenize_for_copying(self, presplit, unsplit):
        if self.word_emb:
            return self.word_emb.tokenize_for_copying(unsplit)
        return presplit, presplit

    def save(self):
        os.makedirs(self.data_dir, exist_ok=True)
        with open(os.path.join(self.data_dir,'schema-linking.jsonl'), 'w') as f:
            for text in self.texts:
                f.write(json.dumps(text) + '\n')

    def load(self, sections):
        for section in sections:
            self.texts[section] = []
            with open(os.path.join(self.data_dir, section + '_schema-linking.jsonl'), 'r') as f:
                for line in f.readlines():
                    if line.strip():
                        self.texts[section].append(json.loads(line))

    def dataset(self, section):
        return [
            json.loads(line)
            for line in open(os.path.join(self.data_dir, section + '.jsonl'))]

    def get_linked_schema(self):
        return self.texts
