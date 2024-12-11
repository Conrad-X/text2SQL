import re
import string
import collections
import json
import attr
import networkx as nx
import time

import nltk.corpus

STOPWORDS = set(nltk.corpus.stopwords.words('english'))
PUNKS = set(a for a in string.punctuation)

CELL_EXACT_MATCH_FLAG = "EXACTMATCH"
CELL_PARTIAL_MATCH_FLAG = "PARTIALMATCH"
COL_PARTIAL_MATCH_FLAG = "CPM"
COL_EXACT_MATCH_FLAG = "CEM"
TAB_PARTIAL_MATCH_FLAG = "TPM"
TAB_EXACT_MATCH_FLAG = "TEM"

@attr.s
class SpiderItem:
    text = attr.ib()
    code = attr.ib()
    schema = attr.ib()
    orig = attr.ib()
    orig_schema = attr.ib()


@attr.s
class Column:
    id = attr.ib()
    table = attr.ib()
    name = attr.ib()
    unsplit_name = attr.ib()
    orig_name = attr.ib()
    type = attr.ib()
    foreign_key_for = attr.ib(default=None)


@attr.s
class Table:
    id = attr.ib()
    name = attr.ib()
    unsplit_name = attr.ib()
    orig_name = attr.ib()
    columns = attr.ib(factory=list)
    primary_keys = attr.ib(factory=list)


@attr.s
class Schema:
    db_id = attr.ib()
    tables = attr.ib()
    columns = attr.ib()
    foreign_key_graph = attr.ib()
    orig = attr.ib()
    connection = attr.ib(default=None)

# schema linking, similar to IRNet
def compute_schema_linking(question, column, table):

    # if y_list appears as a substring in x_list return True
    def partial_match(x_list, y_list):
        x_str = " ".join(x_list)
        y_str = " ".join(y_list)
        if x_str in STOPWORDS or x_str in PUNKS:
            return False
        if re.match(rf"\b{re.escape(x_str)}\b", y_str):
            assert x_str in y_str
            return True
        else:
            return False

    # return true if exact match
    def exact_match(x_list, y_list):
        x_str = " ".join(x_list)
        y_str = " ".join(y_list)
        if x_str == y_str:
            return True
        else:
            return False

    q_col_match = dict()
    q_tab_match = dict()

    col_id2list = dict()
    for col_id, col_item in enumerate(column):
        if col_id == 0:
            continue
        col_id2list[col_id] = col_item      # column index: column tokens with each word seperate

    tab_id2list = dict()
    for tab_id, tab_item in enumerate(table):
        tab_id2list[tab_id] = tab_item  # similar to column where key table index and value table tokens seperated.



    # 5-gram
    n = 5
    while n > 0:
        for i in range(len(question) - n + 1):
            
            n_gram_list = question[i:i + n]
            n_gram = " ".join(n_gram_list)      # take i to n tokens of the question
            if len(n_gram.strip()) == 0:        # checks if n_gram empty or consists for whitespace
                continue
            # exact match case
            for col_id in col_id2list:
                if exact_match(n_gram_list, col_id2list[col_id]):
                    for q_id in range(i, i + n):
                        q_col_match[f"{q_id},{col_id}"] = COL_EXACT_MATCH_FLAG
                    
            for tab_id in tab_id2list:
                if exact_match(n_gram_list, tab_id2list[tab_id]):
                    for q_id in range(i, i + n):
                        q_tab_match[f"{q_id},{tab_id}"] = TAB_EXACT_MATCH_FLAG

            # partial match case
            for col_id in col_id2list:
                if partial_match(n_gram_list, col_id2list[col_id]):
                    for q_id in range(i, i + n):
                        if f"{q_id},{col_id}" not in q_col_match:
                            q_col_match[f"{q_id},{col_id}"] = COL_PARTIAL_MATCH_FLAG
                    
            for tab_id in tab_id2list:
                if partial_match(n_gram_list, tab_id2list[tab_id]):
                    for q_id in range(i, i + n):
                        if f"{q_id},{tab_id}" not in q_tab_match:
                            q_tab_match[f"{q_id},{tab_id}"] = TAB_PARTIAL_MATCH_FLAG
        n -= 1

   
    # key is {question index, col/table index} value is FLAG for table/column partial/exact match
    return {"q_col_match": q_col_match, "q_tab_match": q_tab_match}

# tokens is the question
def compute_cell_value_linking(tokens, schema, connection, cv_partial_cache={}, cv_exact_cache={}):
    print("tokens",tokens)
    def isnumber(word):
        try:
            float(word)
            return True
        except:
            return False

    def db_word_partial_match(word, column, table, db_conn):    # sees if a column value is like the word partially

        try:
            ret=cv_partial_cache[f"{word}{column}{table}{schema.db_id}"]
            print("partial cache hit")
            return ret 
        except KeyError as e:
            cursor = db_conn.cursor()

            p_str = f"select {column} from {table} where {column} like '{word} %' or {column} like '% {word}' or " \
                    f"{column} like '% {word} %' or {column} like '{word}'" 
            try:
                cursor.execute(p_str)
                p_res = cursor.fetchall()
                if len(p_res) == 0:                 
                    cv_partial_cache[f"{word}{column}{table}{schema.db_id}"]=False
                    return False
                else:              
                    cv_partial_cache[f"{word}{column}{table}{schema.db_id}"]=True
                    return p_res
            except Exception as e:
                cv_partial_cache[f"{word}{column}{table}{schema.db_id}"]=False
                return False

    def db_word_exact_match(word, column, table, db_conn): # sees if a column value is like the word exactly
        try:
            ret= cv_exact_cache[f"{word}{column}{table}{schema.db_id}"]
            print("exact cache hit")
            return ret
        except KeyError as e:
            cursor = db_conn.cursor()

            p_str = f"select {column} from {table} where {column} like '{word}' or {column} like ' {word}' or " \
                    f"{column} like '{word} ' or {column} like ' {word} '"
            try:
                cursor.execute(p_str)
                p_res = cursor.fetchall()
                if len(p_res) == 0:
                    cv_exact_cache[f"{word}{column}{table}{schema.db_id}"]=False
                    return False
                else:
                    cv_exact_cache[f"{word}{column}{table}{schema.db_id}"]=True
                    return p_res
            except Exception as e:
                cv_exact_cache[f"{word}{column}{table}{schema.db_id}"]=False
                return False

    num_date_match = {}
    cell_match = {}

    for col_id, column in enumerate(schema.columns):
        if col_id == 0:
            assert column.orig_name == "*"
            continue
        match_q_ids = []
        print(f"Column: {column.orig_name}")
        for q_id, word in enumerate(tokens):
            if len(word.strip()) == 0:
                continue
            if word in STOPWORDS or word in PUNKS:
                continue

            num_flag = isnumber(word)
            if num_flag:    # TODO refine the date and time match
                if column.type in ["number", "time"]:
                    num_date_match[f"{q_id},{col_id}"] = column.type.upper()
            else:
                start_time=time.time()
                ret = db_word_partial_match(word, column.orig_name, column.table.orig_name, connection)
                end_time=time.time()
                execution_time = end_time - start_time  # Calculate the elapsed time
                print(f"Execution Time: {execution_time:.6f} seconds for {word}")
                
                if ret:
                    # print(word, ret)
                    match_q_ids.append(q_id)
        
        print(f"match_q_ids: {match_q_ids}")
        f = 0
        while f < len(match_q_ids):
            t = f + 1
            while t < len(match_q_ids) and match_q_ids[t] == match_q_ids[t - 1] + 1:
                t += 1
            q_f, q_t = match_q_ids[f], match_q_ids[t - 1] + 1 # groups consecutive matches together
            words = [token for token in tokens[q_f: q_t]]
            print(f"testing exact match for: {' '.join(words)}")
            ret = db_word_exact_match(' '.join(words), column.orig_name, column.table.orig_name, connection)
            if ret:
                for q_id in range(q_f, q_t):
                    cell_match[f"{q_id},{col_id}"] = CELL_EXACT_MATCH_FLAG
            else:
                for q_id in range(q_f, q_t):
                    cell_match[f"{q_id},{col_id}"] = CELL_PARTIAL_MATCH_FLAG
            f = t

    cv_link = {"num_date_match": num_date_match, "cell_match": cell_match}
    return cv_link


def match_shift(q_col_match, q_tab_match, cell_match):

    q_id_to_match = collections.defaultdict(list)
    for match_key in q_col_match.keys():
        q_id = int(match_key.split(',')[0])
        c_id = int(match_key.split(',')[1])
        type = q_col_match[match_key]
        q_id_to_match[q_id].append((type, c_id))
    for match_key in q_tab_match.keys():
        q_id = int(match_key.split(',')[0])
        t_id = int(match_key.split(',')[1])
        type = q_tab_match[match_key]
        q_id_to_match[q_id].append((type, t_id))
    relevant_q_ids = list(q_id_to_match.keys())

    priority = []
    for q_id in q_id_to_match.keys():
        q_id_to_match[q_id] = list(set(q_id_to_match[q_id]))
        priority.append((len(q_id_to_match[q_id]), q_id))
    priority.sort()
    matches = []
    new_q_col_match, new_q_tab_match = dict(), dict()
    for _, q_id in priority:
        if not list(set(matches) & set(q_id_to_match[q_id])):
            exact_matches = []
            for match in q_id_to_match[q_id]:
                if match[0] in [COL_EXACT_MATCH_FLAG, TAB_EXACT_MATCH_FLAG]:
                    exact_matches.append(match)
            if exact_matches:
                res = exact_matches
            else:
                res = q_id_to_match[q_id]
            matches.extend(res)
        else:
            res = list(set(matches) & set(q_id_to_match[q_id]))
        for match in res:
            type, c_t_id = match
            if type in [COL_PARTIAL_MATCH_FLAG, COL_EXACT_MATCH_FLAG]:
                new_q_col_match[f'{q_id},{c_t_id}'] = type
            if type in [TAB_PARTIAL_MATCH_FLAG, TAB_EXACT_MATCH_FLAG]:
                new_q_tab_match[f'{q_id},{c_t_id}'] = type

    new_cell_match = dict()
    for match_key in cell_match.keys():
        q_id = int(match_key.split(',')[0])
        if q_id in relevant_q_ids:
            continue
        # if cell_match[match_key] == CELL_EXACT_MATCH_FLAG:
        new_cell_match[match_key] = cell_match[match_key]

    return new_q_col_match, new_q_tab_match, new_cell_match

def load_tables(paths):
    schemas = {}
    eval_foreign_key_maps = {}

    for path in paths:
        schema_dicts = json.load(open(path))
        for schema_dict in schema_dicts:
            tables = tuple(
                Table(
                    id=i,
                    name=name.split(),
                    unsplit_name=name,
                    orig_name=orig_name,
                )
                for i, (name, orig_name) in enumerate(zip(
                    schema_dict['table_names'], schema_dict['table_names_original']))
            )
            columns = tuple(
                Column(
                    id=i,
                    table=tables[table_id] if table_id >= 0 else None,
                    name=col_name.split(),
                    unsplit_name=col_name,
                    orig_name=orig_col_name,
                    type=col_type,
                )
                for i, ((table_id, col_name), (_, orig_col_name), col_type) in enumerate(zip(
                    schema_dict['column_names'],
                    schema_dict['column_names_original'],
                    schema_dict['column_types']))
            )

            # Link columns to tables
            for column in columns:
                if column.table:
                    column.table.columns.append(column)

            for column_id in schema_dict['primary_keys']:
                # Register primary keys
                if isinstance(column_id, list):
                    for each_id in column_id:
                        column = columns[each_id]
                        column.table.primary_keys.append(column)
                else:
                    column = columns[column_id]
                    column.table.primary_keys.append(column)

            foreign_key_graph = nx.DiGraph()
            for source_column_id, dest_column_id in schema_dict['foreign_keys']:
                # Register foreign keys
                source_column = columns[source_column_id]
                dest_column = columns[dest_column_id]
                source_column.foreign_key_for = dest_column
                foreign_key_graph.add_edge(
                    source_column.table.id,
                    dest_column.table.id,
                    columns=(source_column_id, dest_column_id))
                foreign_key_graph.add_edge(
                    dest_column.table.id,
                    source_column.table.id,
                    columns=(dest_column_id, source_column_id))

            db_id = schema_dict['db_id']
            assert db_id not in schemas
            schemas[db_id] = Schema(db_id, tables, columns, foreign_key_graph, schema_dict)
            # eval_foreign_key_maps[db_id] = build_foreign_key_map(schema_dict)

    return schemas, eval_foreign_key_maps

