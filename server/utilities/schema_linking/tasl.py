import os
import json
import sqlite3
import tqdm
import csv
from utilities.utility_functions import format_sql_response
from utilities.config import PATH_CONFIG

dummy_sql_prompt = """# the key is the table, the value is a dict which key is original column name and value is the column information including full name, column description, value_description and example values.
database_schema = {database_schema}

# the key is the table, the value is the list of its counterpart primary keys
primary_keys = {primary_key_dic}

# the key is the source column, the value is the target column referenced by foreign key relationship.
foreign_keys = {foreign_key_dic}

question = "{question_prompt}"

evidence = "{evidence}"

def question_to_SQL(question):
  # DO NOT select more things other than what the question asks
  # Generate the SQL to answer the question considering database_schema, primary_keys and foreign_keys
  # Also consider the evidence when generating the SQL
  SQL = "SELECT """

class BaseModule():
    def __init__(self, base_dir, mode, client):
        self.db_root_path = base_dir
        self.mode = mode
        table_json_path = os.path.join(base_dir, f'{mode}_tables.json')
        question_path = os.path.join(base_dir, f'{mode}.json')
        self.table_json = json.load(open(table_json_path, 'r'))
        self.question_json = json.load(open(question_path, 'r'))
        self.client = client
        # self.csv_info, self.value_prompts = self._get_info_from_csv()
    
    def _get_info_from_csv(self):
        csv_info = {}
        value_prompt = {}
        for i in tqdm.tqdm(range(len(self.table_json))):
            table_info = self.table_json[i]
            db_id = table_info['db_id']
            db_path = PATH_CONFIG.sqlite_path(db_id)
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            csv_dir = os.path.join(self.db_root_path,db_id,'database_description')
            otn_list = table_info['table_names_original']
            tn_list = table_info['table_names']
            for otn,tn in zip(otn_list, tn_list):
                if os.path.exists(os.path.join(csv_dir, f"{tn}.csv")):
                    csv_path = os.path.join(os.path.join(csv_dir, f"{tn}.csv"))
                else:
                    csv_path = os.path.join(os.path.join(csv_dir, f"{otn}.csv"))
                csv_dict = csv.DictReader(open(csv_path, newline='', encoding="latin1"))
                column_info = {}
                
                for row in csv_dict:
                    headers = list(row.keys())
                    ocn_header = [h for h in headers if 'original_column_name' in h][0]  # remove BOM
                    ocn, cn = row[ocn_header].strip(), row['column_name']
                    column_description = row['column_description'].strip()
                    column_type = row['data_format'].strip()
                    column_name = cn if cn not in ['', ' '] else ocn
                    value_description = row['value_description'].strip()
                    column_info[ocn] = [column_name, column_description, column_type, value_description]

                    if column_type in ['text', 'date', 'datetime']:
                        sql = f'''SELECT DISTINCT "{ocn}" FROM `{otn}` where "{ocn}" IS NOT NULL ORDER BY RANDOM()'''
                        cursor.execute(sql)
                        values = cursor.fetchall()
                        if len(values) > 0 and len(values[0][0]) < 50:
                            if len(values) <= 10:
                                example_values = [v[0] for v in values]
                                value_prompt[f"{db_id}|{otn}|{ocn}"] = f"all possible values are {example_values}"
                                # value_prompt[f"{db_id}|{otn}|{ocn}"] = f"all possible values of the column are {', '.join(example_values)}."
                            else:
                                example_values = [v[0] for v in values[:3]]
                                value_prompt[f"{db_id}|{otn}|{ocn}"] = f"example values are {example_values}"
                                # value_prompt[f"{db_id}|{otn}|{ocn}"] = f"three example values of the column are {', '.join(example_values)}."
                        
                csv_info[f"{db_id}|{otn}"] = column_info
            # pdb.set_trace()
        return csv_info, value_prompt 
    
    def generate_pk_fk(self, question_id):
        question_info = self.question_json[question_id]
        db_id = question_info['db_id']
        table = [content for content in self.table_json if content['db_id'] == db_id][0]
        pk_dict = {}
        fk_dict = {}
        table_names_original = table['table_names_original']
        column_names_original = table['column_names_original']
        primary_keys = table['primary_keys']
        foreign_keys = table['foreign_keys']
        
        for _,pk_idx in enumerate(primary_keys):
            if type(pk_idx) == int:
                pk_dict[str(table_names_original[column_names_original[pk_idx][0]])] = [column_names_original[pk_idx][-1]]
            else:
                pk_dict[str(table_names_original[column_names_original[pk_idx[0]][0]])] = [column_names_original[idx][-1] for idx in pk_idx]
        
        for cur_fk in foreign_keys:
            src_col_idx, tgt_col_idx = cur_fk
            src_col_name = str(table_names_original[column_names_original[src_col_idx][0]]) + '.' + str(column_names_original[src_col_idx][-1])
            tgt_col_name = str(table_names_original[column_names_original[tgt_col_idx][0]]) + '.' + str(column_names_original[tgt_col_idx][-1])
            fk_dict[src_col_name] = tgt_col_name
        return pk_dict, fk_dict

class TASL(BaseModule):
    def __init__(self, db_root_path, mode, column_meaning_path, client):
        super().__init__(db_root_path, mode, client)
        self.column_meanings = json.load(open(column_meaning_path, 'r'))
        self.mode = mode
        self.schema_item_dic = self._reconstruct_schema()
        
    def _reconstruct_schema(self):
        schema_item_dic = {}
        db_id_list = [content['db_id'] for content in self.table_json]
        
        schema_item_dic = {}
        for db_id in db_id_list:
            content = [content for content in self.table_json if content['db_id'] == db_id][0]
            otn_list = content['table_names_original']
            schema_for_db = dict(zip(otn_list, [{} for _ in range(len(otn_list))]))
            schema_item_dic[db_id] = schema_for_db
        
        for key, value in self.column_meanings.items():
            db_id, otn, ocn = key.split('|')
            value = value.replace('#', '')
            value = value.replace('\n', ',  ')
            schema_item_dic[db_id][otn][ocn] = value
        return schema_item_dic
    
    def _generate_database_schema(self, schema_for_db):
        schema_prompt = '{\n '
        for table_name, cn_prompt in schema_for_db.items():
            schema_prompt += f'{table_name}:\n  ' + '{\n\t'
            for cn, prompt in cn_prompt.items():
                schema_prompt += f"{cn}: {prompt}" + '\n\t'
                schema_prompt += '\n\t'
            schema_prompt += '}\n '
        schema_prompt += '}'
        return schema_prompt
    
    def generate_dummy_sql(self, question_id):
        question = self.question_json[question_id]
        db_id = question['db_id']
        q = question['question']
        evidence = question['evidence']
        pk_dict, fk_dict = self.generate_pk_fk(question_id)
        db_prompt_dic = self._reconstruct_schema()
        db_prompt = db_prompt_dic[db_id]
        database_schema = self._generate_database_schema(db_prompt)
        prompt = dummy_sql_prompt.format(database_schema = database_schema, primary_key_dic = pk_dict, foreign_key_dic = fk_dict, question_prompt = q, evidence = evidence)
        sql = self.client.execute_prompt(prompt=prompt)
        sql = format_sql_response(sql)

        return prompt, sql
        
    def get_schema(self, question_id):
        question_info = self.question_json[question_id]
        db_id = question_info['db_id']
        _, dummy_sql = self.generate_dummy_sql(question_id)
        table_info = [content for content in self.table_json if content['db_id'] == db_id][0]
        table_names_list = table_info["table_names_original"]
        column_names_list = [[table_names_list[int(content[0])], content[1]] for content in table_info['column_names_original'][1:]]
        pure_column_name_list = [i[1] for i in column_names_list]
        filtered_tables, filtered_columns, schemas = [], [], []
        for table in table_names_list:
            if table in dummy_sql:
                filtered_tables.append(table)
        for column in pure_column_name_list:
            if column in dummy_sql:
                filtered_columns.append(column)
        filtered_tables = list(set(filtered_tables))
        filtered_columns = list(set(filtered_columns))
        schema_used = {}
        for columns in filtered_columns:
            for table_column in column_names_list:
                if table_column[1] == columns and table_column[0] in filtered_tables:
                    schemas.append(table_column)
                    if table_column[0] not in schema_used:
                        schema_used[table_column[0]] = [table_column[1]]
                    else:
                        schema_used[table_column[0]].append(table_column[1])
        schema_used = {key.lower(): [item.lower() for item in value] for key, value in schema_used.items()}
        return schema_used
    