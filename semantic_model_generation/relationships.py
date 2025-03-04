import sqlite3
import semantic_model_pb2

def get_sqlite_tables(db_path, conn=None):
    if conn is None:
        conn = sqlite3.connect(db_path)
    cursor=conn.cursor()
    cursor.execute("SELECT * FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    tables=[table[1] for table in tables]
    return tables


def get_relationships(db_path, conn=None):
    if conn is None:
        conn = sqlite3.connect(db_path)
    tables=get_sqlite_tables(db_path, conn)
    cursor=conn.cursor()
    rel_list=[]
    for i in tables:
        cursor.execute(f"PRAGMA foreign_key_list({i});")

        relationships=cursor.fetchall()

        rel_dict={}
        for rel in relationships:
            if f"{i.upper()}_to_{rel[2].upper()}" in rel_dict:
                rel_dict[f"{i.upper()}_to_{rel[2].upper()}"].append([rel[3].upper(),rel[4].upper()])
            else:
                rel_dict[f"{i.upper()}_to_{rel[2].upper()}"]=[[rel[3].upper(),rel[4].upper()]]
        
        for rel in rel_dict:
            split=rel.split('_to_')
            left_table=split[0]
            right_table=split[1]
            rel_list.append(
            semantic_model_pb2.Relationship(
            name=rel,
            left_table=left_table,
            right_table=right_table,
            join_type=semantic_model_pb2.JoinType.inner,
            relationship_columns=[
                semantic_model_pb2.RelationKey(
                    left_column=col[0],
                    right_column=col[1],
                )
                for col in rel_dict[rel]
            ],
            relationship_type=semantic_model_pb2.RelationshipType.many_to_one,
        )
            )

    return rel_list

def get_primary_keys(table ,db_path, conn=None):
    if conn is None:
        conn = sqlite3.connect(db_path)
    cursor=conn.cursor()
    cursor.execute(f"PRAGMA table_info({table});")
    res=cursor.fetchall()
    primary_keys = [row[1].upper() for row in res if row[5] != 0]
    return primary_keys