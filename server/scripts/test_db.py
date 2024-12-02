import json
import snowflake.connector

def get_snowflake_sql_result(sql_query):
    """
    is_save = True, output a 'result.csv'
    if_save = False, output a string
    """
    snowflake_credential = json.load(open('./data/spider2/spider2-snow/snowflake_credential.json'))
    conn = snowflake.connector.connect(
        **snowflake_credential
    )
    cursor = conn.cursor()
    try:
        cursor.execute(sql_query)
        results = cursor.fetchall()
        print(results)
    except Exception as e:
        print("Error occurred while fetching data: ", e)  
        return False, str(e)

    

with open('./data/spider2/spider2-snow/test_data/spider2-snow.jsonl', 'r') as file:
    test_questions=[json.loads(line) for line in file]
    file.close()

snowflake_credential = json.load(open('./data/spider2/spider2-snow/snowflake_credential.json'))
conn = snowflake.connector.connect(
    **snowflake_credential
)
cursor = conn.cursor()
prev_db=[]
for num,i in enumerate(test_questions[247:]):
    db=i['db_id']
    if db in prev_db:
        continue
    prev_db.append(db)
    try:
        cursor.execute(f'SHOW SCHEMAS IN DATABASE {db}')
        first_schema_result = cursor.fetchall()
        first_schema="INFORMATION_SCHEMA"
        idx=0
        first_schema=first_schema_result[0][1]
        table_result=[]
        while len(table_result)==0:
            try:
                first_schema=first_schema_result[idx][1]
            except:
                raise Exception("No Tables found")
            cursor.execute(f"SHOW TABLES IN SCHEMA {db}.{first_schema}")
            table_result=cursor.fetchall()
            idx+=1
        
            
        table=table_result[0][1]

        cursor.execute(f"SELECT * FROM {db}.{first_schema}.{table} LIMIT 1")
        cursor.fetchall()
        print(f"Success on {num}: {db}")
    except Exception as e:
        print("Error occurred while fetching data: ", e) 
        print(f"Error on {db}")
        
# Error occurred while fetching data:  No Tables found
# Error on GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI
# Error occurred while fetching data:  No Tables found
# Error on FINANCE__ECONOMICS
# Error occurred while fetching data:  No Tables found
# Error on WEATHER__ENVIRONMENT
# Error occurred while fetching data:  No Tables found
# Error on BRAZE_USER_EVENT_DEMO_DATASET
# Error occurred while fetching data:  No Tables found
# Error on US_ADDRESSES__POI
# Error occurred while fetching data:  002003 (42S02): SQL compilation error:
# Object 'CENSUS_GALAXY__ZIP_CODE_TO_BLOCK_GROUP_SAMPLE.PUBLIC.DIM_CENSUSGEOGRAPHY' does not exist or not authorized.
# Error on CENSUS_GALAXY__ZIP_CODE_TO_BLOCK_GROUP_SAMPLE
# Error occurred while fetching data:  002003 (42S02): SQL compilation error:
# Object 'CENSUS_GALAXY__AIML_MODEL_DATA_ENRICHMENT_SAMPLE.PUBLIC.DIM_CENSUSGEOGRAPHY' does not exist or not authorized.
# Error on CENSUS_GALAXY__AIML_MODEL_DATA_ENRICHMENT_SAMPLE