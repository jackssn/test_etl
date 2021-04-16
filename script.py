import os
import csv
import json
from sqlalchemy import create_engine


LIMIT_TO_LOAD = 1  # max rows to transfer from source_db to destination_db by one running script

TEMP_CSV_PATH = 'temp.csv'
LAST_UPDATED_AT_PATH = 'last_updated.txt'

with open("dbinfo.json", 'r') as f:
    dbinfo = json.load(f)

source_login = dbinfo['source']['login']
source_pass= dbinfo['source']['password']
source_host= dbinfo['source']['hostname']
source_port = dbinfo['source']['port']
source_name = dbinfo['source']['db_name']
if source_port: source_port = ':' + source_port

dest_login = dbinfo['destination']['login']
dest_pass= dbinfo['destination']['password']
dest_host= dbinfo['destination']['hostname']
dest_port = dbinfo['destination']['port']
dest_name = dbinfo['destination']['db_name']
if dest_port: dest_port = ':' + dest_port

if not os.path.exists(LAST_UPDATED_AT_PATH):
    # It's first run.
    # Create very old updated_at date
    with open(LAST_UPDATED_AT_PATH, 'w') as f:
        f.write("'1001-01-01'")
    # Update type of id-column in destination_db to correct insert DEFAULT value
    engine = create_engine(f"postgresql://{dest_login}:{dest_pass}@{dest_host}{dest_port}/{dest_name}")
    with engine.connect() as conn:
        query_alter = "CREATE SEQUENCE raw_order_id_seq;" \
                      "ALTER TABLE raw_order ALTER COLUMN id SET DEFAULT nextval('raw_order_id_seq');"
        conn.execute(query_alter)
        print('Type of id-column updated successfully.')

with open(LAST_UPDATED_AT_PATH, 'r') as f:
    last_updated_at = f.read()

# Load data from source_db to temp .csv
engine = create_engine(f"postgresql://{source_login}:{source_pass}@{source_host}{source_port}/{source_name}")
with engine.connect() as conn:
    cursor = conn.execute(f'SELECT * FROM "order" '
                          f'WHERE updated_at > {last_updated_at} '
                          f'ORDER BY updated_at LIMIT {LIMIT_TO_LOAD};')
    with open(TEMP_CSV_PATH, 'w') as f:
        csv = csv.writer(f)
        csv.writerows(cursor)

dest_source_mapping = {
    'order_id': 'id',
    'student_id': 'student_id',
    'teacher_id': 'teacher_id',
    'stage': 'stage',
    'status': 'status',
    'created_at': 'created_at',
    'updated_at': 'updated_at',
}

# Record data from temp .csv to destination_db
engine = create_engine(f"postgresql://{dest_login}:{dest_pass}@{dest_host}{dest_port}/{dest_name}")
with engine.connect() as conn:
    with open(TEMP_CSV_PATH, 'r') as f:
        lines = f.read().splitlines()
        line_map = {
            i: k.strip() for i, k in enumerate(
                'id,student_id,teacher_id,stage,status,created_at,updated_at'.split(',')
            )
        }
        for line in lines:
            line_dict = {
                    line_map[i]: v for i, v in enumerate(line.split(','))
                }
            new_line = "DEFAULT," \
                       f"{line_dict[dest_source_mapping['order_id']]}," \
                       f"{line_dict[dest_source_mapping['student_id']]}," \
                       f"{line_dict[dest_source_mapping['teacher_id']]}," \
                       f"'{line_dict[dest_source_mapping['stage']]}'," \
                       f"'{line_dict[dest_source_mapping['status']]}'," \
                       f"{hash(line)}," \
                       f"'{line_dict[dest_source_mapping['created_at']]}'," \
                       f"'{line_dict[dest_source_mapping['updated_at']]}'"

            query = f'INSERT INTO raw_order VALUES ({new_line});'
            print('query:', query)
            conn.execute(query)
            last_updated_at = f'\'{line_dict[dest_source_mapping["updated_at"]]}\''
        with open(LAST_UPDATED_AT_PATH, 'w') as f:
            f.write(last_updated_at)
    os.remove(TEMP_CSV_PATH)
