from bids import BIDSLayout
import pandas as pd
import os
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Load layout
layout = BIDSLayout(r"E:\term8\5. Bachelor Project\search\BIDS\datasets\ds006012", validate=False)

# Prepare bulk data
actions = []
index_name = "file_v0"

files = layout.get()
df = layout.to_df()
df = df[~df['path'].str.contains(r'\.git', case=False, regex=True)]

for _, row in df.iterrows():
    doc = row.dropna().to_dict()
    
    file_path = row['path']
    try:
        file_size = os.path.getsize(file_path)  # byte
        doc['filesize'] = file_size
    except OSError:
        doc['filesize'] = None 
        
    session_raw = doc.get('session')
    if session_raw and len(str(session_raw)) == 8:
        try:
            session_str = str(session_raw)
            session_date = f"{session_str[:4]}-{session_str[4:6]}-{session_str[6:]}"
            doc['session_date'] = session_date
            print(session_date)
        except Exception:
            doc['session_date'] = None

    action = {
        "_index": index_name,
        "_source": doc
    }
    actions.append(action)

# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

# Elasticsearch connection with authentication
es = Elasticsearch(
    hosts=[{
        'host': 'localhost',
        'port': 9200,
        'scheme': 'http'
    }],
    basic_auth=('elastic', 'changeme')  # user: elastic, pass: changeme
)

# Check connection
if not es.ping():
    print("Elasticsearch connection failed.")
    exit()
else:
    print("Elasticsearch connected successfully.")

# Optional: Delete index if exists (for clean run)
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)
    print(f"index {index_name} removed.")

# Create index (optional settings/mappings can be added)
es.indices.create(index=index_name)
print(f"index {index_name} created.")

# Bulk insert into Elasticsearch
bulk(es, actions)
print(f"all ({len(actions)}) data added to index {index_name}.")