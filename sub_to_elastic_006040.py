from bids import BIDSLayout
import pandas as pd
import os
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Load layout
layout = BIDSLayout(r"E:\term8\5. Bachelor Project\search\BIDS\datasets\ds006040", validate=False)

# Load participants.tsv
participants_df = pd.read_csv(r"E:\term8\5. Bachelor Project\search\BIDS\datasets\ds006040\Demographic_Information.tsv", sep='\t')
participants_df['Anonymized ID'] = participants_df['Anonymized ID'].str.replace('sub-', '')

# Prepare bulk data
actions = []
index_name = "subject_v2"

subjects = sorted(layout.get_subjects() + ['003'])
print(subjects)
for subj in subjects:
    files = layout.get(subject=subj)
    modalities = sorted({f.datatype for f in files if f.datatype})

    participant_info = participants_df[participants_df['Anonymized ID'] == subj]

    if not participant_info.empty:
        info_dict = participant_info.iloc[0].to_dict()
        info_dict['modalities'] = modalities
        info_dict['subject'] = subj 
        
        doc = info_dict
        # print(doc)

    action = {
        "_index": index_name,
        "_source": doc
    }
    actions.append(action)


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