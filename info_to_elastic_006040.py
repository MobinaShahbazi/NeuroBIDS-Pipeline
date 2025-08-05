import os
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

beh_dir = r"E:\term8\5. Bachelor Project\search\BIDS\datasets\ds006040-test"

index_name = "info_6040"

actions = []

for root, dirs, files in os.walk(beh_dir):
    for file in files:
        if file.endswith("_beh.tsv"):

            # extract info from file name
            parts = file.split('_')
            subject = parts[0].replace("sub-", "")
            task = parts[1].replace("task-", "")
            run = parts[2].replace("run-", "")
            
            file_path = os.path.join(root, file)

            # read TSV
            df = pd.read_csv(file_path, sep="\t")

            for _, row in df.iterrows():
                doc = row.to_dict()
                doc["subject"] = subject
                doc["task"] = task
                doc["run"] = run
                
                print(doc['PressedC'])

                actions.append({
                    "_index": index_name,
                    "_source": doc
                })


# //////////////////////////////////////////////////////////////////////

# # Elasticsearch connection with authentication
# es = Elasticsearch(
#     hosts=[{
#         'host': 'localhost',
#         'port': 9200,
#         'scheme': 'http'
#     }],
#     basic_auth=('elastic', 'changeme')  # user: elastic, pass: changeme
# )

# # Check connection
# if not es.ping():
#     print("Elasticsearch connection failed.")
#     exit()
# else:
#     print("Elasticsearch connected successfully.")

# # Optional: Delete index if exists (for clean run)
# if es.indices.exists(index=index_name):
#     es.indices.delete(index=index_name)
#     print(f"index {index_name} removed.")

# # Create index (optional settings/mappings can be added)
# es.indices.create(index=index_name)
# print(f"index {index_name} created.")

# # Bulk insert into Elasticsearch
# bulk(es, actions)
# print(f"all ({len(actions)}) data added to index {index_name}.")