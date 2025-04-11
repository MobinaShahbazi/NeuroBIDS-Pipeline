#%% extract entities
from bids import BIDSLayout

layout = BIDSLayout(r"E:\term8\5. Bachelor Project\search\BIDS\datasets\ds006012", validate=False)
print(" Entityهای معتبر و مقادیرشون:\n")

for entity_name in layout.entities:
    try:
        values = layout.get_values(target=entity_name)  # 
        print(f"{entity_name}: {values}")
    except Exception as e:
        print(f"{entity_name}:  خطا: {e}")



#%% extract dada to index files
from bids import BIDSLayout
import os
layout = BIDSLayout(r"E:\term8\5. Bachelor Project\search\BIDS\datasets\ds006012", validate=False)

files = layout.get()

for f in files:
    entities = layout.parse_file_entities(f)
    metadata = layout.get_metadata(f)

    doc = {
        "subject": entities.get("subject"),
        "session": entities.get("session"),
        "run": entities.get("run"),
        "task": entities.get("task"),
        "modality": "fMRI",
        "file_type": "nii.gz",
        "file_path": f,
        "file_size": os.path.getsize(f),
        "TR": metadata.get("RepetitionTime"),
        "TE": metadata.get("EchoTime"),
        "flip_angle": metadata.get("FlipAngle"),
        "age": metadata.get("Age"),
        "sex": metadata.get("Sex"),
        "acquisition_date": metadata.get("AcquisitionDate")
    }
    print(doc)


#%% extract dada to index sub
from bids import BIDSLayout
layout = BIDSLayout(r"E:\term8\5. Bachelor Project\search\BIDS\datasets\ds006012", validate=False)

subjects = layout.get_subjects()

for subj in subjects:
    files = layout.get(subject=subj, return_type='file')
    metadata = layout.get_metadata(files[0])  # فقط از یکی استفاده کن

    doc = {
        "subject": subj,
        "age": metadata.get("Age"),
        "sex": metadata.get("Sex"),
        "sessions": len(layout.get_sessions(subject=subj)),
        "modalities": list(set(layout.get_modalities(subject=subj)))
    }
    print(doc)
    # es.index(index="bids-subjects", document=doc)

