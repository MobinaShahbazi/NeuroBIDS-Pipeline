from bids import BIDSLayout
import os
import pandas as pd

layout = BIDSLayout(r"E:\term8\5. Bachelor Project\search\BIDS\datasets\ds006012", validate=False)

participants_df = pd.read_csv(r"E:\term8\5. Bachelor Project\search\BIDS\datasets\ds006012\participants.tsv", sep='\t')
participants_df['participant_id'] = participants_df['participant_id'].str.replace('sub-', '')

files = layout.get()

for f in files:
    entities = layout.parse_file_entities(f)
    metadata = layout.get_metadata(f)

    path_parts = f.path.split(os.sep)
    modality = None
    for part in path_parts:
        if part in ['anat', 'meg', 'func', 'dwi', 'fmap', 'eeg', 'ieg']:
            modality = part
            break

    # load data from participants.tsv
    participant_info = participants_df[participants_df['participant_id'] == entities.get("subject")]
    age = participant_info.iloc[0].get('age') if not participant_info.empty else None
    handedness = participant_info.iloc[0].get('hand') if not participant_info.empty else None

    doc = {
        "subject": entities.get("subject"),
        "session": entities.get("session"),
        "run": entities.get("run"),
        "task": entities.get("task"),
        "modality": modality,
        "suffix": entities.get("suffix"),
        "file_path": f.path,
        "file_size": os.path.getsize(f.path),
        "age": age,
        "handedness": handedness,
        
        # check later
        "TR": metadata.get("RepetitionTime"),
        "TE": metadata.get("EchoTime"),
        "flip_angle": metadata.get("FlipAngle"),
        "acquisition_date": metadata.get("AcquisitionDate")
    }

    print(doc)
    # es.index(index="bids-subjects", document=doc) 
