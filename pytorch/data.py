import json
import pandas as pd
import torch
from torch.utils.data import Dataset, DataLoader

class MIMICDataset(Dataset):
    def __init__(self, records_path, split_path, code_map_path, split='train'):
        # Load split info
        splits = pd.read_feather(split_path)
        train_ids = splits[splits.split == split].hadm_id.values
        
        # Load code mapping
        with open(code_map_path) as f:
            self.code_to_idx = json.load(f)
        
        # Load only training records
        self.records = pd.read_parquet(
            records_path,
            columns=['hadm_id', 'discharge_note', 'codes']
        )
        self.records = self.records[self.records.hadm_id.isin(train_ids)]
    
    def __len__(self):
        return len(self.records)
    
    def codes_to_indices(self, codes):
        return torch.tensor([
            self.code_to_idx[code] 
            for code in codes 
            if code in self.code_to_idx
        ])
    
    def __getitem__(self, idx):
        row = self.records.iloc[idx]
        codes = self.codes_to_indices(row.codes)
        
        # Create sparse label tensor
        labels = torch.zeros(len(self.code_to_idx))
        labels[codes] = 1.0
        
        return {
            'text': row.discharge_note,
            'codes': codes,
            'labels': labels,
            'hadm_id': row.hadm_id
        }


if __name__ == "__main__":
    dataset = MIMICDataset(records_path="~/data/physionet.org/processed/mimiciv/patient_records_with_notes_icd10_complete_coded_combined_filtered_preprocessed.parquet", 
                        split_path="/Users/me/data/physionet.org/processed/mimiciv/mimiciv_icd10_split.feather", 
                        code_map_path="/Users/me/projects/mimic-iv-visualization/data/code_to_index.json",
                        split='train')

    for batch in dataset:
        print(batch)
        break
# dataloader = DataLoader(
#         dataset,
#         batch_size=batch_size,
#         shuffle=shuffle,
#         num_workers=num_workers
#     )