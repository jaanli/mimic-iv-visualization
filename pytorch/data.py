import json
import pandas as pd
import torch
from torch.utils.data import Dataset

class MIMICDataset(Dataset):
    def __init__(self, records_path, code_map_path, split='train'):
        # Load code mapping
        with open(code_map_path) as f:
            self.code_to_idx = json.load(f)
        
        # Load split-specific records
        self.records = pd.read_parquet(
            records_path,
            columns=['hadm_id', 'discharge_note', 'codes', 'split']
        )
        self.records = self.records[self.records.split == split]
    
    def __len__(self):
        return len(self.records)
    
    def __getitem__(self, idx):
        row = self.records.iloc[idx]
        codes = torch.tensor([self.code_to_idx[c] for c in row.codes if c in self.code_to_idx])
        
        labels = torch.zeros(len(self.code_to_idx))
        labels[codes] = 1.0
        
        return {
            'text': row.discharge_note,
            'codes': codes,
            'labels': labels,
            'hadm_id': row.hadm_id
        }

if __name__ == "__main__":
    dataset = MIMICDataset(
        records_path="~/data/physionet.org/processed/mimiciv/patient_records_with_splits.parquet",
        code_map_path="/Users/me/projects/mimic-iv-visualization/data/code_to_index.json",
        split='train'
    )
    print(next(iter(dataset)))