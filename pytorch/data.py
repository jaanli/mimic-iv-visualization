import pandas as pd
from torch.utils.data import Dataset

class MIMICDataset(Dataset):
    def __init__(self, records_path, split_path, split='train'):
        # Load split info
        splits = pd.read_feather(split_path)
        train_ids = splits[splits.split == split].hadm_id.values
        
        # Load only training records
        self.records = pd.read_parquet(
            records_path,
            columns=['hadm_id', 'discharge_note', 'codes']  # Add/modify columns as needed
        )
        self.records = self.records[self.records.hadm_id.isin(train_ids)]
    
    def __len__(self):
        return len(self.records)
    
    def __getitem__(self, idx):
        row = self.records.iloc[idx]
        return {
            'text': row.text,
            'codes': row.codes,
            'hadm_id': row.hadm_id
        }