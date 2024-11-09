import torch
from torch import nn
from torch.utils.data import DataLoader
from transformers import AutoTokenizer, AutoModel
from data import MIMICDataset
from tqdm import tqdm

class MIMICPredictor(nn.Module):
    def __init__(self, num_codes):
        super().__init__()
        self.bert = AutoModel.from_pretrained("emilyalsentzer/Bio_ClinicalBERT")
        self.classifier = nn.Linear(768, num_codes)
    
    def forward(self, input_ids, attention_mask):
        outputs = self.bert(input_ids=input_ids, attention_mask=attention_mask)
        return self.classifier(outputs.last_hidden_state[:, 0, :])

def main():
    # Init model and data
    tokenizer = AutoTokenizer.from_pretrained("emilyalsentzer/Bio_ClinicalBERT")
    dataset = MIMICDataset(
        records_path="~/data/physionet.org/processed/mimiciv/patient_records_with_splits.parquet",
        code_map_path="/Users/me/projects/mimic-iv-visualization/data/code_to_index.json",
        split='train'
    )
    
    def collate_fn(batch):
        texts = [b['text'] for b in batch]
        encodings = tokenizer(texts, truncation=True, padding=True, max_length=128, return_tensors='pt')
        return {
            'input_ids': encodings['input_ids'],
            'attention_mask': encodings['attention_mask'],
            'labels': torch.stack([b['labels'] for b in batch]),
            'hadm_id': [b['hadm_id'] for b in batch]
        }
    
    loader = DataLoader(dataset, batch_size=4, shuffle=True, collate_fn=collate_fn)
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model = MIMICPredictor(num_codes=len(dataset.code_to_idx)).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=2e-5)
    criterion = nn.BCEWithLogitsLoss()
    
    # Train
    model.train()
    for batch in tqdm(loader):
        optimizer.zero_grad()
        input_ids = batch['input_ids'].to(device)
        attention_mask = batch['attention_mask'].to(device)
        labels = batch['labels'].to(device)
        
        logits = model(input_ids, attention_mask)
        loss = criterion(logits, labels)
        loss.backward()
        optimizer.step()

        if loss.item() < 0.1:  # Optional early stopping example
            torch.save(model.state_dict(), 'model.pt')
            break

if __name__ == "__main__":
    main()