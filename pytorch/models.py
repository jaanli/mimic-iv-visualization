from transformers import AutoTokenizer, AutoModel


class MIMICPredictor(nn.Module):
    def __init__(self, num_codes):
        super().__init__()
        self.bert = AutoModel.from_pretrained("emilyalsentzer/Bio_ClinicalBERT")
        self.classifier = nn.Linear(768, num_codes)  # BERT hidden size is 768
    
    def forward(self, input_ids, attention_mask):
        outputs = self.bert(input_ids=input_ids, attention_mask=attention_mask)
        return self.classifier(outputs.last_hidden_state[:, 0, :])  # Use [CLS] token
