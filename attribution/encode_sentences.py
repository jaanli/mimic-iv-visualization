from pathlib import Path
from typing import List
import pandas as pd
import torch
from beartype import beartype
from jaxtyping import Float, jaxtyped
from transformers import AutoModel, AutoTokenizer
import nltk
from tqdm.auto import tqdm

class NoteEncoder:
    @beartype
    def __init__(
        self,
        model_name: str = "UFNLP/gatortron-base",
        batch_size: int = 32,
        device: str = "cuda" if torch.cuda.is_available() else "cpu"
    ):
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name).to(device)
        self.batch_size = batch_size
        self.device = device
        nltk.download('punkt', quiet=True)

    @beartype
    def split_note(self, note: str) -> List[str]:
        """Split note into sentences."""
        return nltk.sent_tokenize(note) if pd.notna(note) else []

    @jaxtyped
    @beartype
    def encode_sentences(
        self,
        sentences: List[str]
    ) -> Float[torch.Tensor, "num_sentences hidden_dim"]:
        """Encode sentences into embeddings."""
        embeddings = []
        
        for i in range(0, len(sentences), self.batch_size):
            batch = sentences[i:i + self.batch_size]
            inputs = self.tokenizer(
                batch,
                padding=True,
                truncation=True,
                max_length=512,
                return_tensors="pt"
            ).to(self.device)
            
            with torch.no_grad():
                outputs = self.model(**inputs)
                batch_embeddings = outputs.last_hidden_state[:, 0, :].cpu()
                embeddings.append(batch_embeddings)

        return torch.cat(embeddings, dim=0) if embeddings else torch.zeros(0, self.model.config.hidden_size)

@beartype
def process_patient_records(
    input_path: Path,
    output_path: Path,
    encoder: NoteEncoder
) -> None:
    """Process patient records and save embeddings."""
    df = pd.read_parquet(input_path)
    all_embeddings = []
    
    for note in tqdm(df['discharge_note'], desc='Processing notes'):
        sentences = encoder.split_note(note)
        embeddings = encoder.encode_sentences(sentences)
        all_embeddings.append(embeddings.numpy().tolist())
    
    df['note_embeddings'] = all_embeddings
    df.to_parquet(output_path, index=False)

def main():
    input_path = Path('~/data/physionet.org/processed/mimiciv/patient_records_with_notes.parquet')
    output_path = input_path.parent / 'patient_records_with_embeddings.parquet'
    
    encoder = NoteEncoder()
    process_patient_records(input_path, output_path, encoder)

if __name__ == "__main__":
    main()