import pandas as pd
TEXT_COLUMN = "discharge_note"

class TextPreprocessor:
    def __init__(
        self,
        lower: bool = True,
        remove_special_characters_mullenbach: bool = True,
        remove_special_characters: bool = False,
        remove_digits: bool = True,
        remove_accents: bool = False,
        remove_brackets: bool = False,
        convert_danish_characters: bool = False,
    ) -> None:
        self.lower = lower
        self.remove_special_characters_mullenbach = remove_special_characters_mullenbach
        self.remove_digits = remove_digits
        self.remove_accents = remove_accents
        self.remove_special_characters = remove_special_characters
        self.remove_brackets = remove_brackets
        self.convert_danish_characters = convert_danish_characters

    def __call__(self, df):
        if self.lower:
            df[TEXT_COLUMN] = df[TEXT_COLUMN].str.lower()
        if self.convert_danish_characters:
            df[TEXT_COLUMN] = df[TEXT_COLUMN].str.replace("å", "aa", regex=True)
            df[TEXT_COLUMN] = df[TEXT_COLUMN].str.replace("æ", "ae", regex=True)
            df[TEXT_COLUMN] = df[TEXT_COLUMN].str.replace("ø", "oe", regex=True)
        if self.remove_accents:
            df[TEXT_COLUMN] = df[TEXT_COLUMN].str.replace("é|è|ê", "e", regex=True)
            df[TEXT_COLUMN] = df[TEXT_COLUMN].str.replace("á|à|â", "a", regex=True)
            df[TEXT_COLUMN] = df[TEXT_COLUMN].str.replace("ô|ó|ò", "o", regex=True)
        if self.remove_brackets:
            df[TEXT_COLUMN] = df[TEXT_COLUMN].str.replace("\[[^]]*\]", "", regex=True)
        if self.remove_special_characters:
            df[TEXT_COLUMN] = df[TEXT_COLUMN].str.replace("\n|/|-", " ", regex=True)
            df[TEXT_COLUMN] = df[TEXT_COLUMN].str.replace(
                "[^a-zA-Z0-9 ]", "", regex=True
            )
        if self.remove_special_characters_mullenbach:
            df[TEXT_COLUMN] = df[TEXT_COLUMN].str.replace(
                "[^A-Za-z0-9]+", " ", regex=True
            )
        if self.remove_digits:
            df[TEXT_COLUMN] = df[TEXT_COLUMN].str.replace("(\s\d+)+\s", " ", regex=True)

        df[TEXT_COLUMN] = df[TEXT_COLUMN].str.replace("\s+", " ", regex=True)
        df[TEXT_COLUMN] = df[TEXT_COLUMN].str.strip()
        return df

preprocessor = TextPreprocessor()

df = pd.read_parquet('~/data/physionet.org/processed/mimiciv/patient_records_with_notes_icd10_complete_coded_combined_filtered.parquet')

df = preprocessor(df)

df.to_parquet('~/data/physionet.org/processed/mimiciv/patient_records_with_notes_icd10_complete_coded_combined_filtered_preprocessed.parquet')