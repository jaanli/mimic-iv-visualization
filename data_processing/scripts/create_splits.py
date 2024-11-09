import logging
import random
import pandas as pd
from pathlib import Path
from data_processing.scripts.stratify import (
    iterative_stratification,
)

from data_processing.scripts.helper_funcs import (
    kl_divergence,
    labels_not_in_split
)

TARGET_COLUMN = "codes"
SUBJECT_ID_COLUMN = "subject_id"
ID_COLUMN = "hadm_id"

TEST_SIZE = 0.15  # Test split ratios
VAL_SIZE = 0.1  # Val split ratio
STEP_SIZE = 0.2  # Step size for the iterative stratification

random.seed(10)

DATA_DIRECTORY_MIMICIV_ICD10 = "/Users/me/data/physionet.org/processed/mimiciv"

logging.basicConfig(level=logging.INFO)
output_dir_icd10 = Path(DATA_DIRECTORY_MIMICIV_ICD10)

mimic_icd10 = pd.read_parquet(output_dir_icd10 / "patient_records_with_notes_icd10_complete_coded_combined_filtered_preprocessed.parquet")
mimic_icd10[TARGET_COLUMN] = mimic_icd10[TARGET_COLUMN].apply(lambda x: list(x))


# Generate splits
def generate_split(dataset: pd.DataFrame, split_path: Path):
    splits = dataset[[SUBJECT_ID_COLUMN, ID_COLUMN]]
    subject_series = dataset.groupby(SUBJECT_ID_COLUMN)[TARGET_COLUMN].sum()
    subject_ids = subject_series.index.to_list()
    codes = subject_series.to_list()
    subject_ids_train, subject_ids_test = iterative_stratification(
        subject_ids, codes, [1 - TEST_SIZE, TEST_SIZE]
    )
    codes_train = [
        codes[subject_ids.index(subject_id)] for subject_id in subject_ids_train
    ]
    val_size = VAL_SIZE / (1 - TEST_SIZE)
    subject_ids_train, subject_ids_val = iterative_stratification(
        subject_ids_train, codes_train, [1 - val_size, val_size]
    )

    codes_train = [
        codes[subject_ids.index(subject_id)] for subject_id in subject_ids_train
    ]
    codes_val = [codes[subject_ids.index(subject_id)] for subject_id in subject_ids_val]
    codes_test = [
        codes[subject_ids.index(subject_id)] for subject_id in subject_ids_test
    ]

    splits.loc[splits[SUBJECT_ID_COLUMN].isin(subject_ids_train), "split"] = "train"
    splits.loc[splits[SUBJECT_ID_COLUMN].isin(subject_ids_val), "split"] = "val"
    splits.loc[splits[SUBJECT_ID_COLUMN].isin(subject_ids_test), "split"] = "test"

    logging.info("------------- Splits Statistics -------------")
    logging.info(
        f"Labels missing in the test set: {labels_not_in_split(codes, codes_test)}"
    )
    logging.info(
        f"Labels missing in the val set: {labels_not_in_split(codes, codes_val)} %"
    )
    logging.info(
        f"Labels missing in the train set: {labels_not_in_split(codes, codes_train)} %"
    )
    logging.info(f"Test: KL divergence: {kl_divergence(codes, codes_test)}")
    logging.info(f"Val: KL divergence: {kl_divergence(codes, codes_val)}")
    logging.info(f"Train: KL divergence: {kl_divergence(codes, codes_train)}")
    logging.info(f"Test Size: {len(codes_test) / len(codes)}")
    logging.info(f"Val Size: {len(codes_val) / len(codes)}")
    logging.info(f"Train Size: {len(codes_train) / len(codes)}")

    splits = splits[[ID_COLUMN, "split"]].reset_index(drop=True)
    splits.to_feather(split_path)
    logging.info(
        "Splits generated and saved. Now making subsplits used to analyse the performance of the models when trained on less data."
    )


def generate_training_subset(
    dataset: pd.DataFrame,
    splits: pd.DataFrame,
    number_of_training_examples: int,
    split_path: Path,
):
    dataset = pd.merge(dataset, splits, on=ID_COLUMN)
    training_set = dataset[dataset["split"] == "train"]
    val_set = dataset[dataset["split"] == "val"]
    test_set = dataset[dataset["split"] == "test"]

    size = number_of_training_examples / len(training_set)
    subject_series = training_set.groupby(SUBJECT_ID_COLUMN)[TARGET_COLUMN].sum()
    subject_ids = subject_series.index.to_list()
    codes = subject_series.to_list()
    _, subject_ids_train_subset = iterative_stratification(
        subject_ids, codes.copy(), [1 - size, size]
    )
    codes_train_subset = [
        codes[subject_ids.index(subject_id)] for subject_id in subject_ids_train_subset
    ]
    logging.info("------------- Splits Statistics -------------")

    logging.info(
        f"Labels missing in the training subset: {labels_not_in_split(codes, codes_train_subset)} %"
    )
    logging.info(
        f"Train subset: KL divergence: {kl_divergence(codes, codes_train_subset)}"
    )
    logging.info(f"Train subset size: {len(codes_train_subset) / len(codes)}")

    training_set = training_set[
        training_set[SUBJECT_ID_COLUMN].isin(subject_ids_train_subset)
    ]
    dataset = pd.concat([training_set, val_set, test_set])[
        [ID_COLUMN, "split"]
    ].reset_index(drop=True)
    dataset.to_feather(split_path)
    
    
    
generate_split(mimic_icd10, output_dir_icd10 / "mimiciv_icd10_split.feather")


mimic_icd10_splits = pd.read_feather(output_dir_icd10 / "mimiciv_icd10_split.feather")
generate_training_subset(
    mimic_icd10,
    mimic_icd10_splits,
    75_000,
    output_dir_icd10 / "mimiciv_icd10_train_subset_75k.feather",
)
generate_training_subset(
    mimic_icd10,
    mimic_icd10_splits,
    50_000,
    output_dir_icd10 / "mimiciv_icd10_train_subset_50k.feather",
)
generate_training_subset(
    mimic_icd10,
    mimic_icd10_splits,
    25_000,
    output_dir_icd10 / "mimiciv_icd10_train_subset_25k.feather",
)
generate_training_subset(
    mimic_icd10,
    mimic_icd10_splits,
    10_000,
    output_dir_icd10 / "mimiciv_icd10_train_subset_10k.feather",
)


print("Done")