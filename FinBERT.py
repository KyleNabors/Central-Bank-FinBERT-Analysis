import os
import sys
import pandas as pd
import torch
from tqdm import tqdm
import numpy as np

from transformers import (
    BertTokenizer,
    BertForSequenceClassification,
)

from transformers import BertTokenizer, pipeline
import platform
from tqdm.auto import tqdm
from pathlib import Path

cwd = os.getcwd()
# Find and import config file
config_path = os.getcwd()

sys.path.append(config_path)
import config

database = config.database
central_banks = config.central_banks
training_data = os.path.join(database, "Training Data")
fed_docs = config.fed_docs
ecb_docs = config.ecb_docs

platform.platform()

torch.backends.mps.is_built()

if torch.backends.mps.is_available():
    mps_device = torch.device("mps")
    x = torch.ones(1, device=mps_device)
    print(x)
else:
    print("MPS device not found.")

model = BertForSequenceClassification.from_pretrained(
    "ZiweiChen/FinBERT-FOMC", num_labels=3
).to("mps")

tokenizer = BertTokenizer.from_pretrained("ZiweiChen/FinBERT-FOMC")

finbert_fomc = pipeline(
    "text-classification", model=model, tokenizer=tokenizer, device="mps"
)

url_map = pd.read_csv(os.path.join(cwd, "url_map.csv"))

finbert_urls = []

for i in range(len(url_map)):
    tqdm.pandas()
    print(url_map["central bank"][i], url_map["document"][i])
    url = url_map["processed_url"][
        i
    ]  # For uniform, or use processed_url_weighted for weighted
    df = pd.read_csv(url, low_memory=False)
    # Pass through weights for analysis
    if "weight" in df.columns:
        weights = df["weight"]
    else:
        weights = np.ones(len(df))  # Default to 1 if not present

    df["sentence_simple"] = np.where(
        df["len"] < 10, df["segment"], df["sentence_simple"]
    )
    df["sentiment"] = df["sentence_simple"].progress_apply(lambda x: finbert_fomc(x))
    df["sentiment"] = df["sentiment"].apply(lambda x: x[0]["label"])
    df["sentiment"] = df["sentiment"].replace(
        {"Positive": 1, "Neutral": 0, "Negative": -1}
    )
    # Save weights in output for further analysis
    df["weight"] = weights

    finbert_url = os.path.join(
        database,
        "FinBERT Models",
        url_map["central bank"][i],
        url_map["document"][i],
    )
    Path(finbert_url).mkdir(parents=True, exist_ok=True)
    finbert_url = os.path.join(
        finbert_url,
        "finbert.csv",
    )
    df.to_csv(finbert_url, index=False)
    finbert_urls.append(finbert_url)

url_map["finbert_url"] = finbert_urls
url_map.to_csv(os.path.join(cwd, "url_map.csv"), index=False)
