import os
import sys
import pandas as pd
import torch
from tqdm import tqdm
import numpy as np

from transformers import (
    BertTokenizer,
    BertForSequenceClassification,
    pipeline,
)
import platform
from tqdm.auto import tqdm
from pathlib import Path

cwd = os.getcwd()
# Find and import config file
config_path = os.getcwd()
sys.path.append(config_path)
import config

# Set database path based on OS
if platform.system() == "Windows":
    database = r"C:\Users\Kyle N\Documents\Database"
else:
    database = config.database

central_banks = config.central_banks
training_data = os.path.join(database, "Training Data")
fed_docs = config.fed_docs
ecb_docs = config.ecb_docs

# Device selection logic
if platform.system() == "Darwin" and torch.backends.mps.is_available():
    device_str = "mps"
    torch_device = torch.device("mps")
    print("Using Apple Silicon MPS device.")
elif torch.cuda.is_available():
    device_str = 0  # CUDA device index for pipeline
    torch_device = torch.device("cuda")
    print("Using Nvidia CUDA device.")
else:
    device_str = -1  # CPU for pipeline
    torch_device = torch.device("cpu")
    print("Using CPU.")

# Model and tokenizer
model = BertForSequenceClassification.from_pretrained(
    "ZiweiChen/FinBERT-FOMC", num_labels=3
).to(torch_device)

tokenizer = BertTokenizer.from_pretrained("ZiweiChen/FinBERT-FOMC")

finbert_fomc = pipeline(
    "text-classification", model=model, tokenizer=tokenizer, device=device_str
)

url_map = pd.read_csv(os.path.join(cwd, "url_map.csv"))

finbert_urls = []

for i in range(len(url_map)):
    tqdm.pandas()
    print(url_map["central bank"][i], url_map["document"][i])
    url = url_map["processed_url"][i]
    # Remove any leading user/home directory from the path
    url = url.replace("\\", "/")
    # Find the part after 'Database/' (works for both Windows and Mac paths)
    if "Database/" in url:
        url = url.split("Database/", 1)[1]
    elif "Database\\" in url:
        url = url.split("Database\\", 1)[1]
    # Always join with database root
    url = os.path.join(database, url)
    url = os.path.normpath(url)
    df = pd.read_csv(url, low_memory=False)
    # Pass through weights for analysis
    if "weight" in df.columns:
        weights = df["weight"]
    else:
        weights = np.ones(len(df))  # Default to 1 if not present

    df["sentence_simple"] = np.where(
        df["len"] < 10, df["segment"], df["sentence_simple"]
    )
    # Batch process sentences for efficiency
    sentences = df["sentence_simple"].tolist()
    batch_size = 16384  # You can adjust this based on your GPU memory
    sentiments = []
    for batch_start in tqdm(range(0, len(sentences), batch_size)):
        batch = sentences[batch_start : batch_start + batch_size]
        batch_results = finbert_fomc(batch)
        sentiments.extend([result["label"] for result in batch_results])
    df["sentiment"] = sentiments
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
