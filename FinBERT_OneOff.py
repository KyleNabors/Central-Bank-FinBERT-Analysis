import os
import pandas as pd
import torch
from tqdm import tqdm
from transformers import BertTokenizer, BertForSequenceClassification, pipeline

# Device setup
if torch.backends.mps.is_available():
    device = "mps"
else:
    device = -1  # CPU

# Load FinBERT model and tokenizer
model = BertForSequenceClassification.from_pretrained(
    "ZiweiChen/FinBERT-FOMC", num_labels=3
)
tokenizer = BertTokenizer.from_pretrained("ZiweiChen/FinBERT-FOMC")
finbert_fomc = pipeline(
    "text-classification", model=model, tokenizer=tokenizer, device=device
)

# --- USER INPUT SECTION ---
excel_path = "/Users/kylenabors/Downloads/Sentences.xlsx"  # <-- Change this to your Excel file path
sheet_name = 0  # or the name of the sheet

# Read sentences from Excel
df = pd.read_excel(excel_path, sheet_name=sheet_name)
# Assume the column with sentences is named 'sentence'
if "sentence" not in df.columns:
    raise ValueError("Excel file must have a column named 'sentence'.")

tqdm.pandas()
df["sentiment_raw"] = df["sentence"].progress_apply(lambda x: finbert_fomc(str(x)))
df["sentiment_label"] = df["sentiment_raw"].apply(lambda x: x[0]["label"])
df["sentiment_score"] = df["sentiment_label"].replace(
    {"Positive": 1, "Neutral": 0, "Negative": -1}
)

# Save results to Excel
output_path = "/Users/kylenabors/Downloads/sentences_with_sentiment.xlsx"
df.to_excel(output_path, index=False)
print(f"Sentiment scores saved to {output_path}")
