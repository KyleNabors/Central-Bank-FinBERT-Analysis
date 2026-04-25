import os
import pandas as pd
import torch
from tqdm.auto import tqdm
from transformers import BertTokenizer, BertForSequenceClassification, pipeline

# text processing imports
import spacy
from nltk.tokenize import sent_tokenize, word_tokenize
import random
import re
import numpy as np

# --- USER CONFIG ---
INPUT_CSV = "/Users/kylenabors/Documents/Database/Training Data/fed/fed_minutes/meeting_minutes.csv"
OUTPUT_XLSX = "/Users/kylenabors/Documents/Database/Training Data/fed/fed_minutes/oneoff_sentiment_comparison.xlsx"
MAX_SENT_LENGTH = 512
MAX_PUBS = 100000  # max number of most recent publications to process
MIN_PROCESSED_LEN = (
    100  # minimum length (chars) of the processed sentence required to include the row
)

# Device setup (CPU fallback for reliability)
if torch.cuda.is_available():
    device = 0
# --- DEVICE SETUP FOR APPLE SILICON (MPS) ---
if torch.backends.mps.is_available():
    device = torch.device("mps")
    _test = torch.ones(1, device=device)
    print(f"MPS active: {_test}")
else:
    print("MPS device not found. Falling back to CPU.")
    device = torch.device("cpu")


# Seed for reproducibility
SEED = 0
random.seed(SEED)
np.random.seed(SEED)
torch.manual_seed(SEED)

# Load FinBERT model and tokenizer
model = BertForSequenceClassification.from_pretrained(
    "ZiweiChen/FinBERT-FOMC", num_labels=3
).to(device)
model.eval()
tokenizer = BertTokenizer.from_pretrained("ZiweiChen/FinBERT-FOMC")

# Pipeline (HuggingFace now supports torch.device for MPS)
finbert_fomc = pipeline(
    "text-classification",
    model=model,
    tokenizer=tokenizer,
    device=device,  # will use MPS if available, else CPU
)

# Load spaCy
try:
    nlp = spacy.load("en_core_web_lg")
except Exception:
    nlp = spacy.load("en_core_web_sm")


# Re-use preprocessing helpers from Text Processing Threading
def remove_comma(sentence):
    doc = nlp(sentence)
    indices = []
    for i, token in enumerate(doc):
        if token.dep_ == "punct":
            if i + 1 < len(doc):
                next_token = doc[i + 1]
                if next_token.dep_ == "ROOT" or next_token.dep_ == "conj":
                    indices.append(i)
    if not indices:
        return sentence
    parts = []
    last_idx = 0
    for idx in indices:
        parts.append(doc[last_idx:idx].text.strip())
        last_idx = idx + 1
    parts.append(doc[last_idx:].text.strip())
    return " ".join(parts)


def sentiment_focus(sentence):
    doc = nlp(sentence)
    focus_changed = 1
    # "but" rule
    for token in doc[:-1]:
        if token.lower_ == "but":
            return str(doc[token.i + 1 :]).strip(), focus_changed
    # although / though rules
    for sent in doc.sents:
        sent_tokens = [token for token in sent]
        for token in sent_tokens:
            if token.lower_ in {"although", "though"}:
                comma_back_list = [t.i for t in doc[token.i :] if t.text == ","]
                if comma_back_list:
                    comma_index_back = comma_back_list[0]
                else:
                    comma_front_list = [t.i for t in doc[: token.i] if t.text == ","]
                    if comma_front_list:
                        comma_index_front = comma_front_list[-1]
                        return str(doc[:comma_index_front]).strip(), focus_changed
                    else:
                        return str(doc).strip(), focus_changed
                comma_front_list = [t.i for t in doc[: token.i] if t.text == ","]
                if comma_front_list:
                    comma_index_front = comma_front_list[-1]
                else:
                    return str(doc[comma_index_back + 1 :]).strip(), focus_changed
                return (
                    str(
                        doc[:comma_index_front].text + doc[comma_index_back:].text
                    ).strip(),
                    focus_changed,
                )
    # while rule
    if len(doc) > 0 and doc[0].lower_ == "while":
        comma_back_list = [t.i for t in doc if t.text == ","]
        if comma_back_list:
            comma_index_back1 = comma_back_list[0]
            return str(doc[comma_index_back1 + 1 :]).strip(), focus_changed
        else:
            return str(doc).strip(), focus_changed
    return str(doc).strip(), 0


def preprocess_sentence(raw_sentence):
    # tokenise words, clean, remove commas and apply focus heuristic
    words = word_tokenize(raw_sentence)
    segment_clean = " ".join(words)
    s1 = remove_comma(segment_clean)
    sentence_simple, _ = sentiment_focus(s1)
    # fallback to original if too short or empty
    if sentence_simple is None or len(sentence_simple) < MIN_PROCESSED_LEN:
        return segment_clean
    # truncate if too long
    return sentence_simple[:MAX_SENT_LENGTH]


# --- Main: build table of (sentence, raw_sentiment_score, preprocessed_sentiment_score, scores_differ) ---
def main():
    df_in = pd.read_csv(INPUT_CSV, low_memory=False)

    # ensure release_date exists or fall back to known date columns
    if "release_date" not in df_in.columns and "date" in df_in.columns:
        df_in = df_in.rename(columns={"date": "release_date"})
    elif "release_date" not in df_in.columns and "release_date" not in df_in.columns:
        # No explicit date column, try common alternatives
        for candidate in (
            "releaseDate",
            "Release Date",
            "release_date",
            "pub_date",
            "published",
        ):
            if candidate in df_in.columns:
                df_in = df_in.rename(columns={candidate: "release_date"})
                break

    # parse dates and sort descending, keep most recent MAX_PUBS publications
    if "release_date" in df_in.columns:
        df_in["release_date_parsed"] = pd.to_datetime(
            df_in["release_date"], errors="coerce"
        )
        # drop rows with invalid dates for the selection step
        df_valid_dates = df_in.dropna(subset=["release_date_parsed"])
        if not df_valid_dates.empty:
            df_recent = (
                df_valid_dates.sort_values("release_date_parsed", ascending=False)
                .head(MAX_PUBS)
                .copy()
            )
        else:
            # if no valid parsed dates, fallback to last MAX_PUBS rows
            df_recent = df_in.tail(MAX_PUBS).copy()
    else:
        # no release_date column at all: just take last MAX_PUBS rows
        df_recent = df_in.tail(MAX_PUBS).copy()

    print(f"Processing {len(df_recent)} publications (most recent {MAX_PUBS})")

    # Determine text column on the (filtered) DataFrame
    text_col = None
    for candidate in ("segment", "text", "Text", "sentence", "content"):
        if candidate in df_recent.columns:
            text_col = candidate
            break
    if text_col is None:
        for c in df_recent.columns:
            if df_recent[c].dtype == object:
                text_col = c
                break
    if text_col is None:
        raise RuntimeError("No text column found in input CSV.")

    rows = []
    for idx, row in tqdm(
        df_recent.iterrows(), total=len(df_recent), desc="publications"
    ):
        segment = str(row[text_col])
        # split into sentences
        sents = sent_tokenize(segment)
        for sent in sents:
            sent = sent.strip()
            if len(sent) == 0 or len(sent) > MAX_SENT_LENGTH:
                continue

            # preprocess first and enforce minimum processed length
            preproc = preprocess_sentence(sent)
            if preproc is None or len(preproc) < MIN_PROCESSED_LEN:
                # skip this sentence because processed output is too short
                continue

            # raw sentiment -> score only
            try:
                raw_res = finbert_fomc(sent)
                raw_label = raw_res[0]["label"]
                raw_score = {"Positive": 1, "Neutral": 0, "Negative": -1}.get(
                    raw_label, None
                )
            except Exception:
                raw_score = None

            # preprocessed sentence and sentiment -> score only
            try:
                proc_res = finbert_fomc(preproc)
                proc_label = proc_res[0]["label"]
                proc_score = {"Positive": 1, "Neutral": 0, "Negative": -1}.get(
                    proc_label, None
                )
            except Exception:
                proc_score = None

            # indicate whether scores differ (handles None correctly: None != None -> False)
            scores_differ = raw_score != proc_score

            rows.append(
                {
                    "publication_index": idx,
                    "release_date": row.get("release_date", None),
                    "raw_sentence": sent,
                    "raw_sentiment_score": raw_score,
                    "preprocessed_sentence": preproc,
                    "preprocessed_sentiment_score": proc_score,
                    "scores_differ": scores_differ,
                }
            )

    out = pd.DataFrame(rows)
    # Reorder so the two sentiment scores are adjacent for easy comparison
    desired_order = [
        "publication_index",
        "release_date",
        "raw_sentence",
        "preprocessed_sentence",
        "raw_sentiment_score",
        "preprocessed_sentiment_score",
        "scores_differ",
    ]
    # Preserve any unexpected columns by appending them after the desired order
    remaining = [c for c in out.columns if c not in desired_order]
    ordered_cols = [c for c in desired_order if c in out.columns] + remaining
    out = out[ordered_cols]

    # Save as Excel
    out.to_excel(OUTPUT_XLSX, index=False, engine="openpyxl")
    print(f"Saved comparison table to: {OUTPUT_XLSX}")


if __name__ == "__main__":
    main()
