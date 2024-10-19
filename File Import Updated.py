# The change in this file import is the assumption all files are CSVs to reduce complexity and redundancy.

import os
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
from PyPDF2 import PdfReader

# Find and import config file
config_path = os.getcwd()
sys.path.append(config_path)
import config

print(sys.path)

database = config.database
central_banks = config.central_banks
training_data = os.path.join(database, "Training Data")

# Define documents for each central bank
bank_docs = {
    "fed": config.fed_docs,
    "ecb": config.ecb_docs,
    "boe": config.boe_docs,
    "boj": config.boj_docs,
    "boa": config.boa_docs,
    "boc": config.boc_docs,
    "bosweden": config.bosweden_docs,
    "boswiss": config.boswiss_docs,
}

# Define document types
doc_types = {
    "beigebooks": "PDF",
    "fed_statements": "CSV",
    "fed_minutes": "CSV",
    "fed_speeches": "CSV",
    "economic_bulletins": "CSV",
    "monetary_policy_accounts": "CSV",
    "ecb_speeches": "CSV",
    "press_conferences": "CSV",
    "boe_minutes": "CSV",
    "boe_speeches": "CSV",
    "boj_minutes": "CSV",
    "boj_speeches": "CSV",
    "boa_speeches": "CSV",
    "boc_speeches": "CSV",
    "bosweden_speeches": "CSV",
    "boswiss_speeches": "CSV",
}

# Initialize the file_dir DataFrame
file_dir = pd.DataFrame(columns=["central bank", "document", "type", "filepath"])

for bank in central_banks:
    bank_training_data = os.path.join(training_data, bank)
    for doc in bank_docs.get(bank, []):
        doc_list = os.path.join(bank_training_data, doc)
        doc_type = doc_types.get(doc, "Unknown")
        file_dir = pd.concat(
            [
                file_dir,
                pd.DataFrame(
                    {
                        "central bank": [bank],
                        "document": [doc],
                        "type": [doc_type],
                        "filepath": [doc_list],
                    }
                ),
            ],
            ignore_index=True,
        )

url_map = []


# Define helper functions
def process_speeches_csv(filepath, date_format="mixed"):
    raw_text = pd.read_csv(filepath)
    # print(f"Columns in raw_text after reading CSV: {raw_text.columns.tolist()}")
    raw_text = raw_text.rename(
        columns={
            "text": "segment",
            "speaker": "group",
            "author": "group",
            "Type": "group",
            "Text": "segment",
            "release_date": "date",
            "Release Date": "date",
        }
    )
    if "date" in raw_text.columns:
        raw_text["date"] = raw_text["date"].fillna(0)
        raw_text = raw_text[raw_text["date"] != 0]
        try:
            raw_text["date"] = pd.to_datetime(raw_text["date"], format="%Y%m%d")
        except ValueError:
            raw_text["date"] = pd.to_datetime(raw_text["date"], format=date_format)
    else:
        raw_text["date"] = pd.NaT
    # This is to add a group for each document. So far has not been used so I am deprecating this function
    # return raw_text[["date", "group", "segment"]]
    return raw_text[["date", "segment"]]


# Main processing loop
for i in range(len(file_dir)):
    bank = file_dir.loc[i, "central bank"]
    document = file_dir.loc[i, "document"]
    doc_type = file_dir.loc[i, "type"]
    filepath = file_dir.loc[i, "filepath"]

    raw_text = None
    # Get list of files in filepath folder
    file_list = [f for f in os.listdir(filepath) if not f.startswith(".")]

    if doc_type == "CSV":
        for file_name in file_list:
            if file_name.endswith(".csv"):
                url = os.path.join(filepath, file_name)
                if document.endswith("_speeches"):
                    # For speeches
                    raw_text = process_speeches_csv(url)
                else:
                    raw_text = process_speeches_csv(url)
                    pass
                break  # Found CSV file
    elif doc_type == "PDF":
        pdf_out = []
        for file in file_list:
            url = os.path.join(file_dir["filepath"][i], file)
            if file.endswith(".DS_Store"):
                continue
            if file.endswith(".pdf"):
                pdf = PdfReader(url)
                file = file.replace(".pdf", "")
            if "_" in file:
                date = file.split("_")[1]
            else:
                date = date[:10]
            whole_text = []
            for j in range(len(pdf.pages)):
                whole_text.append(pdf.pages[j].extract_text())
            pdf_out.append(
                {
                    "date": date,
                    "segment": whole_text,
                }
            )
        raw_text = pd.DataFrame(pdf_out)
        raw_text["date"] = pd.to_datetime(raw_text["date"], format="%Y%m%d")
        pass
    elif doc_type == "XLSX":
        # Process XLSX files as per your existing code
        pass
    else:
        # Handle other document types or raise an error
        pass

    # Save the processed data
    if raw_text is not None:
        output_dir = os.path.join(
            database,
            "Processed Text",
            bank,
            document,
        )
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        raw_text.to_csv(
            os.path.join(
                output_dir,
                "raw_text.csv",
            ),
            index=False,
        )

        url_map.append(
            {
                "central bank": bank,
                "document": document,
                "url": os.path.join(
                    output_dir,
                    "raw_text.csv",
                ),
            }
        )

# Save url_map
url_map = pd.DataFrame(url_map)
url_map.to_csv(os.path.join(config_path, "url_map.csv"), index=False)

print(url_map)
