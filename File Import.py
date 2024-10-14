import os
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
import concurrent.futures
from PyPDF2 import PdfReader

# Find and import config file
config_path = os.getcwd()
sys.path.append(config_path)
import config

print(sys.path)

database = config.database
central_banks = config.central_banks
training_data = os.path.join(database, "Training Data")
fed_docs = config.fed_docs
ecb_docs = config.ecb_docs
boe_docs = config.boe_docs

# Get content of training data
training_data_files = os.listdir(training_data)

file_dir = pd.DataFrame(columns=["central bank", "document", "type", "filepath"])

csv_filepaths = []


for i in central_banks:
    if i == "fed":
        fed_training_data = os.path.join(training_data, "fed")
        for j in fed_docs:
            doc_list = os.path.join(fed_training_data, j)
            if j == "beigebooks":
                doc_type = "PDF"
            if j in ["fed_statements", "fed_minutes", "fed_speeches"]:
                doc_type = "CSV"
            file_dir = pd.concat(
                [
                    file_dir,
                    pd.DataFrame(
                        {
                            "central bank": [i],
                            "document": [j],
                            "type": [doc_type],
                            "filepath": [doc_list],
                        }
                    ),
                ],
                ignore_index=True,
            )
    if i == "ecb":
        ecb_training_data = os.path.join(training_data, "ecb")
        for j in ecb_docs:
            doc_list = os.path.join(ecb_training_data, j)
            if j == "economic_bulletins":
                doc_type = "PDF"
            if j in ["monetary_policy_accounts", "ecb_speeches"]:
                doc_type = "CSV"
            if j == "press_conferences":
                doc_type = "XLSX"
            file_dir = pd.concat(
                [
                    file_dir,
                    pd.DataFrame(
                        {
                            "central bank": [i],
                            "document": [j],
                            "type": [doc_type],
                            "filepath": [doc_list],
                        }
                    ),
                ],
                ignore_index=True,
            )

    if i == "boe":
        boe_training_data = os.path.join(training_data, "boe")
        for j in boe_docs:
            doc_list = os.path.join(boe_training_data, j)

            if j == "boe_minutes":
                doc_type = "CSV"
            if j == "boe_speeches":
                doc_type = "CSV"
            file_dir = pd.concat(
                [
                    file_dir,
                    pd.DataFrame(
                        {
                            "central bank": [i],
                            "document": [j],
                            "type": [doc_type],
                            "filepath": [doc_list],
                        }
                    ),
                ],
                ignore_index=True,
            )

for i in range(len(file_dir)):
    # Get list of files in filepath folder
    file_list = os.listdir(file_dir["filepath"][i])

    # print(file_list)
    if file_dir["central bank"][i] == "boe":

        if file_dir["document"][i] == "boe_speeches":
            for j in file_list:
                if j.endswith(".csv"):
                    url = os.path.join(file_dir["filepath"][i], j)
            raw_text = pd.read_csv(url)

        if file_dir["document"][i] == "boe_minutes":
            for j in file_list:
                if j.endswith(".csv"):
                    url = os.path.join(file_dir["filepath"][i], j)
            raw_text = pd.read_csv(url)
            raw_text = raw_text.rename(
                columns={
                    "date": "date",
                    "text": "segment",
                }
            )
            raw_text["group"] = "boe_minutes"

    if file_dir["central bank"][i] == "fed":

        if file_dir["document"][i] == "fed_statements":
            for j in file_list:
                if j.endswith(".csv"):
                    url = os.path.join(file_dir["filepath"][i], file_list[0])
            raw_text = pd.read_csv(url)
            raw_text = raw_text.rename(
                columns={
                    "Date": "date",
                    "Text": "segment",
                    "Type": "group",
                }
            )
            raw_text["date"] = pd.to_datetime(raw_text["date"], format="%Y-%m-%d")

        if file_dir["document"][i] == "fed_minutes":
            for j in file_list:
                if j.endswith(".csv"):
                    url = os.path.join(file_dir["filepath"][i], file_list[0])
            raw_text = pd.read_csv(url)
            raw_text = raw_text.rename(
                columns={
                    "document_kind": "group",
                    "text": "segment",
                    "release_date": "date",
                }
            )
            raw_text["date"] = pd.to_datetime(raw_text["date"], format="mixed")

        if file_dir["document"][i] == "beigebooks":
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
                print(date, url)
                pdf_out.append(
                    {
                        "date": date,
                        "segment": whole_text,
                        "group": date,
                    }
                )
                raw_text = pd.DataFrame(pdf_out)
                raw_text["date"] = pd.to_datetime(raw_text["date"], format="%Y%m%d")

        if file_dir["document"][i] == "fed_speeches":
            url = os.path.join(file_dir["filepath"][i], file_list[0])
            if file_list[0].endswith(".csv"):
                raw_text = pd.read_csv(url)
            raw_text["date"] = raw_text["date"].fillna(0)
            raw_text = raw_text[raw_text["date"] != 0]
            raw_text["date"] = raw_text["date"].astype(int)
            raw_text = raw_text.rename(
                columns={
                    "text": "segment",
                    "speaker": "group",
                }
            )
            raw_text["date"] = pd.to_datetime(raw_text["date"], format="%Y%m%d")

    if file_dir["central bank"][i] == "ecb":

        if file_dir["document"][i] == "economic_bulletins":
            pdf_out = []
            for file in file_list:
                url = os.path.join(file_dir["filepath"][i], file)
                date = file[2:10]  # Extract the date from the file name
                if file.endswith(".DS_Store"):
                    continue
                if file.endswith(".pdf"):
                    pdf = PdfReader(url)
                    whole_text = []
                    for j in range(len(pdf.pages)):
                        whole_text.append(pdf.pages[j].extract_text())
                print(date, url)
                pdf_out.append(
                    {
                        "date": date,
                        "segment": whole_text,
                        "group": "date",
                    }
                )
            raw_text = pd.DataFrame(pdf_out)
            raw_text["date"] = pd.to_datetime(raw_text["date"], format="%Y%m%d")

        if file_dir["document"][i] == "monetary_policy_accounts":
            url = os.path.join(file_dir["filepath"][i], file_list[0])
            if file_list[0].endswith(".xlsx"):
                raw_text = pd.read_excel(url)
            raw_text["date"] = pd.to_datetime(raw_text["date"], format="%Y%m%d")
            raw_text["segment"] = raw_text["segment"].astype(str)
            raw_text = raw_text.rename(columns={"title": "group"})

        if file_dir["document"][i] == "press_conferences":
            # url = os.path.join(file_dir["filepath"][i], file_list[0])
            url = "/Users/kylenabors/Documents/Database/Training Data/ecb/press_conferences/Press Conferences.xlsx"
            raw_text = pd.read_excel(url)
            raw_text = raw_text.rename(
                columns={
                    "Date": "date",
                    "Title": "group",
                    "firstPart": "segment",
                }
            )
            raw_text["date"] = pd.to_datetime(raw_text["date"], format="%y/%m/%d")

        if file_dir["document"][i] == "ecb_speeches":
            url = os.path.join(file_dir["filepath"][i], file_list[0])
            if file_list[0].endswith(".csv"):
                raw_text = pd.read_csv(
                    url,
                    sep="|",
                    encoding="UTF-8",
                )
            raw_text["contents"] = raw_text["contents"].astype(str)
            raw_text["len"] = len(raw_text["contents"])
            raw_text = raw_text[raw_text["len"] > 4]
            raw_text = raw_text.rename(
                columns={
                    "contents": "segment",
                    "speakers": "group",
                }
            )
            raw_text["date"] = pd.to_datetime(raw_text["date"], format="%Y-%m-%d")

    try:
        raw_text = raw_text[["date", "group", "segment"]]
    except:
        print("Error: ", file_dir["central bank"][i], file_dir["document"][i])
        continue
    raw_text.to_csv(
        os.path.join(
            database,
            "Processed Text",
            file_dir["central bank"][i],
            file_dir["document"][i],
            "raw_text.csv",
        ),
        index=False,
    )

    url_map.append(
        {
            "central bank": file_dir["central bank"][i],
            "document": file_dir["document"][i],
            "url": os.path.join(
                database,
                "Processed Text",
                file_dir["central bank"][i],
                file_dir["document"][i],
                "raw_text.csv",
            ),
        }
    )

url_map = pd.DataFrame(url_map)
url_map.to_csv(config_path + "/url_map.csv", index=False)

print(url_map)
