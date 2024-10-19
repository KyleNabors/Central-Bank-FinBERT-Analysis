import os
import sys
import pandas as pd
from tqdm.auto import tqdm
from nltk.tokenize import sent_tokenize, word_tokenize
import numpy as np
from multiprocessing import Pool, cpu_count

# Other imports
import spacy
from langdetect import detect_langs

# Define global variables and functions here

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
boe_docs = config.boe_docs


def get_chunks(s, maxlength):
    start = 0
    end = 0
    while start + maxlength < len(s) and end != -1:
        end = s.rfind(" ", start, start + maxlength + 1)
        yield s[start:end]
        start = end + 1
    yield s[start:]


def detect_language_with_langdetect(line):
    try:
        langs = detect_langs(line)
        for item in langs:
            # The first one returned is usually the one that has the highest probability
            return item.lang, item.prob
    except:
        return "err", 0.0


# Initialize spaCy in each worker process
def init_worker():
    global nlp
    nlp = spacy.load("en_core_web_lg")


def remove_comma(sentence):
    global nlp
    doc = nlp(sentence)
    indices = []
    for i, token in enumerate(doc):
        if token.dep_ == "punct":
            try:
                next_token = doc[i + 1]
                if next_token.dep_ == "ROOT" or next_token.dep_ == "conj":
                    indices.append(i)
            except IndexError:
                pass
    if not indices:
        return sentence
    else:
        parts = []
        last_idx = 0
        for idx in indices:
            parts.append(doc[last_idx:idx].text.strip())
            last_idx = idx + 1
        parts.append(doc[last_idx:].text.strip())
        return " ".join(parts)


def sentiment_focus(sentence):
    global nlp
    doc = nlp(sentence)
    focus = ""
    focus_changed = 1
    for token in doc[:-1]:
        if token.lower_ == "but":
            focus = doc[token.i + 1 :]
            return str(focus).strip(), focus_changed

    for sent in doc.sents:
        sent_tokens = [token for token in sent]
        for token in sent_tokens:
            if token.lower_ == "although" or token.lower_ == "though":
                try:
                    comma_index_back = [
                        token1.i for token1 in doc[token.i :] if token1.text == ","
                    ][0]
                except IndexError:
                    try:
                        comma_index_front = [
                            token1.i for token1 in doc[: token.i] if token1.text == ","
                        ][-1]
                    except IndexError:
                        return str(doc).strip(), focus_changed
                    focus = doc[:comma_index_front].text
                    return str(focus).strip(), focus_changed
                try:
                    comma_index_front = [
                        token1.i for token1 in doc[: token.i] if token1.text == ","
                    ][-1]
                except IndexError:
                    focus = doc[comma_index_back + 1 :].text
                    return str(focus).strip(), focus_changed
                focus = doc[:comma_index_front].text + doc[comma_index_back:].text
                return str(focus).strip(), focus_changed

    if doc[0].lower_ == "while":
        try:
            comma_index_back1 = [token2.i for token2 in doc if token2.text == ","][0]
        except IndexError:
            return str(doc).strip(), focus_changed
        focus = doc[comma_index_back1 + 1 :].text
        return str(focus).strip(), focus_changed

    focus_changed = 0
    return str(doc).strip(), focus_changed


def process_chunk(df_chunk):
    output = []
    lenlist = []
    for i in range(len(df_chunk)):
        date = df_chunk["date"].iloc[i]
        # group = df_chunk["group"].iloc[i]
        segment = df_chunk["segment"].iloc[i]
        segment = segment.replace("\n", " ")
        segment = segment.replace("\r", " ")
        segment = segment.replace("\t", " ")
        segment = segment.replace("  ", " ")
        seglen = len(segment)
        lenlist.append(seglen)
        language, langprob = detect_language_with_langdetect(segment)
        blocks = get_chunks(segment, 1000)
        for block in blocks:
            sentences = sent_tokenize(block)
            for sentence in sentences:
                if 60 < len(sentence) < 512:
                    words = word_tokenize(sentence)
                    segment = " ".join(words)
                    output.append(
                        {
                            "date": date,
                            # "group": group,
                            "segment": segment,
                            "length": len(segment),
                            "language": language,
                            "lang_prob": langprob,
                            "doc_len": seglen,
                        }
                    )
    df_result = pd.DataFrame(output)

    df_result["segment"] = df_result["segment"].apply(remove_comma)
    df_result["sentence_simple"] = df_result["segment"].apply(remove_comma)
    # Processing sentiment focus
    df_result[["sentence_simple", "focus_changed"]] = (
        df_result["sentence_simple"].apply(sentiment_focus).apply(pd.Series)
    )

    df_result.drop("focus_changed", axis=1, inplace=True)

    df_result["len"] = df_result["sentence_simple"].apply(lambda x: len(x))
    df_result["sentence_simple"] = np.where(
        df_result["len"] < 10, df_result["segment"], df_result["sentence_simple"]
    )
    prelen = len(df_result)
    df_result = df_result[df_result["len"] < 512]
    postlen = len(df_result)
    print(f"Removed {prelen - postlen} sentences with length > 512")

    return df_result


def text_process(df):
    num_processes = cpu_count()
    chunks = np.array_split(df, num_processes)

    with Pool(processes=num_processes, initializer=init_worker) as pool:
        results = pool.map(process_chunk, chunks)

    df_result = pd.concat(results, ignore_index=True)
    return df_result


if __name__ == "__main__":
    # Main code that uses multiprocessing should go here
    url_map = pd.read_csv(os.path.join(cwd, "url_map.csv"))

    processed_urls = []
    for i in range(len(url_map)):
        url = url_map["url"][i]
        df = pd.read_csv(url)
        print(df.columns)
        print(df.dtypes)
        df["date"] = df["date"].astype(str)
        df["segment"] = df["segment"].astype(str)
        df = df[df["date"] > "1998-01-01"]
        print(len(df))
        folder = os.path.dirname(url)
        df = text_process(df)
        processed_url = os.path.join(folder, "processed_text.csv")
        df.to_csv(processed_url, index=False)
        processed_urls.append(processed_url)

    url_map["processed_url"] = processed_urls
    url_map.to_csv(os.path.join(cwd, "url_map.csv"), index=False)
