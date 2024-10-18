import os
import sys


database = os.path.abspath(os.path.join(os.path.dirname(__file__), "../..", "Database"))
print(database)


central_banks = ["fed", "ecb", "boe", "boj", "boa", "boc", "bosweden", "boswiss"]


fed_docs = [
    "fed_statements",
    "fed_minutes",
    "beigebooks",
    "fed_speeches",
]

ecb_docs = [
    "economic_bulletins",
    "monetary_policy_accounts",
    "press_conferences",
    "ecb_speeches",
]

boe_docs = [
    "boe_minutes",
    "boe_speeches",
]

boj_docs = [
    "boj_minutes",
    "boj_speeches",
]

boa_docs = [
    "boa_speeches",
]

boc_docs = [
    "boc_speeches",
]

bosweden_docs = [
    "bosweden_speeches",
]

boswiss_docs = [
    "boswiss_speeches",
]
