import os
import sys


database = os.path.abspath(os.path.join(os.path.dirname(__file__), "../..", "Database"))
print(database)


central_banks = ["fed", "ecb", "boe"]


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
