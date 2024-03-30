import os
import sys


database = os.path.abspath(os.path.join(os.path.dirname(__file__), "../..", "Database"))
print(database)
central_banks = ["fed", "ecb"]

fed_docs = [
    "statements",
    "minutes",
    "beigebooks",
    "speeches",
]

ecb_docs = [
    "economic bulletins",
    "monetary policy accounts",
    "press conferences",
    "speeches",
]
