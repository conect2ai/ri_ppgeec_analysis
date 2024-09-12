"""
File that contains utility functions for the data acquisition process.
"""
import re
from datetime import datetime


def clean_text(text):
    text = re.sub(r"\s+", " ", text, flags=re.IGNORECASE).strip()
    text = text.replace("- ", "-").replace("\ufeff", "")

    return text

def convert_date(date_str):
    formats = [
        "%d-%b-%Y",  # Formato: 21-May-2024
        "%d-%B-%Y",  # Formato: 21-May-2024 (com nome completo do mês)
        "%b-%Y",     # Formato: Feb-2003
        "%B-%Y",     # Formato: February-2003 (com nome completo do mês)
    ]

    for fmt in formats:
        try:
            if fmt in ["%b-%Y", "%B-%Y"]:
                return datetime.strptime(f"01-{date_str}", f"%d-{fmt}").strftime("%Y-%m-%d")
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue  # Continua tentando com o próximo formato

    return None
