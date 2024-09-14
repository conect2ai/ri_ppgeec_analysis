"""

"""
import os
import glob

def get_latest_csv(directory):
    csv_files = glob.glob(os.path.join(directory, "*.csv"))
    
    if not csv_files:
        raise FileNotFoundError(f"Files not found in {directory}")

    latest_file = max(csv_files, key=os.path.getctime)
    
    return latest_file

def format_docs(docs):
    return "\n\n".join(f"{doc.page_content}\n\nMetadata: {doc.metadata}" for doc in docs)