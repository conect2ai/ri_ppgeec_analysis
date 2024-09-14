"""

"""
import sys

from rag.chains import rag_chain_with_source


try:
    query_index = sys.argv.index("--query") + 1
    query = sys.argv[query_index]
except ValueError:
    raise ValueError("Please provide a prompt using the --query flag")

if __name__ == "__main__":
    print(rag_chain_with_source.invoke({"prompt": query})["answer"])