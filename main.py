"""

"""
import sys
import tiktoken

from rag.chains import rag_chain_with_source
from rag.retriever import retriever, vectostore


try:
    query_index = sys.argv.index("--query") + 1
    query = sys.argv[query_index]
except ValueError:
    raise ValueError("Please provide a prompt using the --query flag")

if __name__ == "__main__":
    answer = rag_chain_with_source.invoke({"prompt": query})["answer"]

    print(answer)
