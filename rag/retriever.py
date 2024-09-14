"""

"""
import os

import pandas as pd

from rag.utils import get_latest_csv
from dotenv import load_dotenv
from langchain_chroma import Chroma
from langchain_core.documents import Document
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain.chains.query_constructor.base import AttributeInfo
from langchain.retrievers.self_query.base import SelfQueryRetriever


load_dotenv()

filename = get_latest_csv("./data")
data = pd.read_csv(filename)
data["defense_date"] = pd.to_datetime(data["defense_date"])
documents = list()

for idx, row in data.iterrows():
    page_content = f"""TÍTULO: {row['title']}\n\nRESUMO [PORTUGUÊS]: {row['pt_abstract']}\n\nRESUMO [INGLÊS]: {row['en_abstract']}\n\nPALAVRAS-CHAVE: {row['auth_keywords']}\n\nODS: {row['sdg_predictions_filtered']}"""

    document = Document(
        id=idx,
        page_content=page_content,
        metadata={
            "category": row["category"],
            "title": row["title"],
            "author": row["author"],
            "advisor": row["advisor"],
            "defense_date_year": row["defense_date"].year,
        }
    )

    documents.append(document)

if not os.path.exists("./data/chroma_index"):
    vectostore = Chroma.from_documents(
        documents=documents,
        embedding=OpenAIEmbeddings(model=os.environ.get("OPENAI_EMBEDDING_MODEL")),
        persist_directory="./data/chroma_index"
    )
else:
    vectostore = Chroma(
        embedding_function=OpenAIEmbeddings(model=os.environ.get("OPENAI_EMBEDDING_MODEL")),
        persist_directory="./data/chroma_index"
    )

metadata_field_info = [
    AttributeInfo(
        name="category",
        description="A categoria do trabalho se refere ao nível que pode ser doutorado ou mestrado. Deve ser buscado como um valor entre ['PhD', 'MSc']",
        type="string",
    ),
    AttributeInfo(
        name="author",
        description="O nome do autor do trabalho",
        type="string",
    ),
    AttributeInfo(
        name="advisor",
        description="O nome do orientador do trabalho",
        type="string",
    ),
    AttributeInfo(
        name="defense_date_year",
        description="O ano da defesa do trabalho",
        type="integer",
    )
]

document_content_description = "Textos contendo o título, resumo em português, resumo em inglês e palavras-chave dos trabalhos acadêmicos"
llm = ChatOpenAI(
    model="gpt-4o-mini",
    temperature=0
)
retriever = SelfQueryRetriever.from_llm(
    llm=llm,
    vectorstore=vectostore,
    document_contents=document_content_description,
    metadata_field_info=metadata_field_info,
    enable_limit=True,
)