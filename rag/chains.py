"""
"""
from langchain.prompts import ChatPromptTemplate
from rag.retriever import retriever
from rag.utils import format_docs
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough, RunnableParallel


llm = ChatOpenAI(model="gpt-4o-mini")
prompt = ChatPromptTemplate.from_messages(
    [
        (
            'system',
            """
            Sua função é ajudar os usuários de um progrmama de pós-graduação a encontrar informações sobre trabalhos acadêmicos.
            O programa em questão é o de pos-graduação em engenharia elétrica e de computação da UFRN.
            
            VOCÊ NÃO DEVE RESPONDER PERGUNTAS FORA DO CONTEXTO DE TRABALHOS ACADÊMICOS.

            Retorne os trabalhos no formato:
            Título: Titulo do trabalho (Ano de defesa)
            Autor: Nome do autor (Mestrado ou Doutorado)
            Orientador: Nome do orientador
            Resumo: Resumo do trabalho
            
            Contexto: {context} 
            Pergunta: {question}
            """
        ),
    ]
)

rag_chain = (
    RunnablePassthrough.assign(context=(lambda x: format_docs(x["context"])))
    | prompt
    | llm
    | StrOutputParser()
)

rag_chain_with_source = RunnableParallel(
    {
        "context": retriever,
        "question": RunnablePassthrough()
    }
).assign(answer=rag_chain)