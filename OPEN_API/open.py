# E:\miniconda3\Scripts\activate e:\miniconda3
#
# conda activate h20gpt
#


from langchain.llms import OpenAI
#from langchain.chat_models import ChatOpenAI
# from langchain.schema import (
#     #AIMessage,
#     HumanMessage,
#     SystemMessage
# )
# from langchain.chains.api.prompt import API_RESPONSE_PROMPT
# from langchain.chains import APIChain
# from langchain.prompts.prompt import PromptTemplate



#llm = OpenAI(temperature=0)
# from langchain.chains.api import open_meteo_docs
# chain_new = APIChain.from_llm_and_api_docs(llm, open_meteo_docs.OPEN_METEO_DOCS, verbose=True)
# chain_new.run('What is the weather like right now in Munich, Germany in degrees Fahrenheit?')

#chat = ChatOpenAI(temperature=0)
#x = chat.predict_messages([HumanMessage(content="Translate this sentence from English to French. I love programming.")])
#print(x)


llm = OpenAI(
    openai_api_key="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", 
    temperature=0,
    model_name="gpt-3.5-turbo-16k")

llm.predict("What would be a good company name for a company that makes colorful socks?")
#print(llm.predict("What would be a good company name for a company that makes colorful socks?"))