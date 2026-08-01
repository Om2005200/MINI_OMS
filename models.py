from pydantic import BaseModel
import uuid
from sqlmodel import create_engine,text,SQLModel,Field,Column
from typing import Optional,List


class ORDERPLACING(BaseModel):
   CLIENT_ID:str
   STOCK_NAME:str
   SYMBOL:str
   QUANTITY:str
   ENTRY_PRICE:str
   INSTRUMENT_TYPE:str
   POSITION_TYPE:str
   STOP_LOSS:Optional[str]
   TARGET_PRICE:Optional[str]
   FNO:bool=False
   

   

class USERVERIFY(BaseModel):
    EMAIL:str
    PASSWORD:str
    
class USERACCOUNT(BaseModel):
    CLIENT_ID:str
    NAME:str
    EMAIL_ID:str
    CONTACT_NO:int
    PASSWORD:str

class TOKENS(BaseModel):
    ACCESS_TOKENS:str
    JWT_KEY:str

# class ORDERPARAM(BaseModel):
#     ACCESS_TOKEN:str
#     REFRESH_TOKEN:str







