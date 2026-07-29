from pydantic import BaseModel
import uuid
from sqlmodel import create_engine,text,SQLModel,Field,Column
from typing import Optional,List


class ORDERPLACING(BaseModel):
   STOCK_NAME:str
   SYMBOL:str
   QUANTITY:str
   ENTRY_PRICE:str
   EXIT_PRICE:Optional[str]
   INSTRUMENT_TYPE:str
   POSITION_TYPE:str
   EXPIRY:str
   CLIENT_ID:str
   ORDER_ID:str
   ENTRY_TIME:Optional[int]
   EXIT_TIME:Optional[int]
   STOP_LOSS:str
   ORDER_CATEGORY:str

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







