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
   ORDER_TYPE:str
   ORDER_CATEGORY:str
   

   

class USERVERIFY(BaseModel):
    EMAIL:str
    PASSWORD:str
    
class USERACCOUNT(BaseModel):
    CLIENT_ID:str
    NAME:str
    EMAIL_ID:str
    CONTACT_NO:str
    PASSWORD:str

class TOKENS(BaseModel):
    ACCESS_TOKENS:str
    REFRESH_TOKENS:str


class DATASET(BaseModel):
    STOCK_NAME:str
    EXCHNAGE_SEGMENT:str
    ISIN_VALUE:str
# class ORDERPARAM(BaseModel):
#     ACCESS_TOKEN:str
#     REFRESH_TOKEN:str







