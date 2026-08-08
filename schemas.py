from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select,desc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine
import sqlalchemy.dialects.postgresql as pg
from sqlmodel import create_engine,text,SQLModel,Field,Column
import uuid
from sqlalchemy.dialects.postgresql import JSON
from typing import List,Optional



class USERDATABASE(SQLModel,table=True):
    __tablename__='SENSIBULL_ACCOUNT'
    id:Optional[int]=Field(default=None,primary_key=True)
    CLIENT_ID:str
    NAME:str
    EMAIL_ID:str
    CONTACT_NO:str
    PASSWORD:str
    
class ORDER_DATABASE(SQLModel,table=True):
    __tablename__='ORDER_DATABASE'
    id:Optional[int]=Field(default=None,primary_key=True)
    CLIENT_ID:str
    STOCK_NAME:str
    TRADINGSYMBOL:str
    STRIKEPRICE:str
    EXPIRY:str
    QUANTITY:str
    EXIT_PRICE:str
    ENTRY_PRICE:str
    EXIT_PRICE:str
    ENTRY_TIME:str
    EXIT_TIME:str
    TOTAL_INVESTED_AMT:str
    STATUS:str
    ORDER_CATEGORY:str='DELIVERY'
    TARGET_PRICE:str
    STOP_LOSS:str
    INSTRUMENT_TYPE:str
    EXCHANGE_SEGMENT:str
    ORDER_TYPE:str



class INTRADAY_ORDERS(SQLModel,table=True):
    __tablename__='INTRADAY_ORDERS'
    id:Optional[int]=Field(default=None,primary_key=True)
    TRADINGSYMBOL:str
    STOCK_NAME:str
    STRIKEPRICE:str
    EXPIRY:str
    QUANTITY:str
    INSTRUMENTYPE:str
    POSITION_TYPE:str
    ENTRY_PRICE:str
    EXIT_PRICE:str
    ENTRY_TIME:str
    EXIT_TIME:str
    CLIENT_ID:str
    ORDER_ID:str
    STOP_LOSS:str
    TARGET_PRICE:str
    ORDER_CATEGORY:str








