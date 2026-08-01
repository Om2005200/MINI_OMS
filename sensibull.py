import requests as rq
import pandas as pd
import csv 
import json
from datetime import datetime,timedelta
import time
from sqlalchemy import text

from  fastapi import FastAPI,APIRouter,HTTPException,status,Depends,BackgroundTasks,Request
from typing import List,Annotated
from sqlmodel import select,desc
from fastapi.responses import JSONResponse
from sqlmodel import SQLModel
from models import ORDERPLACING,USERVERIFY,USERACCOUNT,TOKENS
from concurrent.futures import ProcessPoolExecutor
from sqlmodel import SQLModel
from sqlalchemy.ext.asyncio import create_async_engine,AsyncSession
from schemas import USERDATABASE,OVERNIGHT_ORDERS
from passlib.context import CryptContext
from fastapi.security import OAuth2PasswordRequestForm,OAuth2PasswordBearer,oauth2
import random
from sqlalchemy.orm import sessionmaker
import jwt
from redis.asyncio import Redis 

import websockets 

import asyncio
PORT=6379
database_url='postgresql+asyncpg://postgres:Samnokia123%40@localhost:5432/MINI_SENSIBULL'
jwt_key='c932c7cad4cf33dd43ca01162474b4bce1ca32a76472ac7fb5de486b81f48cd1'
jwt_algorithm='HS256'
mini_sensibull=FastAPI()
router=APIRouter()
engine=create_async_engine(database_url,echo=True)



@mini_sensibull.on_event('startup')
async def startup():
    await init_db()
    await SENSE.getting_the_master_scripts_data()
    

async def init_db():
    async with engine.begin() as connection:
        await connection.run_sync(SQLModel.metadata.create_all)
async def get_session():
    Session=sessionmaker(bind=engine,class_=AsyncSession,expire_on_commit=False)
    async with Session() as session:
        yield session

@mini_sensibull.on_event('shutdown')
async def shutdown_evenet():
    await mini_sensibull.state.redis.close()
    await engine.dispose()

class SENSE:
    """THIS APPLICATION IS SOLELY DEVELOPED  AS A REPLICA OF SENSIBULL"""
    def getting_the_master_scripts_data(self):
        api_endpoint='https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
        headers = {
        'Authorization': 'Bearer AUTHORIZATION_TOKEN',
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'X-UserType': 'USER',
        'X-SourceID': 'WEB',
        'X-ClientLocalIP': 'CLIENT_LOCAL_IP',
        'X-ClientPublicIP': 'CLIENT_PUBLIC_IP',
        'X-MACAddress': 'MAC_ADDRESS',
        'X-PrivateKey': 'API_KEY'
        }
        request=rq.get(api_endpoint,headers=headers)
        main_data=request.json()
        with open(r"C:\Users\dasho\angelone_srip_master_for_mini_sensibull.json",'w') as x:
            json.dump(main_data,x,indent=4)
        return main_data
        
    def getting_the_live_prices(self):
        pass

    async def pre_processing_the_helpers(self):
        loop_scanner=asyncio.get_running_loop()
        with ProcessPoolExecutor() as executor:
            task1=loop_scanner.run_in_executor(executor,self.getting_the_master_scripts_data)
           
           
            await task1


    



    




    async def order_processing(self,orders:dict,session:AsyncSession):
        master_data=await self.getting_the_master_scripts_data()        
        main_orders=orders
        for order in main_orders:
            stock_name=order['STOCK_NAME']
            symbol=order['SYMBOL']
            quantity=order['QUANTITY']
            entry_price=orders['ENTRY_PRICE']
            exit_price=orders['EXIT_PRICE']
            instrumentype=orders['INSTRUMENT_TYPE']
            position_type=orders['POSITION_TYPE']
            expiry=orders['EXPIRY']
            stop_loss=orders['STOP_LOSS']
            target_price=orders['TARGET_PRICE']
            order_category=orders['ORDER_CATEGORY']
            client_id=orders['CLIENT_ID']
            #exchnagetoken=orders['EXCHANGETOKEN']
            
            if order_category=='DELIVERY':
                if position_type=='SELL':
                    for datas in master_data:
                        master_symbol=datas['tradingsymbol']
                        if master_symbol==symbol:
                            original_strikeprice=datas['strikeprice']
                            original_expiry=datas['expiry']
                            original_instrumenttype=datas['instrumenttype']
                            original_lotsize=datas['lotsize']
                            original_name=datas['name']
                            original_strike=datas['strikeprice']
                            original_entry_price=self.getting_the_live_prices(master_symbol)
                            exchangetoken=datas['exchangetoken']
                            order_placing={
                                'STOCK_NAME':original_name,
                                'STRIKEPRICE':original_strikeprice,
                                'EXPIRY':original_expiry,
                                'QUANTITY':original_lotsize*quantity,
                                'INSTRUMENTTYPE':original_instrumenttype,
                                'POSITION_TYPE':position_type,
                                'ENTRY_PRICE':self.getting_the_live_prices(master_symbol),
                                'EXIT_PRICE':'NA',
                                'ENTRY_TIME':datetime.now().strftime("%H:%Y"),
                                'EXIT_TIME':'NA',
                                'TOTAL_INVESTED_AMT':original_entry_price*original_lotsize*quantity,
                                'CLIENT_ID':client_id,
                                'STATUS':'OPEN',
                                'EXCHANGETOKEN':exchangetoken,
                                'ORDER_CATEGORY':'DELIVERY',
                                'TARGET_PRICE':target_price,
                                'STOP_LOSS':stop_loss,
                                
                            }
                            session.add(order_placing)
                            await session.commit()
                            await session.refresh(order_placing)
                elif position_type=='BUY':
                    for buy_datas in master_data:
                        buy_symbol=buy_datas['symbol']
                        if buy_symbol==symbol:
                            b_original_strikeprice=buy_datas['strikeprice']
                            b_original_expiry=buy_datas['expiry']
                            b_original_lot_size=buy_datas['lot_size']
                            b_original_instrumenttype=datas['instrumenttype']
                            b_original_exchangetoken=datas['exhcangetoken']
                            b_entry_price=self.getting_the_live_prices(buy_symbol)
                            b_original_name=datas['name']
                            order_placing={
                                'STOCK_NAME':b_original_name,
                                'STRIKEPRICE':b_original_strikeprice,
                                'EXPIRY':b_original_expiry,
                                'QUANTITY':b_original_lot_size*quantity,
                                'INSTRUMENTTYPE':b_original_instrumenttype,
                                'POSITION_TYPE':position_type,
                                'ENTRY_PRICE':self.getting_the_live_prices(buy_symbol),
                                'EXIT_PRICE':'NA',
                                'ENTRY_TIME':datetime.now().strftime("%H:%Y"),
                                'EXIT_TIME':'NA',
                                'TOTAL_INVESTED_AMT':b_entry_price*b_original_lot_size*quantity,
                                'CLIENT_ID':client_id,
                                'STATUS':'OPEN',
                                'EXCHANGETOKEN':b_original_exchangetoken,
                                'ORDER_CATEGORY':order_category,
                                'TARGET_PRICE':target_price,
                                'STOP_LOSS':stop_loss
                            }
                            session.add(order_placing)
                            await session.commit()
                            await session.refresh(order_placing)


                            



                    


