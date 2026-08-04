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
from schemas import USERDATABASE,ORDER_DATABASE
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
    await s.pre_processing_the_helpers()
    

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
        
    def getting_the_live_prices(self,trade_symbol:str):
        pass

    async def pre_processing_the_helpers(self):
        loop_scanner=asyncio.get_running_loop()
        with ProcessPoolExecutor() as executor:
            task1=loop_scanner.run_in_executor(executor,self.getting_the_master_scripts_data)
            final_master=await task1
            return final_master
        
           


    



    

    
    async def placing_the_orders(self,orders:list[dict],session:AsyncSession):
        master_orders=orders
        print(master_orders)
        placed_orders=[]
        master_data_book=await self.pre_processing_the_helpers()
        for datas in master_orders:
            master_stock_name=datas['STOCK_NAME']
            master_client_id=datas['CLIENT_ID']
            master_symbol=datas['SYMBOL']
            master_quantity=datas['QUANTITY']
            master_instrumentype=datas['INSTRUMENT_TYPE']
            master_position_type=datas['POSITION_TYPE']
            stop_loss=datas['STOP_LOSS']
            order_type=datas['ORDER_TYPE']
            master_quantity=datas['QUANTITY']
            target_price=datas['TARGET_PRICE']
            order_category=datas['ORDER_CATEGORY']
            if order_type=='SELL':
                for new_book in master_data_book:
                    original_symbol=new_book['symbol']
                    if master_symbol==original_symbol:
                        original_strikeprice=new_book['strike']
                        original_expiry=new_book['expiry']
                        original_lot_size=new_book['lotsize']
                        original_instrumentype=new_book['instrumenttype']
                        original_exch_seg=new_book['exch_seg']
                        original_name=new_book['name']
                        #original_exchangetoken=new_book['exchangetoken']
                        #original_price=self.getting_the_live_prices(master_symbol)
                        original_price='NA'
                        
                        order_placing={
                            'CLIENT_ID':master_client_id,
                            'TRADINGSYMBOL':master_symbol,
                            'STRIKEPRICE':original_strikeprice,
                            'EXPIRY':original_expiry,
                            'QUANTITY':original_lot_size,
                            'EXIT_PRICE':'NA',
                            'ENTRY_PRICE':original_price,
                            'ENTRY_TIME':datetime.now().strftime('%H:%Y'),
                            'EXIT_TIME':'NA',
                            'TOTAL_INVESTED_AMT':original_lot_size,
                            'STATUS':'OPEN',
                            'ORDER_CATEGORY':order_category,
                            'TARGET_PRICE':target_price,
                            'STOP_LOSS':stop_loss,
                            'STOCK_NAME':original_name,
                            'INSTRUMENT_TYPE':original_instrumentype,
                            'EXCHANGE_SEGMENT':original_exch_seg,
                        

                        }
                        placed_orders.append(order_placing)

                       
                    
            elif order_type=='BUY':
                for new_buy in master_data_book:
                    new_buy_symbol=new_buy['symbol']
                    if master_symbol==new_buy_symbol:
                        buy_strikeprice=new_buy['strike']
                        buy_instrumenttype=new_buy['instrumenttype']
                        buy_expiry=new_buy['expiry']
                        buy_quantity=master_quantity
                        #buy_entry_price=self.getting_the_live_prices(new_buy_symbol)
                        buy_entry_price='NA'
                        
                        buy_lot_size=new_buy['lotsize']
                        buy_name=new_buy['name']
                        exchangesegment=new_buy['exch_seg']

                        order_buy={
                            'CLIENT_ID':master_client_id,
                            'TRADINGSYMBOL':master_symbol,
                            'STRIKEPRICE':buy_strikeprice,
                            'INSTRUMENT_TYPE':buy_instrumenttype,
                            'EXPIRY':buy_expiry,
                            'QUANTITY':buy_quantity,
                            'ENTRY_PRICE':buy_entry_price,
                            'EXIT_PRICE':'NA',
                            'ENTRY_TIME':datetime.now().strftime("%H:%Y"),
                            'EXIT_TIME':'NA',
                            'TOTAL_INVESTED_AMT':buy_lot_size,
                            'STATUS':'OPEN',
                            'STOCK_NAME':buy_name,
                            'EXCHANGE_SEGMENT':exchangesegment,
                            'ORDER_CATEGORY':order_category,
                            'TARGET_PRICE':target_price,
                            'STOP_LOSS':stop_loss
                        }
                        placed_orders.append(order_buy)
        print("Placed Orders:", placed_orders)
        for orde in placed_orders:
            db_order=ORDER_DATABASE(**orde)
            session.add(db_order)
        await session.commit()

        return placed_orders
    
    



                    




                    




        



s=SENSE()
@router.post('/order/placing/')

async def placing_the_router_orders(order_model:List[ORDERPLACING],session:AsyncSession=Depends(get_session)):
    orders=[order.model_dump() for order in order_model]
    order_place=await s.placing_the_orders(orders,session)

    if order_place is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail='ORDER_NOT PLACED')
    return JSONResponse(content={
        'mesaage':'ORDER_PLACED_SUCCESFULLY',
        
    })
mini_sensibull.include_router(router)








                            



                    


