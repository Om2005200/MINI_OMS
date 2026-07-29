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

sensibull=FastAPI()
router=APIRouter()
engine=create_async_engine(database_url,echo=True)
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
@sensibull.on_event("startup")
async def startup():
    await init_db()
bcrypt_context=CryptContext(schemes=['bcrypt'],deprecated='auto')
oauth_passowrd=OAuth2PasswordBearer
oauth2_scheme=OAuth2PasswordBearer(tokenUrl='/login')
#oauth2_scheme_api_endpoint=OAuth2PasswordBearer(tokenUrl='/api')
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
async def get_session():
    Session=sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False

    )
    async with Session() as session:
        yield session

SessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

class MINI_SENSIBULL:
    def __init__(self):
        self.access_token=''
        self.api_key=''
    def opening_the_credentials_data(self):
        pass


    # stretgy builder
    async def getting_the_live_prices(self):
        pass


    def getting_the_nifty_data(self):
    
        api_endpoint=''
        credentials_file=self.opening_the_credentials_data()
        access_token=credentials_file['access_token']

        headers = {
            "Authorization":access_token ,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-UserType": "USER",
            "X-SourceID": "WEB",
            "X-ClientLocalIP": "117.199.220.240",
            "X-ClientPublicIP": "117.199.220.204",
            "X-MACAddress": "58-CD-C9-37-C7-65",
            "X-PrivateKey": self.api_key,
        }
        payload = {"name": "NIFTY", "expirydate": "02JUN2026"}
        request=rq.post(api_endpoint,headers=headers,json=payload)

        print(request.status_code)
        print(request.text)

        return 
    
    def getting_the_banknifty_data(self):
        pass


    def getting_the_sensex_data(self):

        pass

    async def opening_the_banknifty_data(self):
        with open(r"C:\Users\dasho\banknifty_revised_options_chain.json",'r') as x:
            data=json.load(x)
            return data

    async def opening_the_nifty_data(self):
        with open(r"C:\Users\dasho\nifty_revised_options_chain.json",'r') as c:
            data=json.load(c)
            return data
    async def opening_the_sensex_data(self):
        with open(r"C:\Users\dasho\sensex_revised_options_chain.json",'r') as v:
            data=json.load(v)
            return data 
        
    

    async def running_the_synchronous_objects(self):
        loop=asyncio.get_running_loop() 
        with ProcessPoolExecutor as executor:
            task1=loop.run_in_executor(executor,self.getting_the_banknifty_data)
            task2=loop.run_in_executor(executor,self.getting_the_sensex_data)
            task3=loop.run_in_executor(executor,self.getting_the_nifty_data)
            banknifty=await task1
            sensex=await task2
            nifty_data=await task3
            return {
                'NIFTY_DATA':nifty_data,
                'SENSEX_DATA':sensex,
                'BANKNIFTY_DATA':banknifty
            }
    async def pre_processing_the_datas(self):
        nifty_dataset=self.running_the_synchronous_objects('NIFTY_DATA')
        sensex_data=self.running_the_synchronous_objects('SENSEX_DATA')
        banknifty_data=self.running_the_synchronous_objects('BANKNIFTY_DATA')
        redis_nifty=await sensibull.state.redis.set('NIFTY',json.dumps(nifty_dataset),ex=480)
        redis_sensex=await sensibull.state.redis.set('SENSEX',json.dumps(sensex_data),ex=480)
        redis_banknifty=await sensibull.state.redis.set('BANKNIFTY',json.dumps(banknifty_data),ex=480)
        return redis_banknifty,redis_nifty,redis_sensex
    

    
      

     

    async def placing_the_orders(self,orders:list[dict]):
        nifty_set=await sensibull.state.redis.get('NIFTY')
        nifty_data_loading=json.load(nifty_set)
        banknifty_set=await sensibull.state.redis.get('BANKNIFTY')
        banknifty_load=json.load(banknifty_set)
        sensex_set=await sensibull.state.redis.get('SENSEX')
        sensex_data_load=json.load(sensex_set)
        buy_master_orders=await sensibull.state.redis.get('BUYING')
        buy_master_load=json.load(buy_master_orders)
        sell_master_orders=await sensibull.state.redis.get('SELLING')
        for datas in orders:
            stock_name=datas['STOCK_NAME']
            tradingsymbol=datas['TRADINGSYMBOL']
            client_id=datas['CLIENT_ID']
            action=datas['POSITION_TYPE']
            master_quantity=datas['QUANTITY']
            master_client_id=datas['CLIENT_ID']
            master_order_id=datas['ORDER_ID']
            stop_loss=datas['STOP_LOSS']
            order_category=datas['ORDER_CATEGORY']
        
            


            if action=='SELL':
                if order_category=='DELIVERY':
                    for data in buy_master_load:
                        buy_client_id=data['CLIENT_ID']
                        buy_tradingsymbol=data['TRADING_SYMBOL']
                        order_status=data['STATUS']
                        if buy_client_id==client_id and buy_tradingsymbol==tradingsymbol and order_status=='OPEN':
                            master_exchangetoken=data['EXCHNAGE_TOKEN']
                            exit_price=self.getting_the_live_prices(master_exchangetoken)
                            data['STATUS']='CLOSED'
                            data['EXIT_TIME']=datetime.now().strftime('%H:%Y')
                            data['EXIT_PRICE']=exit_price
                            
                            await sensibull.state.redis.set('BUYING',json.dumps(data))
                            updated_order=await sensibull.state.redis.set('DELIVERY_ORDERS',json.dumps(data))
                            return updated_order


                            
                    
                        
                    if stock_name=='NIFTY':
                        nifty_buy_data=nifty_data_loading('data')
                        for nifty in nifty_buy_data:
                            nifty_tradingsymbol=nifty['tradingsymbol']
                            if nifty_tradingsymbol==tradingsymbol:
                                strikeprice=nifty['strikeprice']
                                exchange_token=nifty['exchangetoken']
                                expiry=nifty['expiry']
                                segment=nifty['name']
                                instrument_type=nifty['instrumentype']
                                quantity=master_quantity,
                                nifty_lot_size='65'

                                entry_price=self.getting_the_live_prices(exchange_token)
                                new_order={
                                    'STOCK_NAME':segment,
                                    'SYMBOL':nifty_tradingsymbol,
                                    'STRIKEPRICE':strikeprice,
                                    'EXPIRY':expiry,
                                    'ENTRY_PRICE':entry_price,
                                    'QUANTITY':nifty_lot_size*quantity,
                                    'EXIT_PRICE':'NA',
                                    'ENTRY_TIME':datetime.now().strftime('%H:%Y'),
                                    'EXIT_TIME':'NA',
                                    'INSTRUMENT_TYPE':nifty_lot_size*quantity,
                                    'POSITION_TYPE':action,
                                    'CLIENT_ID':master_client_id,
                                    'ORDER_ID':master_order_id,
                                    'ORDER_TYPE':'',
                                    'ORDER_CATEGORY':order_category,
                                    'STOP_LOSS':stop_loss
                                    
                                }
                                latest_order=await sensibull.state.redis.set('sell_order',json.dumps(new_order))
                                await sensibull.state.redis.set('DELIVERY_ORDERS',json.dumps(new_order))
                                return latest_order
                            
                            else:
                                return None
                    elif stock_name=='SENSEX':
                        sensex_data_loading=sensex_data_load
                        for sensex in sensex_data_loading:
                            sensex_master_tradingsymbol=sensex['tradingsymbol']
                            if tradingsymbol==sensex_master_tradingsymbol:
                                sensex_strikeprice=sensex['strikeprice']
                                sensex_expiry=sensex['expiry']
                                sensex_exchangetoken=sensex['exchangetoken']
                                sensex_segment=sensex['name']
                                sensex_ltp=self.getting_the_live_prices(sensex_exchangetoken)
                                sensex_lot_size=20
                                sensex_instrumentype=sensex['instrumentype']
                                new_order={
                                    'STOCK_NAME':sensex_segment,
                                    'SYMBOL':sensex_master_tradingsymbol,
                                    'STRIKEPRICE':sensex_strikeprice,
                                    'EXPIRY':sensex_expiry,
                                    'ENTRY_PRICE':sensex_ltp,
                                    'QUANTITY':quantity*sensex_lot_size,
                                    'EXIT_PRICE':'NA',
                                    'ENTRY_TIME':datetime.now().strftime('%H:%Y'),
                                    'EXIT_TIME':'NA',
                                    'INSTRUMENTYPE':sensex_instrumentype,
                                    'POSITION_TYPE':action,
                                    'CLIENT_ID':client_id,
                                    'ORDER_ID':master_order_id,
                                    'ORDER_TYPE':'',
                                    'ORDER_CATEGORY':'DELIVERY',
                                    'STOP_LOSS':stop_loss
                                }
                                await sensibull.state.redis.set('DELIVERY_ORDERS',json.dumps(new_order))
                                await sensibull.state.redis.set('sell_orders',json.dumps(new_order))
                                return new_order
                    elif stock_name=='BANKNIFTY':
                        master_banknifty=banknifty_load('data')
                        for banknifty in master_banknifty:
                            banknifty_tradingsymbol=banknifty['tradingsymbol']
                            if banknifty_tradingsymbol==tradingsymbol:
                                banknifty_strikeprice=banknifty['strikeprice']
                                banknifty_expiry=banknifty['expiry']
                                banknifty_exchangetoken=banknifty['exchangetoken']
                                banknifty_ltp=self.getting_the_live_prices(banknifty_exchangetoken)
                                banknifty_instrumentype=banknifty['instrumentype']
                                banknifty_lot_size=35
                                banknifty_segment=banknifty['segment']
                                pre_order={
                                    'STOCK_NAME':banknifty_segment,
                                    'SYMBOL':banknifty_tradingsymbol,
                                    'STRIKEPRICE':banknifty_strikeprice,
                                    'EXPIRY':banknifty_expiry,
                                    'ENTRY_PRICE':banknifty_ltp,
                                    'QUANTITY':quantity*banknifty_lot_size,
                                    'EXIT_PRICE':'NA',
                                    'ENTRY_TIME':datetime.now().strftime('%H:%Y'),
                                    'EXIT_TIME':'NA',
                                    'INSTRUMENTYPE':banknifty_instrumentype,
                                    'POSITION_TYPE':action,
                                    'CLIENT_ID':client_id,
                                    'ORDER_ID':master_order_id,
                                    'EXCHANGETOKEN':banknifty_exchangetoken,
                                    'ORDER_TYPE':'',
                                    'ORDER_CATEGORY':order_category,
                                    'STOP_LOSS':stop_loss
                                    
                                }
                                await sensibull.state.redis.set('DELIVERY',json.dumps(pre_order))
                                await sensibull.state.redis.set('sell_order',json.dumps(pre_order))

                                return pre_order
                            
            elif action=='BUY':
                if order_category=='DELIVERY':
                    sell_master_data_loading=json.load(sell_master_orders)
                    for sells in sell_master_data_loading:
                        sells_client_id=sells['CLIENT_ID']
                        sells_order_status=sells['STATUS']
                        sells_tradingsymbol=sells['TRADINGSYMBOL']
                        if sells_client_id==client_id and sells_order_status=='OPEN' and sells_tradingsymbol==tradingsymbol:
                            sells_exchange_token=sells['EXCHANGETOKEN']
                            sells_exit_price=self.getting_the_live_prices(sells_exchange_token)
                            sells_exit_time=datetime.now().strftime('/%H:%Y')
                            sells['POSITION_TYPE']='BUY'
                            sells['EXIT_TIME']=sells_exit_time
                            sells['EXIT_PRICE']=sells_exit_price
                            sells['STATUS']='CLOSED'
                            updated_orders=await sensibull.state.redis.set('BUYING',json.dumps(sells))
                            await sensibull.state.redis.set('DELIVERY',json.dumps(sells))

                            return updated_orders
                    if stock_name=='NIFTY':
                        nifty_buy_data_loading=nifty_data_loading
                        for sell_side in nifty_buy_data_loading:
                            sell_side_tradingsymbol=sell_side['tradingsymbol']
                            if sell_side_tradingsymbol==tradingsymbol:
                                sell_strikeprice=sell_side['strikeprice']
                                sell_expiry=sell_side['expiry']
                                sell_exchange_token=sell_side['exchangetoken']
                                sell_entry_time=datetime.now().strftime('%H:%Y')
                                #sell_exit_time='NA'
                                sell_entry_price=self.getting_the_live_prices(sell_exchange_token)
                                sell_side_instrumentype=sell_side['instrumentype']     
                                sell_nifty_lot_size=65
                                new_sell_order={
                                    'SYMBOL':sell_side_tradingsymbol,
                                    'STRIKEPRICE':sell_strikeprice,
                                    'EXPIRY':sell_expiry,
                                    'ENTRY_PRICE':sell_entry_price,
                                    'QUANTITY':quantity*sell_nifty_lot_size,
                                    'EXIT_PRICE':'NA',
                                    'ENTRY_TIME':sell_entry_time,
                                    'EXIT_TIME':'NA',
                                    'INSTRUMENTYPE':sell_side_instrumentype,
                                    'POSITION_TYPE':action,
                                    'CLIENT_ID':client_id,
                                    'ORDER_ID':master_order_id,
                                    'EXCHANGETOKEN':sell_exchange_token,
                                    'ORDER_TYPE':'',
                                    'ORDER_CATEGORY':order_category,
                                    'STOP_LOSS':stop_loss
                                }        
                                await sensibull.state.redis.set('buy_order',json.dumps(new_sell_order))
                                await sensibull.state.redis.set('buy_order',json.dumps(new_sell_order))       
                                return new_sell_order
                            else:
                                return None
                            
                    elif stock_name=='SENSEX':
                        sensex_sell_data=sensex_data_load('data')
                        for sensex_sell in sensex_sell_data:
                            sensex_sell_tradingsymbol=sensex_sell['tradingsymbol']
                            if sensex_sell_tradingsymbol==tradingsymbol:
                                sensex_sell_strikeprice=sensex_sell['strikeprice']
                                sensex_sell_expiry=sensex_sell['expiry']
                                sensex_sell_instrumentype=sensex_sell['instrumentype']
                                sensex_sell_exchange_token=sensex_sell['exchangetoken']
                                sensex_sell_ltp=self.getting_the_live_prices(sensex_sell_exchange_token)
                                sensex_sell_lot_size=20
                                new_sensex_sell_order={
                                    'SYMBOL':sensex_sell_tradingsymbol,
                                    'STRIKEPRICE':sensex_sell_strikeprice,
                                    'EXPIRY':sensex_sell_expiry,
                                    'ENTRY_PRICE':sensex_sell_ltp,
                                    'QUANTITY':quantity*sensex_sell_lot_size,
                                    'EXIT_PRICE':'NA',
                                    'ENTRY_TIME':datetime.now().strftime('%H:%Y'),
                                    'EXIT_TIME':'NA',
                                    'INSTRUMENT_TYPE':sensex_sell_instrumentype,
                                    'POSITION_TYPE':'SELL',
                                    'CLIENT_ID':client_id,
                                    'ORDER_ID':master_client_id,
                                    'EXCHANAGE_TOKEN':sensex_sell_exchange_token,
                                    'ORDER_TYPE':''
                                }
                                await sensibull.state.redis.set('DELIVERY_ORDERS',json.dumps(new_sensex_sell_order))
                                await sensibull.state.redise.set('buy_order',json.dumps(new_sensex_sell_order))

                                return new_sensex_sell_order
                            else:
                                return None 
                    elif stock_name=='BANKNIFTY':
                        banknifty_data_loading=banknifty_load('data')
                        for bdl in banknifty_data_loading:
                            banknifty_master_tradingsymbol=bdl['tradingsymbol']
                            if banknifty_master_tradingsymbol==tradingsymbol:
                                banknifty_master_strikeprice=bdl['strikeprice']
                                banknifty_master_expiry=bdl['expiry']
                                banknifty_master_exchange_token=bdl['exchnagetoken']
                                banknifty_master_ltp=self.getting_the_live_prices(banknifty_master_exchange_token)
                                banknifty_master_lot_size=35
                                banknifty_master_instrumentype=bdl['instrumentype']
                                banknifty_sell_order_={
                                    'SYMBOL':banknifty_master_tradingsymbol,
                                    'STRIKEPRICE':banknifty_master_strikeprice,
                                    'EXPIRY':banknifty_master_expiry,
                                    'ENTRY_PRICE':banknifty_master_ltp,
                                    'QUANTITY':quantity*banknifty_master_lot_size,
                                    'EXIT_PRICE':'NA',
                                    'INSTRUMENTYPE':banknifty_master_instrumentype,
                                    'POSITION_TYPE':'SELL',
                                    'CLIENT_ID':master_client_id,
                                    'ORDER_ID':master_order_id,
                                    'EXCHANGE_TOKEN':banknifty_master_exchange_token,
                                    'ORDER_TYPE':'',
                                    'ORDER_CATEGORY':order_category,
                                    'STOP_LOSS':stop_loss
                                }
                                await sensibull.state.redise.set('DELIVERY',json.dumps(banknifty_sell_order_))
                                await sensibull.state.redis.set('buy_order',json.dumps(banknifty_sell_order_))

                                return banknifty_sell_order_
    


    async def managing_the_orders(self,session:AsyncSession):
        order_book=select(OVERNIGHT_ORDERS)
        process=await session.execute(order_book)
        
        main_data=process.scalars().all()
        for datas in main_data:
            client_id=datas.CLIENT_ID
            entry_price=datas.ENTRY_PRICE
            exit_price=datas.EXIT_PRICE
            exchange_token=datas.EXCHANGETOKEN
            order_status=datas.STATUS
            order_category=datas.ORDER_CATEGORY
            instrument_type=datas.INSTRUMENTYPE
            stop_loss=datas.STOP_LOSS
            target_price=datas.TARGET_PRICE
            expiry=datas.EXPIRY
            position_type=datas.POSITION_TYPE
            current_date=datetime.now()
            current_time=datetime.now().strftime('%H:%Y')
            if order_status=='OPEN':
                if order_category=='DELIVERY':
                    if position_type=='SELL':
                        if instrument_type=='CE':
                            if stop_loss is not None:
                                if target_price is not None:
                                    if expiry is not None:
                                        master_exchangetoken=datas.EXCHANGETOKEN
                                        expi=datas.EXPIRY
                                        target=datas.TARGET_PRICE
                                        

            



    async def serializing_the_index_sockets(self,websocket):
        nifty_dataset=await sensibull.state.redis.get('NIFTY')
        nifty_loading=json.loads(nifty_dataset)
        banknifty_dataset=await sensibull.state.redis.get('BANKNIFTY')
        banknifty_loading=json.loads(banknifty_dataset)
        sensex_dataset=await sensibull.state.redis.get('SENSEX')
        sensex_loading=json.loads(sensex_dataset)
        current_time=datetime.now().strftime('%H:%Y')
        timestamp=[]
        async for messages in websocket:
            
            if messages=='NIFTY':
                await websocket.send(nifty_loading)
            elif messages=='BANKNIFTY':
                await websocket.send(banknifty_loading)
            elif messages=='SENSEX':
                await websocket.send(sensex_loading)


            try:
                last_time=timestamp[-1]
                if current_time>last_time+20:
                    server_check=await websocket.send('PING_SERVER')
                    if server_check is None:
                        await websocket.close()
                    else:
                        timestamp.append(current_time)




            except Exception as e:
                print('Continuing the sokcet ')
                return
            
    
    async def running_the_newer_events(self):
        task1=await self.managing_the_orders()
        task3=await self.serializing_the_index_sockets()

        result1=asyncio.create_task(task1)
        #result2=asyncio.create_task(task2)
        result3=asyncio.create_task(task3)


        await result1
        #await result2
        await result3


    

    












            
            




                        















                            


                            


                            

                            
                            



                    



        
        







                        

                               






                            


                        


                

   
    
    
            
s=MINI_SENSIBULL()

@router.post('/place/order/')
async def placing_the_new_orders(orderplace:list[ORDERPLACING],verify_access=Depends(helper.verifying_the_refresh_tokens),verify_refresh=Depends(helper.verifying_the_refresh_tokens),session:AsyncSession=Depends(get_session)):
    
    if verify_access is not None:
        if verify_refresh is not None:
            client_set=await sensibull.state.redis.get('client_id_set')
            client_set_load=json.loads(client_set)



            if client_set_load is not None:
                orders=[order.model_dump () for order in orderplace]
                client_orders=await s.placing_the_orders(orders,session)
                if client_orders is None:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail='NOT ABLE TO PLACE THE ORDERS PLEASE TRY AGAIN AFTER SOMETIME')
                return client_orders,JSONResponse(content={
                    'STATUS':'ORDER PLACED SUCCESFULLY',
                    'EXCECUTION_TIME':datetime.now().strftime('%H:%Y')
                })
            
            else:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,detail='PLEASE LOGIN TO PLACE THE ORDERS')





    








   
                
            

            
sensibull.include_router(router)                         