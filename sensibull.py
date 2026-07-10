import requests as rq
import pandas as pd
import csv 

import json
from datetime import datetime,timedelta
import time
import asyncio
from  fastapi import FastAPI,APIRouter,HTTPException,status,Depends,BackgroundTasks,Request
from typing import List
from sqlmodel import select,desc
from fastapi.responses import JSONResponse
from sqlmodel import SQLModel
from models import ORDERPLACING,USERVERIFY,USERACCOUNT,TOKENS
from concurrent.futures import ProcessPoolExecutor
from sqlmodel import SQLModel
from sqlalchemy.ext.asyncio import create_async_engine,AsyncSession
from schemas import USERDATABASE,OVERNINGHT_ORDERS
from passlib.context import CryptContext
from fastapi.security import OAuth2PasswordRequestForm,OAuth2PasswordBearer
import random
from sqlalchemy.orm import sessionmaker
import jwt
from redis.asyncio import Redis 


database_url='postgresql+asyncpg://postgres:Samnokia123%40@localhost:5432/MINI_SENSIBULL'
jwt_key='c932c7cad4cf33dd43ca01162474b4bce1ca32a76472ac7fb5de486b81f48cd1'
jwt_algorithm='HS256'

sensibull=FastAPI()
router=APIRouter()
engine=create_async_engine(database_url,echo=True)
bcrypt_context=CryptContext(schemes=['bcrypt'],deprecated='auto')
oauth_passowrd=OAuth2PasswordBearer

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


            if action=='SELL':
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
                        return await sensibull.state.redis.set('BUYING')
                    
                       
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
                                'ORDER_ID':master_order_id
                                
                            }
                            latest_order=await sensibull.state.redis.set('sell_order',json.dumps(new_order))
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
                                'ORDER_ID':master_order_id
                            }
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
                                'EXCHANGETOKEN':banknifty_exchangetoken
                            }
                            return pre_order
            elif action=='BUY':
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
                                'EXCHANGETOKEN':sell_exchange_token
                            }               
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
                                'EXCHANAGE_TOKEN':sensex_sell_exchange_token
                            }
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
                                'EXCHANGE_TOKEN':banknifty_master_exchange_token
                            }
                            return banknifty_sell_order_
                        



    async def processing_the_overnight_orders(self,session:AsyncSession):
        nifty_main_data=await sensibull.state.redis.get('NIFTY')
        nifty_data_load=json.loads(nifty_main_data)
        getting_the_data=select(OVERNINGHT_ORDERS)
        result=await session.execute(getting_the_data)

        all_data=result.scalars().all()
        current_datetime=datetime.now().strftime('%H:%Y')
        for datas in all_data:
            client_id=datas.CLIENT_ID
            strikeprice=datas.STRIKEPRICE
            expiry=datas.EXPIRY
            tradingsymbol=datas.TRADINGSYMBOL
            stock_name=datas.STOCK_NAME
            entry_price=datas.ENTRY_PRICE
            exit_price=datas.EXIT_PRICE
            quantity=datas.QUANTITY
            status=datas.STATUS
            positiontype=datas.POSITION_TYPE
            exchangetoken=datas.EXCHANGETOKEN
            if 


        
        







                        

                               






                            


                        


                

   
    
    
            
s=MINI_SENSIBULL()
class SENSIBULL_HELPER:
    async def create_new_user(self,user_data:USERACCOUNT,session:AsyncSession):
        create_new_user=USERDATABASE(NAME=user_data.NAME,CONTACT_NO=user_data.CONTACT_NO,EMAIL_ID=user_data.EMAIL_ID,PASSWORD=bcrypt_context.hash(user_data.PASSWORD))
        if self.create_new_user is None:
            return False
        session.add(create_new_user)

        await session.commit()
        await session.refresh(create_new_user)
        await sensibull.state.redis.set('client_id_set',json.dumps(user_data.CONTACT_NO),ex=1000)

        return create_new_user
    

    

    async def verify_user(self,user_data:USERACCOUNT,session:AsyncSession):
        verify_user=select(USERDATABASE).where(USERDATABASE.EMAIL_ID==user_data.EMAIL_ID,USERDATABASE.PASSWORD==bcrypt_context.verify(user_data.PASSWORD))
        execution=await session.execute(verify_user)
        result=execution.first()
        return result
    
    async def creating_the_api_key(self,user_data:USERACCOUNT,expires_delta:timedelta,session:AsyncSession):
        encode={'sub':user_data.EMAIL_ID}
        expires=datetime.utcnow()+expires_delta
        encode.update({'exp':expires})
        return jwt.encode(encode,jwt_key,jwt_algorithm)
    

    async def creating_the_access_token(self,user_data:USERACCOUNT,time:timedelta,session:AsyncSession=Depends(get_session)):
        encode={'sub':user_data.NAME,'id':user_data.EMAIL_ID}
        expires=datetime.utcnow()+time
        encode.update({'exp':expires})
        return jwt.encode(encode,jwt_key,jwt_algorithm)

            

helper=SENSIBULL_HELPER()

@router.post('/create/user/account/')
async def create_user_account(user_data:USERACCOUNT,session:AsyncSession=Depends(get_session)):
    verify_user=await helper.verify_user(user_data,session)
    if verify_user:
        raise HTTPException(status_code=status.HTTP_406_NOT_ACCEPTABLE,detail='USER WITH THESE CREDENTIALS ALREADY EXISTS PLEASE LOGIN FOR MORE DETAILS ')
    user_account=await helper.create_new_user(user_data,session)
    if user_account is None:
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,detail='PLEASE ENTER VALID DETAILS TO CREATE THE ACCOUNT')
    
    return JSONResponse(content={
        'INFO':'ACCOUNT SUCCESFULLY CREATED PLEASE DO LOGIN FOR A NEW START'
    })


@router.post('/user/login/')
async def user_login(user_data:USERACCOUNT,session:AsyncSession=Depends(get_session)):
    user_verify=await helper.verify_user(user_data,session)
    if user_verify is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,detail='PLEASE LOGIN TO GET THE ACCESS TOKENS ')
    access_tokens=await helper.creating_the_access_token(user_data,session)
    if access_tokens is None:
        raise HTTPException(status_code=status.HTTP_204_NO_CONTENT,detail='UNABLE TO GENERATE THE ACCESS TOKENS ')
    client_dataset={
        'CLIENT_ID':user_data.CLIENT_ID,
        'JWT_KEY':access_tokens
    }
    redis_set=await sensibull.state.redis.set('client',json.dumps(client_dataset),ex=4800)
    return JSONResponse(conetent={
        'ACCESS_TOKEN':access_tokens
    })
    


@router.post('/place/order')
async def placing_the_new_orders(orderplace:list[ORDERPLACING],session:AsyncSession=Depends(get_session)):
    verify_client=await sensibull.state.redis.get('CLIENT')
    client_load=json.loads(verify_client)

    if client_load is not None:
        orders=[order.model_dump () for order in orderplace]
        client_orders=await s.placing_the_orders(orders,session)
        if client_orders is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail='NOT ABLE TO PLACE THE ORDERS PLEASE TRY AGAIN AFTER SOMETIME')
        return client_orders,JSONResponse(content={
            'STATUS':'ORDER PLACED SUCCESFULLY',
            'EXCECUTION_TIME':datetime.now().strftime('%H:%Y')
        })
    
    else:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,detail='PLEASE LOGIN TO PLACE THE ORDEERS')




    








   
                
            

            
sensibull.include_router(router)                         