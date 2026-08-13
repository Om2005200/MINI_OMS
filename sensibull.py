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
    while True:
        s.managing_the_orders(main_orders=USERDATABASE)
    

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
    def reading_the_master_file(self):
        with open(r"C:\Users\dasho\angelone_srip_master_for_mini_sensibull.json",'r') as d:
            data=json.load(d)
            return data
    async def processing_the_files(self):
        loop=asyncio.get_running_loop()
        with ProcessPoolExecutor() as runner:
            task=loop.run_in_executor(runner,self.reading_the_master_file)
            final_task=await task
            return final_task
           
           


    
    async def placing_the_orders(self,orders:list[dict],session:AsyncSession):
        master_orders=orders
        old_orders=select(ORDER_DATABASE)
        execution=await session.execute(old_orders)
        response=execution.scalars().all()
        master_data=self.reading_the_master_file()

        
        for new_orders in master_orders:
            client_id=new_orders['CLIENT_ID']
            stock_name=new_orders['STOCK_NAME']
            symbol=new_orders['SYMBOL']
            quantity=new_orders['QUANTITY']
            entry_price=new_orders['ENTRY_PRICE']
            exit_price=new_orders['EXIT_PRICE']
            instrument_type=new_orders['INSTRUMENT_TYPE']
            position_type=new_orders['POSITION_TYPE']
            stop_loss=new_orders['STOP_LOSS']
            target_price=new_orders['TARGET_PRICE']
            order_type=new_orders['ORDER_TYPE']
            order_category=new_orders['ORDER_CATEGORY']
            fno=new_orders['FNO']



            for old in response:
                client=old.CLIENT_ID
                status=old.STATUS
                symbols=old.TRADINGSYMBOL
                name=old.STOCK_NAME
                instrument=old.INSTRUMENT_TYPE
                old_order_type=old.ORDER_TYPE
                strike=old.STRIKEPRICE
                expiry=old.EXPIRY
                entry_price=old.ENTRY_PRICE
                entry_time=old.ENTRY_TIME
                master_quantity_=old.QUANTITY
                total_invested_value=old.TOTAL_INVESTED_AMT



                if fno==True:

                    if order_category=='DELIVERY':
                        if order_type=='SELL':
                            if instrument_type=='PE':
                            
                                if client_id==client_id:
                                    old_client_status=old['STATUS']
                                    if old_client_status=='OPEN':
                                        if symbol==symbols:
                                            if master_quantity_==quantity:


                                                if instrument_type==instrument:
                                                    if old_order_type!=order_type:
                                                        current_ltp=self.getting_the_live_prices(symbol)


                                                        old['STATUS']='CLOSED'
                                                        old['EXIT_TIME']=datetime.now().strftime("%H:%Y")
                                                        old['EXIT_PRICE']=current_ltp
                                                        old['ORDER_TYPE']='BUY'




                                                        new_order=old



                                    elif status=='CLOSED':
                                        for new in master_data:
                                            new_symbol=new['tradingsymbol']
                                            if symbol==new_symbol:
                                                nw_tradingsymbol=new['tradingsymbol']
                                                nw_strikeprice=new['strikeprice']
                                                nw_expiry=new['expiry']
                                                nw_instrument_type=new['instrumenttype']
                                                nw_name=new['name']
                                                nw_segment=new['exch_seg']
                                                lot_size=new['lotsize']
                                                exchangetoken=new['exchangetoken']
                                                current_entry_price=self.getting_the_live_prices(symbol)



                                                new_order={
                                                    'CLIENT_ID':client_id,
                                                    'STOCK_NAME':nw_name,
                                                    'TRADINGSYMBOL':nw_tradingsymbol,
                                                    'STRIKEPRICE':nw_strikeprice,
                                                    'EXPIRY':nw_expiry,
                                                    'QUANTIY':quantity,
                                                    'ENTRY_PRICE':current_entry_price,
                                                    'EXIT_PRICE':'NA',
                                                    'ENTRY_TIME':datetime.now().strftime("%H:%Y"),
                                                    'EXIT_TIME':'NA',
                                                    'TOTAL_INVESETED_AMT':lot_size*quantity*current_entry_price,
                                                    'STATUS':'OPEN',
                                                    'ORDER_CATEGORY':'DELIVERY',
                                                    'TARGET_PRICE':target_price,
                                                    'STOP_LOSS':stop_loss,
                                                    'INSTRUMENT_TYPE':instrument_type,
                                                    'EXCHANGE_TOKEN':exchangetoken,
                                                    'ORDER_TYPE':'SELL'
                                                }
                                if client!=client_id:


                                    for new_ in master_data:
                                        new_trading_symbol=new_['tradingsymbol']
                                        if symbol==new_trading_symbol:
                                            new_strikeprice_=new_['strikeprice']
                                            new_expiry_=new_['expiry']
                                            new_instrumentype=new_['instrumenttype']
                                            exchangetoken_=new_['exchangetoken']
                                            new_segment=new_['exch_seg']
                                            name=new_['name']
                                            lot_sizing=new_['lot_size']
                                            current_new_price=self.getting_the_live_prices(symbol)

                                            new_order={
                                                'CLIENT_ID':client_id,
                                                'STOCK_NAME':name,
                                                'TRADINGSYMBOL':new_trading_symbol,
                                                'STRIKEPRICE':new_strikeprice_,
                                                'EXPIRY':new_expiry_,
                                                'QUANTITY':quantity,
                                                'ENTRY_PRICE':current_new_price,
                                                'EXIT_PRICE':'NA',
                                                'ENTRY_TIME':datetime.now().strftime("%H:%Y"),
                                                'EXIT_TIME':'NA',
                                                'TOTAL_INVESTED_AMT':quantity*current_new_price*lot_sizing,
                                                'STATUS':'OPEN',
                                                'ORDER_CATEGORY':'DELIVERY',
                                                'TARGET_PRICE':target_price,
                                                'STOP_LOSS':stop_loss,
                                                'INSTRUMENT_TYPE':new_instrumentype,
                                                'EXCHANGETOKEN':exchangetoken,
                                                'ORDER_TYPE':'SELL'
                                            }
                            elif instrument_type=='CE':
                                if client_id==client:
                                    if status=='CLOSED':
                                        for maker in master_data:
                                            make_tradingsymbol=maker['tradingsymbol']
                                            if make_tradingsymbol==symbol:
                                                maker_instrumentype=maker['instrumenttype']
                                                maker_expiry=maker['expiry']
                                                maker_lot_size=maker['lot_size']
                                                maker_exchnagetoken=maker['exchangetoken']
                                                maker_segment=maker['exch_seg']
                                                maker_name=maker['name']
                                                maker_strikeprice=maker['strikeprice']
                                                enter_price=self.getting_the_live_prices(make_tradingsymbol)



                                                new_orders={
                                                    'CLIENT_ID':client_id,
                                                    'STOCK_NAME':maker_name,
                                                    'TRADINGSYMBOL':make_tradingsymbol,
                                                    'STRIKEPRICE':maker_strikeprice,
                                                    'EXPIRY':maker_expiry,
                                                    'QUANTITY':quantity,
                                                    'ENTRY_PRICE':enter_price,
                                                    'EXIT_PRICE':'NA',
                                                    'ENTRY_TIME':datetime.now().strftime("%H:%Y"),
                                                    'EXIT_TIME':'NA',
                                                    'TOTAL_INVESETED_AMT':maker_lot_size*enter_price*quantity,
                                                    'STATUS':'OPEN',
                                                    'ORDER_CATEGORY':'DELIVERY',
                                                    'TARGET_PRICE':target_price,
                                                    'STOP_LOSS':stop_loss,
                                                    'INSTRUMENT_TYPE':maker_instrumentype,
                                                    'EXCHANGE_SEGMENT':maker_segment,
                                                    'ORDER_TYPE':'SELL'
                                                }
                                    elif status=='OPEN':
                                        if symbol==symbols:
                                            if quantity==master_quantity_:
                                                if order_type!=old_order_type:
                                                    symb=symbol
                                                    current_pr=self.getting_the_live_prices(symb)
                                                    orders['STATUS']='CLOSED'
                                                    orders['EXIT_PRICE']=current_pr
                                                    orders['EXIT_TIME']=datetime.now().strftime("%H:%Y")
                                                    new_orders=orders


 



                                                elif order_type==old_order_type:
                                                    for latest in  master_data:
                                                        latest_symbol=latest['tradingsymbol']
                                                        if symbol==latest_symbol:
                                                            latest_strikeprice=latest['strikeprice']
                                                            latest_expiry=latest['expiry']
                                                            latest_instrumenttype=latest['instrumenttype']
                                                            latest_lot_size=latest['lot_size']
                                                            latest_exch_seg=latest['exch_seg']
                                                            latest_name=latest['name']
                                                            latest_price=self.getting_the_live_prices(latest_symbol)





                                                            new_order={
                                                                'CLIENT_ID':client_id,
                                                                'STOCK_NAME':latest_name,
                                                                'TRADINGSYMBOL':latest_symbol,
                                                                'STRIKEPRICE':latest_strikeprice,
                                                                'EXPIRY':latest_expiry,
                                                                'QUANTITY':quantity,
                                                                'ENTRY_PRICE':latest_price,
                                                                'EXIT_PRICE':'NA',
                                                                'ENTRY_TIME':datetime.now().strftime("%H:%Y"),
                                                                'EXIT_TIME':'NA',
                                                                'TOTAL_INVESTED_AMT':quantity*latest_lot_size*latest_price,
                                                                'STATUS':"OPEN",
                                                                'ORDER_CATEGORY':'DELIVERY',
                                                                'TARGET_PRICE':target_price,
                                                                'STOP_LOSS':stop_loss,
                                                                'INSTRUMENT_TYPE':latest_instrumenttype,
                                                                'EXCHANGE_SEGMENT':latest_exch_seg,
                                                                'ORDER_TYPE':'SELL'
                                                            }
                                            elif master_quantity_==quantity:
                                                for lat in master_data:
                                                    lat_tradingsymbol=lat['tradingsymbol']
                                                    if symbol==lat_tradingsymbol:
                                                        lat_strikeprice=lat['strikeprice']
                                                        lat_expiry=lat['expiry']
                                                        lat_lot_size=lat['lot_size']
                                                        lat_exhc_seg=lat['exch_seg']
                                                        lat_instrumenttype=lat['instrumenttype']
                                                        lat_name=lat['name']




                                                        new_order={
                                                            ''
                                                        }

                                            





                                                        





    







                                                    


                                                



                                                                                                                




        
            




    







    async def managing_the_orders(self,session:AsyncSession):
        main_orders=select(ORDER_DATABASE)
        execution=await session.execute(main_orders)
        response=execution.scalars().all()
        master_data=self.processing_the_files()
        current_date=datetime.now().strftime("%H:%Y")
        current_time=datetime.now()
        org_orders=response
        for orders in org_orders:
            client_id=orders.CLIENT_ID
            strikeprice=orders.STRIKEPRICE
            tradingsymbol=orders.TRADINGSYMBOL
            quantity=orders.QUANTITY
            entry_price=orders.ENTRY_PRICE
            order_category=orders.ORDER_CATEGORY
            stop_loss=orders.STOP_LOSS
            target_price=orders.TARGET_PRICE
            order_type=orders.ORDER_TYPE
            instrument_type=orders.INSTRUMENT_TYPE




            







                                        
                                                    










    





                    




                    




        



s=SENSE()

class HELPERS:
    async def verify_the_user(self,user_model:USERACCOUNT,session:AsyncSession):
        user_creation=select(USERDATABASE).where(USERDATABASE.CONTACT_NO==user_model.CONTACT_NO,USERDATABASE.EMAIL_ID==user_model.EMAIL_ID)
        execution=await session.execute(user_creation)
        response=execution.first()
        if response is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,detail='PLEASE CREATE AN ACCOUNT TO ACCESS')


    async def creating_the_access_tokens(self,user_model:USERACCOUNT):
        encode={'sub':user_model.CLIENT_ID,'email_id':user_model.EMAIL_ID}
        expires=datetime.utcnow()+timedelta(hours=2)
        encode.update({'exp':expires})
        return jwt.encode(encode,jwt_key,algorithm=jwt_algorithm)



    async def creating_the_refresh_tokens(self,user_model:USERACCOUNT):
        encode={'sub':user_model.EMAIL_ID,'id':user_model.NAME}
        expires=datetime.utcnow()+timedelta(days=1)
        encode.update({'exp':expires})
        return jwt.encode(encode,jwt_key,algorithm=jwt_algorithm)


    async def user_exists(self,user_model:USERACCOUNT,session:AsyncSession):
        check_user=select(USERDATABASE).where(USERDATABASE.EMAIL_ID==user_model.EMAIL_ID,USERDATABASE.PASSWORD==user_model.PASSWORD)
        execution=await session.execute(check_user)
        response=execution.first()
        return response


    async def decoding_the_access_tokens(self,user_model:TOKENS,session:AsyncSession):
        payload=jwt.decode(jwt_key,algorithms=[jwt_algorithm])
        client_id=payload.get('sub')
        email_id=payload.get('id')
        main_checker=select(USERDATABASE).where(USERDATABASE.CLIENT_ID==client_id,USERDATABASE.EMAIL_ID==email_id)
        execution=await session.execute(main_checker)
        response=execution.first()
        
        if response is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,detail='PLEASE ENTER VALID TOKENS TO ACCESS THE DATAS')

        

    async def decoding_the_refresh_tokens(self,user_input:TOKENS,session:AsyncSession):
        payload=jwt.decode(jwt_key,algorithms=[jwt_algorithm])
        email_id=payload.get('sub')
        name=payload.get('id')
        media_checker=select(USERDATABASE).where(USERDATABASE.EMAIL_ID==email_id,USERDATABASE.NAME==name)
        execution=await session.execute(media_checker)
        response=execution.first()

        if response is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,detail='PLEASE ENTER VALID DETAILS TO ACCESS THE DATAS')


        


h=HELPERS()


@router.post('/create/account/')
async def create_account(user_model:USERACCOUNT,session:AsyncSession=Depends(get_session)):
    user_exists=await h.user_exists(user_model,session)
    if user_exists is not  None:
        return JSONResponse(content={
            'message':'USER EXISTS PLEASE LOGIN'
        })
    else:
        new_user=USERDATABASE(CLIENT_ID=user_model.CLIENT_ID,NAME=user_model.NAME,EMAIL_ID=user_model.EMAIL_ID,PASSWORD=user_model.PASSWORD,CONTACT_NO=user_model.CONTACT_NO)
        access_tokens=await h.creating_the_access_tokens(user_model)
        refresh_tokens= await h.creating_the_refresh_tokens(user_model)

        session.add(new_user)
        await session.commit()
        await session.refresh(new_user)
        return JSONResponse({
            'STATUS':'ACCOUNT CREATED SUCCESFULLY',
            'REFRESH_TOKENS':refresh_tokens,
            'ACCESS_TOKENS':access_tokens
        })


@router.get('/create/access_tokens/')
async def creating_the_access_tokens(user_model:USERACCOUNT,session:AsyncSession=Depends(get_session)):
    user_verify=await h.user_exists(user_model,session)
    if user_verify is not None:
        access_tokens=await h.creating_the_access_tokens(user_model)
        if access_tokens is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail='UNABLE TO CREATE THE ACCESS TOKENS DUE TO HEAVY TRAFFIC PLESE RETRY AFTER SOMETIMES ')
        return JSONResponse({
            'STATUS':"ACCESS_TOKENS_CREATED",
            'ACCESS_TOKENS':access_tokens
        })





    



    





    







@router.post('/order/placing/')

async def placing_the_router_orders(order_model:List[ORDERPLACING],user_model:TOKENS,session:AsyncSession=Depends(get_session)):
    user_verify=await h.decoding_the_refresh_tokens(user_model.REFRESH_TOKENS)
    if user_verify is  not None:
        orders=[order.model_dump() for order in order_model]
        order_place=await s.placing_the_orders(orders,session)

        if order_place is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail='ORDER_NOT_PLACED')
        return JSONResponse(content={
            'mesaage':'ORDER_PLACED_SUCCESFULLY',
            
        })














    
mini_sensibull.include_router(router)








                            



                    


