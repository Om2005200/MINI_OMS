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
            order_symbol=new_orders['SYMBOL']
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
                                if client==client_id:
                                    if status=='OPEN':

                                    
                                        if old_order_type!=order_type:

                                            if order_symbol==symbols:
                                                if master_quantity_==quantity:
                                                    exit_price=self.getting_the_live_prices(order_symbol)
                                                    old['STATUS']='CLOSED'
                                                    old['EXIT_TIME']=datetime.now().strftime("%H:%Y")
                                                    old['EXIT_PRICE']=exit_price

                                                elif master_quantity_!=quantity:
                                                    entry_price=self.getting_the_live_prices(order_symbol)
                                                    for new in master_data:
                                                        new_name=new['name']
                                                        strikeprice=new['strikeprice']
                                                        expiry=new['expiry']
                                                        lot_size=new['lot_size']
                                                        exch_seg=new['exch_seg']
                                                        new_order={
                                                            'CLIENT_ID':client_id,
                                                            'STOCK_NAME':new_name,
                                                            'TRADINGSYMBOL':order_symbol,
                                                            'STRIKEPRICE':strikeprice,
                                                            'EXPIRY':expiry,
                                                            'QUANTITY':quantity,
                                                            'EXIT_PRICE':'NA',
                                                            'ENTRY_PRICE':entry_price,
                                                            'EXIT_PRICE':'NA',
                                                            'ENTRY_TIME':datetime.now().strftime("%H:%Y"),
                                                            'EXIT_TIME':'NA',
                                                            'TOTAL_INVESTED_AMT':lot_size*quantity*entry_price,
                                                            'STATUS':'OPEN',
                                                            'ORDER_CATEGORY':'DELIVERY',
                                                            'TARGET_PRICE':target_price,
                                                            'STOP_LOSS':stop_loss,
                                                            'INSTRUMENT_TYPE':instrument_type,
                                                            'EXCHANGE_SEGMENT':exch_seg,
                                                            'ORDER_TYPE':'SELL'
                                                        }
                                            elif order_symbol!=symbols:
                                                new_entry_price=self.getting_the_live_prices(order_symbol)
                                                for newbies in master_data:
                                                    
                                                   
                                                    n_tradingsymbol=newbies['tradingsymbol']
                                                    if order_symbol==n_tradingsymbol:

                                                        n_strikeprice=newbies['strikeprice']
                                                        n_expiry=newbies['expiry']
                                                        n_name=newbies['name']
                                                        n_lot_size=newbies['lot_size']
                                                        n_exch_seg=newbies['exch_seg']
                                                        n_instrument_type=newbies['instrumenttype']
                                                        new_order={
                                                        'CLIENT_ID':client_id,
                                                        'STOCK_NAME':n_name,
                                                        'TRADINGSYMBOL':n_tradingsymbol,
                                                        'STRIKEPRICE':n_strikeprice,
                                                        'EXPIRY':n_expiry,
                                                        'QUANTITY':quantity,
                                                        'ENTRY_PRICE':self.getting_the_live_prices(n_tradingsymbol),
                                                        'EXIT_PRICE':'NA',
                                                        'ENTRY_TIME':datetime.now().strftime("%H:%Y"),
                                                        'EXIT_TIME':'NA',
                                                        'TOTAL_INVESTED_AMT':n_lot_size*quantity*new_entry_price,
                                                        'STATUS':'OPEN',
                                                        'ORDER_CATEGORY':'DELIVERY',
                                                        'TARGET_PRICE':target_price,
                                                        'STOP_LOSS':stop_loss,
                                                        'INSTRUMENT_TYPE':n_instrument_type,
                                                        'EXCHANGE_SEGMENT':n_exch_seg,
                                                        'ORDER_TYPE':'SELL'
                                                        }
                                        
                                    elif status=='CLOSED':
                                        for datas in master_data:
                                            new_tradingsymbols=datas['tradingsymbol']
                                            if symbols==new_tradingsymbols:
                                                new_strikeprice=datas['strikeprice']
                                                new_expiry=datas['expiry']
                                                new_exch_seg=datas['exch_seg']
                                                new_lot_size=datas['lot_size']
                                                new_instrument_type=datas['instrumenttype']
                                                new_entry_price_=self.getting_the_live_prices(symbols)
                                                new_name_=datas['name']
                                                new_orders={
                                                    'CLINET_ID':client_id,
                                                    'STOCK_NAME':new_name_,
                                                    'TRADINGSYMBOL':symbols,
                                                    'STRIKEPRICE':new_strikeprice,
                                                    'EXPIRY':new_expiry,
                                                    'QUANTITY':quantity,
                                                    'ENTRY_PRICE':new_entry_price_,
                                                    'EXIT_PRICE':'NA',
                                                    'TOTAL_INVESTED_AMT':new_entry_price_*new_lot_size*quantity,
                                                    'STATUS':'OPEN',
                                                    'ORDER_CATEGORY':'DELIVERY',
                                                    'TARGET_PRICE':target_price,
                                                    'STOP_LOSS':stop_loss,
                                                    'INSTRUMENT_TYPE':new_instrument_type,
                                                    'EXCHANGE_SEGMENT':new_exch_seg,
                                                    'ORDER_TYPE':'SELL'
                                                }
                                elif client_id!=client:
                                    for zerod in  master_data:
                                        zerod_tradingsymbol=zerod['tradingsymbol']
                                        zerod_strikeprice=zerod['strikeprice']
                                        zerod_expiry=zerod['expiry']
                                        zerod_exch_seg=zerod['exch_seg']
                                        zerod_lot_size=zerod['lot_size']
                                        zerod_instrumenttype=zerod['instrumenttype']
                                        zerod_name=zerod['name']
                                        n_entry_price=self.getting_the_live_prices(zerod_tradingsymbol)
                                        new_order={
                                            'CLIENT_ID':client_id,
                                            'STOCK_NAME':zerod_name,
                                            'TRADINGSYMBOL':symbols,
                                            'STRIKEPRICE':zerod_strikeprice,
                                            'EXPIRY':zerod_expiry,
                                            'QUANTITY':quantity,
                                            'ENTRY_PRICE':n_entry_price,
                                            'EXIT_PRICE':'NA',
                                            'TOTAL_INVESTED_AMT':zerod_lot_size*quantity*n_entry_price,
                                            'STATUS':'OPEN',
                                            'ORDER_CATEGORY':'DELIVERY',
                                            'TARGET_PRICE':target_price,
                                            'STOP_LOSS':stop_loss,
                                            'INSTRUMENT_TYPE':zerod_instrumenttype,
                                            'EXCHANGE_SEGMENT':zerod_exch_seg,
                                            'ORDER_TYPE':'SELL'
                                        }
                            elif instrument_type=='CE':
                                if client_id==client_id:
                                    if old_order_type==order_type:
                                        if order_symbol==symbols:
                                            if master_quantity_==quantity:
                                                exit_price=self.getting_the_live_prices(order_symbol)
                                                old['STATUS']='CLOSED'
                                                old['EXIT_TIME']=datetime.now().strftime("%H:%Y")
                                                old['EXIT_PRICE']=exit_price


                                        

                                            elif master_quantity_!=quantity:
                                                entry_price=self.getting_the_live_prices(symbols)
                                                for taat in master_data:
                                                    tta_tradingsymbol=taat['tradingsymbol']
                                                    if symbols==tta_tradingsymbol:
                                                        tt_strikeprice=taat['strikeprice']
                                                        tt_expiry=taat['expiry']
                                                        tt_lot_size=taat['lot_size']
                                                        tt_exch_seg=taat['exch_seg']
                                                        tt_name=taat['name']
                                                        tt_instrument_type=taat['instrumenttype']
                                                        tt_entry_price=self.getting_the_live_prices(symbols)
                                                        new_order={
                                                            'CLIENT_ID':client_id,
                                                            'STOCK_NAME':tt_name,
                                                            'TRADINGSYMBOL':symbols,
                                                            'STRIKEPRICE':tt_strikeprice,
                                                            'EXPIRY':tt_expiry,
                                                            'QUANTITY':quantity,
                                                            'ENTRY_PRICE':self.getting_the_live_prices(symbols),
                                                            'EXIT_PRICE':'NA',
                                                            'ENTRY_TIME':datetime.now().strftime("%H:%Y"),
                                                            'EXIT_TIME':'NA',
                                                            'TOTAL_INVESTED_AMT':quantity*tt_lot_size*tt_entry_price,
                                                            'STATUS':'OPEN',
                                                            'ORDER_CATEGORY':'DELIVERY',
                                                            'TARGET_PRICE':target_price,
                                                            'STOP_LOSS':stop_loss,
                                                            'INSTRUMENT_TYPE':tt_instrument_type,
                                                            'EXCHANGE_SEGMENT':tt_exch_seg,
                                                            'ORDER_TYPE':'SELL'
                                                        }
                                        elif order_symbol!=symbols:
                                            new_entry_price=self.getting_the_live_prices(order_symbol)
                                            for lt in master_data:
                                                lt_name=lt['name']
                                                lt_tradingsymbol=lt['tradingsymbol']
                                                if order_symbol==lt_tradingsymbol:
                                                    new_e_price=self.getting_the_live_prices(order_symbol)
                                                    lt_strikeprice=lt['strikeprice']
                                                    lt_expiry=lt['expiry']
                                                    lt_exch_seg=lt['exch_seg']
                                                    lt_lot_size=lt['lot_size']
                                                    lt_name=lt['name']
                                                    lt_instrument_type=lt['instrumenttype']
                                                    new_order={
                                                        'CLIENT_ID':client_id,
                                                        'STOCK_NAME':lt_name,
                                                        'TRADINGSYMBOL':order_symbol,
                                                        'STRIKEPRICE':lt_strikeprice,
                                                        'EXPIRY':lt_expiry,
                                                        'QUANTITY':quantity,
                                                        'ENTRY_PRICE':new_e_price,
                                                        'EXIT_PRICE':'NA',
                                                        'ENTRY_TIME':datetime.now().strftime("%H:%Y"),
                                                        'EXIT_TIME':'NA',
                                                        'TOTAL_INVESTED_AMT':lt_lot_size*new_e_price*quantity,
                                                        'STATUS':'OPEN',
                                                        'ORDER_CATEGORY':'DELIVERY',
                                                        'TARGET_PRICE':target_price,
                                                        'STOP_LOSS':stop_loss,
                                                        'INSTRUMENT_TYPE':lt_instrument_type,
                                                        'EXCHANGE_SEGMENT':lt_exch_seg,
                                                        'ORDER_TYPE':'SELL' 
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








                            



                    


