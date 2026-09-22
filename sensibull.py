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
from models import ORDERPLACING,USERVERIFY,USERACCOUNT,TOKENS,DATASET
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
import redis
import websockets 
from redis.asyncio import Redis
import asyncio
import httpx
PORT=6379
database_url='postgresql+asyncpg://postgres:Samnokia123%40@localhost:5432/MINI_SENSIBULL'
jwt_key='c932c7cad4cf33dd43ca01162474b4bce1ca32a76472ac7fb5de486b81f48cd1'
jwt_algorithm='HS256'
mini_sensibull=FastAPI()


@mini_sensibull.get("/")
async def home():
    return {
        "message": "MINI SENSIBULL API IS RUNNING"
    }
router=APIRouter()
engine=create_async_engine(database_url,echo=True)
http_client = httpx.AsyncClient()

pwd_context=CryptContext(schemes=['bcrypt'],depriciated='auto')
oauth_2=OAuth2PasswordBearer(token_url='/login')



@mini_sensibull.on_event('startup')
async def startup():
    await init_db()
    mini_sensibull.state.redis=Redis(host='localhost',port=6379,decode_responses=True)

    mini_sensibull.state.http_client=httpx.AsyncClient()

    await s.pre_processing_the_helpers()
    # while True:
    #     s.processing_the_orders()
    

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
    await mini_sensibull.state.http_client.aclose()
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


        for old_data in response:
            client_sets=old_data['CLIENT_ID']

        for basket in master_orders:
            client_id=basket['CLIENT_ID']
            stock_name=basket['STOCK_NAME']
            symbol=basket['SYMBOL']
            quantity=basket['QUANTITY']
            entry_price=basket['ENTRY_PRICE']
            exit_price=basket['EXIT_PRICE']
            instrumenttype=basket['INSTRUMENT_TYPE']
            position_type=basket['POSITION_TYPE']
            stop_loss=basket['STOP_LOSS']
            target_price=basket['TARGET_PRICE']
            fno=basket['FNO']
            order_type=basket['ORDER_TYPE']
            order_category=basket['ORDER_CATEGORY']



            if order_category=='DELIVERY':
                if order_type=='SELL':
                    for datas in master_data:
                        name=datas['name']
                        tradingsymbol=datas['tradingsymbol']
                        if symbol==tradingsymbol:

                            new_order=json.dumps(datas)




                        else:
                            return 

                        


                elif order_type=='BUY':
                    for gen_data in master_data:
                        gen_symbol=gen_data['tradingsymbol']
                        if gen_symbol==symbol:
                            new_order=json.dumps(datas)




            elif order_category=='OPTIONS':
                if order_type=='SELL':
                    for new in master_data:
                        new_symbol=new['tradingsymbol']
                        if new_symbol==symbol:
                            new_order=json.dumps(datas)



                elif order_type=='BUY':
                    for latest in master_data:
                        new_symbols=latest['tradingsymbol']
                        if new_symbols==symbol:
                            new_order=json.dumps(datas)


        session.add(new_order)

        await session.commit()
        await session.refresh(new_order)


    


    






            



        






                        
    def ip_file(self):
        try:
            with open(r'C:\Users\dasho\ip_files','r') as x:
                data=json.load(x)
                if data is None:
                    data=[]
                return data


        except FileNotFoundError:
            with open(r'C:\Users\dasho\ip_files','w') as k:
                json.dump([],k,indent=4)


            return []


        
                                    
                



                                        

                                                


            






                












    

    async def getting_the_required_day_data(exchange_segment:str,isin_value:str,stock_name:str):
        
        api='https://api.upstox.com/v3/market-quote/ohlc'
        headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': ''
        }
        data={
            'instrument_key':exchange_segment|isin_value
        }
        response= await http_client.get(url=api,headers=headers,params=data)
        print(response.status_code)
        main_data=response.json()
        redis_load=await mini_sensibull.redis.set(stock_name,main_data,ex=3600)
        return redis_load

    


    
        

s=SENSE()

class HELPERS:
    async def creating_the_new_user(self,db_model:USERACCOUNT,session:AsyncSession):
        create_user=USERDATABASE(NAME=db_model.NAME,PASSWORD=pwd_context.hash(db_model.PASSWORD))
        session.add(create_user)
        await session.commit()
        await session.refresh(create_user)



    async def verify_the_user(self,user_input:USERACCOUNT,session:AsyncSession):
        validate_user=select(USERDATABASE).where(USERDATABASE.NAME==user_input.NAME)
        execution=await session.execute(validate_user)
        response=execution.first()
        if response is  not None:

            password_get=pwd_context.verify(user_input.PASSWORD,response['PASSWORD'])
            if password_get is True:




              

                return True




    async def getting_the_access_tokens(self,form_data:OAuth2PasswordRequestForm,session:AsyncSession):
        user_data=await self.verify_the_user(form_data,session)
        if user_data is not None:
            encode={'sub':form_data.username}
            expires=datetime.utcnow()+timedelta(hours=24)
            encode.update({'exp':expires})
            return jwt.encode(encode,jwt_key,algorithm=jwt_algorithm)



    

            
            

        





            





    async def decoding_the_access_tokens(self,user_model:TOKENS,session:AsyncSession):
        payload=jwt.decode(jwt_key,algorithms=[jwt_algorithm])
        client_id=payload.get('sub')
        #contact_no=payload.get('id')
        main_checker=select(USERDATABASE).where(USERDATABASE.NAME==client_id)
        execution=await session.execute(main_checker)
        response=execution.first()
        
        if response is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,detail='PLEASE ENTER VALID TOKENS TO ACCESS THE DATAS')

        



    async def getting_the_ip(self,request:Request):
        ip=request.client.host
        return ip

    async def user_exists(self,user_model:USERACCOUNT,session:AsyncSession):
        check_user=select(USERDATABASE).where(USERDATABASE.NAME==user_model.NAME,USERDATABASE.CONTACT_NO==user_model.CONTACT_NO)
        execution=await session.execute(check_user)
        response=execution.first()
        if response is not None:
            return True



    


        


h=HELPERS()


@router.post('/create/account/')
async def create_account(user_model:USERACCOUNT,session:AsyncSession=Depends(get_session)):
    user_exists=await h.user_exists(user_model,session)

    if user_exists is True:
        return JSONResponse(content={
            'message':'USER EXISTS PLEASE LOGIN'
        })
    else:
        new_user=await h.creating_the_new_user(user_model,session)
        if new_user is not None:

            return JSONResponse({
                'STATUS':'ACCOUNT CREATED SUCCESFULLY'
                
            })


@router.get('/user/login/')

async def user_login(user_model:USERACCOUNT,session:AsyncSession=Depends(get_session)):
    user_verify=await h.verify_the_user(user_model,session)
    if user_verify is not None:
        create_access_tokens=await h.getting_the_access_tokens(user_model,session)
        if create_access_tokens is not None:
            return JSONResponse({
                'STATUS':'SUCCESSFULL',
                'ACCESS_TOKENS':create_access_tokens
            })



   

@router.get('/stock/day/data/')
async def getting_the_daily_data(user_model:DATASET,session:AsyncSession=Depends(get_session)):
    token_verify=await h.decoding_the_access_tokens(user_model,session)
    if token_verify is not None:

    
        required_data=await s.getting_the_required_day_data(user_model)
        if required_data is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail='NO DATA AVAILAIBLE FOR THE GIVEN ISIN')
        return JSONResponse({
            'STATUS':'DATA FETCHED SUCCESFULLY',
            'DATA':required_data
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








                            



                    


