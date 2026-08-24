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
router=APIRouter()
engine=create_async_engine(database_url,echo=True)
http_client = httpx.AsyncClient()




@mini_sensibull.on_event('startup')
async def startup():
    await init_db()
    mini_sensibull.state.redis=Redis(host='localhost',port=6379)
    mini_sensibull.http_client=httpx.AsyncClient()

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
                                                    exit_price=self.getting_the_live_prices(order_symbol)   #SQUARE OFF ORDER USER INPUT BUY
                                                    old['STATUS']='CLOSED'
                                                    old['EXIT_TIME']=datetime.now().strftime("%H:%Y")
                                                    old['EXIT_PRICE']=exit_price

                                        
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
                                        new_orders={
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
                                    if status=='OPEN':
                                            
                                        if old_order_type==order_type:
                                            if order_symbol==symbols:
                                                if master_quantity_==quantity:
                                                    exit_price=self.getting_the_live_prices(order_symbol)
                                                    old['STATUS']='CLOSED'
                                                    old['EXIT_TIME']=datetime.now().strftime("%H:%Y")
                                                    old['EXIT_PRICE']=exit_price

                                    elif status=='CLOSED':
                                        for net in master_data:
                                            net_tradingsymbol=net['tradingsymbol']
                                            if net_tradingsymbol==symbols:
                                                net_strikeprice=net['strikeprice']
                                                net_expiry=net['expiry']
                                                net_exch_seg=net['exch_seg']
                                                net_lot_size=net['lot_size']
                                                net_instrumenttype=net['instrumenttype']
                                                nety_entry_price=self.getting_the_live_prices(net_tradingsymbol)
                                                net_name=net['name']
                                                new_orders={
                                                    'CLIENT_ID':client_id,
                                                    'STOCK_NAME':net_name,
                                                    'TRADINGSYMBOL':net_tradingsymbol,
                                                    'STRIKEPRICE':net_strikeprice,
                                                    'EXPIRY':net_expiry,
                                                    'QUANTITY':quantity,
                                                    'ENTRY_PRICE':self.getting_the_live_prices(net_tradingsymbol),
                                                    'EXIT_PRICE':'NA',
                                                    'TOTAL_INVESTED_AMT':net_lot_size*quantity*nety_entry_price,
                                                    'STATUS':'OPEN',
                                                    'ORDER_CATEGORY':'DELIVERY',
                                                    'TARGET_PRICE':target_price,
                                                    'STOP_LOSS':stop_loss,
                                                    'INSTRUMENT_TYPE':net_instrumenttype,
                                                    'EXCHANGE_SEGMENT':net_exch_seg,
                                                    'ORDER_TYPE':'SELL' 
                                                }
                                elif client_id!=client:
                                    for new_client in master_data:
                                        new_tradingsymbol=new_client['tradingsymbol']
                                        if new_tradingsymbol==symbols:
                                            new_strikeprice=new_client['strikeprice']
                                            new_expiry=new_client['expiry']
                                            new_instrumenttype=new_client['instrumenttype']
                                            new_lot_size=new_client['lot_size']
                                            new_exch_seg=new_client['exch_seg']
                                            new_entry_price=self.getting_the_live_prices(new_tradingsymbol)
                                            new_name=new_client['name']
                                            new_orders={
                                                'CLIENT_ID':client_id,
                                                'STOCK_NAME':new_name,
                                                'TRADINGSYMBOL':new_tradingsymbol,
                                                'STRIKEPRICE':new_strikeprice,
                                                'EXPIRY':new_expiry,
                                                'QUANTITY':quantity,
                                                'ENTRY_PRICE':new_entry_price,
                                                'EXIT_PRICE':'NA',
                                                'TOTAL_INVESTED_AMT':quantity*new_lot_size*new_entry_price,
                                                'STATUS':'OPEN',
                                                'ORDER_CATEGORY':'DELIVERY',
                                                'TARGET_PRICE':target_price,
                                                'STOP_LOSS':stop_loss,
                                                'INSTRUMENTTYPE':new_instrumenttype,
                                                'EXCHANGE_SEGMENT':new_exch_seg,
                                                'ORDER_TYPE':'SELL'
                                            }
                        elif order_type=='BUY':
                            if instrument_type=='PE':
                                if client_id==client:
                                    if status=='OPEN':
                                        if old_order_type!=order_type:
                                            if order_symbol==symbols:
                                                if master_quantity_==quantity:
                                                    exit_price=self.getting_the_live_prices(order_symbol)
                                                    old['STATUS']='CLOSED'
                                                    old['EXIT_TIME']=datetime.now().strftime("%H:%Y")
                                                    old['EXIT_PRICE']=exit_price



                                    elif status=='CLOSED':
                                        for sta in master_data:
                                            st_tradingsymbol=sta['tradingsymbol']
                                            if st_tradingsymbol==symbols:
                                                st_strikeprice=sta['strikeprice']
                                                st_expiry=sta['expiry']
                                                st_instrumenttype=sta['instrumenttype']
                                                st_exch_seg=sta['exch_seg']
                                                st_lot_size=sta['lot_size']
                                                st_name=sta['name']
                                                st_entry_price=self.getting_the_live_prices(st_tradingsymbol)
                                                new_orders={
                                                    'CLIENT_ID':client_id,
                                                    'STOCK_NAME':st_name,
                                                    'TRADINGSYMBOL':st_tradingsymbol,
                                                    'STRIKEPRICE':st_strikeprice,
                                                    'EXPIRY':st_expiry,
                                                    'QUANTITY':quantity,
                                                    'ENTRY_PRICE':self.getting_the_live_prices(st_tradingsymbol),
                                                    'EXIT_PRICE':'NA',
                                                    'TOTAL_INVESETED_AMT':quantity*st_lot_size*st_entry_price,
                                                    'STATUS':'OPEN',
                                                    'ORDER_CATEGORY':'DELIVERY',
                                                    'TARGET_PRICE':target_price,
                                                    'STOP_LOSS':stop_loss,
                                                    'INSTRUMENT_TYPE':st_instrumenttype,
                                                    'EXCHANGE_SEGMENT':st_exch_seg,
                                                    'ORDER_TYPE':'SELL'
                                                }
                                elif client_id!=client:
                                    for wizard in master_data:
                                        wizard_tradingsymbol=wizard['tradingsymbol']
                                        if wizard_tradingsymbol==symbols:
                                            wizard_strikeprice=wizard['strikeprice']
                                            wizard_expiry=wizard['expiry']
                                            wizard_instrumenttype=wizard['instrumenttype']
                                            wizard_lot_size=wizard['lot_size']
                                            wizard_exch_seg=wizard['exch_seg']
                                            wizard_name=wizard['name']
                                            wizard_entry_price=self.getting_the_live_prices(wizard_tradingsymbol)
                                            new_order={
                                                'CLIENT_ID':client_id,
                                                'STOCK_NAME':wizard_name,
                                                'TRADINGSYMBOL':wizard_tradingsymbol,
                                                'STRIKEPRICE':wizard_strikeprice,
                                                'EXPIRY':wizard_expiry,
                                                'QUANTITY':quantity,
                                                'ENTRY_PRICE':wizard_entry_price,
                                                'EXIT_PRICE':'NA',
                                                'TOTAL_INVESTED_AMT':wizard_lot_size*wizard_entry_price*quantity,
                                                'STATUS':'OPEN',
                                                'ORDER_CATEGORY':'DELIVERY',
                                                'TARGET_PRICE':target_price,
                                                'STOP_LOSS':stop_loss,
                                                'INSTRUMENT_TYPE':wizard_instrumenttype,
                                                'EXCHANGE_SEGMENT':wizard_exch_seg,
                                                'ORDER_TYPE':'SELL'
                                            }
                    if order_category == "INRTADAY":
                        if order_type == "SELL":
                            if instrument_type == "PE":
                                if client == client_id:
                                    if status == "OPEN":

                                        if old_order_type != order_type:

                                            if order_symbol == symbols:
                                                if master_quantity_ == quantity:
                                                    exit_price = self.getting_the_live_prices(
                                                        order_symbol
                                                    )  # SQUARE OFF ORDER USER INPUT BUY
                                                    old["STATUS"] = "CLOSED"
                                                    old["EXIT_TIME"] = datetime.now().strftime(
                                                        "%H:%Y"
                                                    )
                                                    old["EXIT_PRICE"] = exit_price

                                    elif status == "CLOSED":
                                        for datas in master_data:
                                            new_tradingsymbols = datas["tradingsymbol"]
                                            if symbols == new_tradingsymbols:
                                                new_strikeprice = datas["strikeprice"]
                                                new_expiry = datas["expiry"]
                                                new_exch_seg = datas["exch_seg"]
                                                new_lot_size = datas["lot_size"]
                                                new_instrument_type = datas["instrumenttype"]
                                                new_entry_price_ = self.getting_the_live_prices(
                                                    symbols
                                                )
                                                new_name_ = datas["name"]
                                                new_orders = {
                                                    "CLINET_ID": client_id,
                                                    "STOCK_NAME": new_name_,
                                                    "TRADINGSYMBOL": symbols,
                                                    "STRIKEPRICE": new_strikeprice,
                                                    "EXPIRY": new_expiry,
                                                    "QUANTITY": quantity,
                                                    "ENTRY_PRICE": new_entry_price_,
                                                    "EXIT_PRICE": "NA",
                                                    "TOTAL_INVESTED_AMT": new_entry_price_
                                                    * new_lot_size
                                                    * quantity,
                                                    "STATUS": "OPEN",
                                                    "ORDER_CATEGORY": "DELIVERY",
                                                    "TARGET_PRICE": target_price,
                                                    "STOP_LOSS": stop_loss,
                                                    "INSTRUMENT_TYPE": new_instrument_type,
                                                    "EXCHANGE_SEGMENT": new_exch_seg,
                                                    "ORDER_TYPE": "SELL",
                                                }
                                elif client_id != client:
                                    for zerod in master_data:
                                        zerod_tradingsymbol = zerod["tradingsymbol"]
                                        zerod_strikeprice = zerod["strikeprice"]
                                        zerod_expiry = zerod["expiry"]
                                        zerod_exch_seg = zerod["exch_seg"]
                                        zerod_lot_size = zerod["lot_size"]
                                        zerod_instrumenttype = zerod["instrumenttype"]
                                        zerod_name = zerod["name"]
                                        n_entry_price = self.getting_the_live_prices(
                                            zerod_tradingsymbol
                                        )
                                        new_orders = {
                                            "CLIENT_ID": client_id,
                                            "STOCK_NAME": zerod_name,
                                            "TRADINGSYMBOL": symbols,
                                            "STRIKEPRICE": zerod_strikeprice,
                                            "EXPIRY": zerod_expiry,
                                            "QUANTITY": quantity,
                                            "ENTRY_PRICE": n_entry_price,
                                            "EXIT_PRICE": "NA",
                                            "TOTAL_INVESTED_AMT": zerod_lot_size
                                            * quantity
                                            * n_entry_price,
                                            "STATUS": "OPEN",
                                            "ORDER_CATEGORY": "DELIVERY",
                                            "TARGET_PRICE": target_price,
                                            "STOP_LOSS": stop_loss,
                                            "INSTRUMENT_TYPE": zerod_instrumenttype,
                                            "EXCHANGE_SEGMENT": zerod_exch_seg,
                                            "ORDER_TYPE": "SELL",
                                        }
                            elif instrument_type == "CE":
                                if client_id == client_id:
                                    if status == "OPEN":

                                        if old_order_type == order_type:
                                            if order_symbol == symbols:
                                                if master_quantity_ == quantity:
                                                    exit_price = self.getting_the_live_prices(
                                                        order_symbol
                                                    )
                                                    old["STATUS"] = "CLOSED"
                                                    old["EXIT_TIME"] = datetime.now().strftime(
                                                        "%H:%Y"
                                                    )
                                                    old["EXIT_PRICE"] = exit_price

                                    elif status == "CLOSED":
                                        for net in master_data:
                                            net_tradingsymbol = net["tradingsymbol"]
                                            if net_tradingsymbol == symbols:
                                                net_strikeprice = net["strikeprice"]
                                                net_expiry = net["expiry"]
                                                net_exch_seg = net["exch_seg"]
                                                net_lot_size = net["lot_size"]
                                                net_instrumenttype = net["instrumenttype"]
                                                nety_entry_price = self.getting_the_live_prices(
                                                    net_tradingsymbol
                                                )
                                                net_name = net["name"]
                                                new_orders = {
                                                    "CLIENT_ID": client_id,
                                                    "STOCK_NAME": net_name,
                                                    "TRADINGSYMBOL": net_tradingsymbol,
                                                    "STRIKEPRICE": net_strikeprice,
                                                    "EXPIRY": net_expiry,
                                                    "QUANTITY": quantity,
                                                    "ENTRY_PRICE": self.getting_the_live_prices(
                                                        net_tradingsymbol
                                                    ),
                                                    "EXIT_PRICE": "NA",
                                                    "TOTAL_INVESTED_AMT": net_lot_size
                                                    * quantity
                                                    * nety_entry_price,
                                                    "STATUS": "OPEN",
                                                    "ORDER_CATEGORY": "DELIVERY",
                                                    "TARGET_PRICE": target_price,
                                                    "STOP_LOSS": stop_loss,
                                                    "INSTRUMENT_TYPE": net_instrumenttype,
                                                    "EXCHANGE_SEGMENT": net_exch_seg,
                                                    "ORDER_TYPE": "SELL",
                                                }
                                elif client_id != client:
                                    for new_client in master_data:
                                        new_tradingsymbol = new_client["tradingsymbol"]
                                        if new_tradingsymbol == symbols:
                                            new_strikeprice = new_client["strikeprice"]
                                            new_expiry = new_client["expiry"]
                                            new_instrumenttype = new_client["instrumenttype"]
                                            new_lot_size = new_client["lot_size"]
                                            new_exch_seg = new_client["exch_seg"]
                                            new_entry_price = self.getting_the_live_prices(
                                                new_tradingsymbol
                                            )
                                            new_name = new_client["name"]
                                            new_orders = {
                                                "CLIENT_ID": client_id,
                                                "STOCK_NAME": new_name,
                                                "TRADINGSYMBOL": new_tradingsymbol,
                                                "STRIKEPRICE": new_strikeprice,
                                                "EXPIRY": new_expiry,
                                                "QUANTITY": quantity,
                                                "ENTRY_PRICE": new_entry_price,
                                                "EXIT_PRICE": "NA",
                                                "TOTAL_INVESTED_AMT": quantity
                                                * new_lot_size
                                                * new_entry_price,
                                                "STATUS": "OPEN",
                                                "ORDER_CATEGORY": "DELIVERY",
                                                "TARGET_PRICE": target_price,
                                                "STOP_LOSS": stop_loss,
                                                "INSTRUMENTTYPE": new_instrumenttype,
                                                "EXCHANGE_SEGMENT": new_exch_seg,
                                                "ORDER_TYPE": "SELL",
                                            }
                        elif order_type == "BUY":
                            if instrument_type == "PE":
                                if client_id == client:
                                    if status == "OPEN":
                                        if old_order_type != order_type:
                                            if order_symbol == symbols:
                                                if master_quantity_ == quantity:
                                                    exit_price = self.getting_the_live_prices(
                                                        order_symbol
                                                    )
                                                    old["STATUS"] = "CLOSED"
                                                    old["EXIT_TIME"] = datetime.now().strftime(
                                                        "%H:%Y"
                                                    )
                                                    old["EXIT_PRICE"] = exit_price

                                    elif status == "CLOSED":
                                        for sta in master_data:
                                            st_tradingsymbol = sta["tradingsymbol"]
                                            if st_tradingsymbol == symbols:
                                                st_strikeprice = sta["strikeprice"]
                                                st_expiry = sta["expiry"]
                                                st_instrumenttype = sta["instrumenttype"]
                                                st_exch_seg = sta["exch_seg"]
                                                st_lot_size = sta["lot_size"]
                                                st_name = sta["name"]
                                                st_entry_price = self.getting_the_live_prices(
                                                    st_tradingsymbol
                                                )
                                                new_orders = {
                                                    "CLIENT_ID": client_id,
                                                    "STOCK_NAME": st_name,
                                                    "TRADINGSYMBOL": st_tradingsymbol,
                                                    "STRIKEPRICE": st_strikeprice,
                                                    "EXPIRY": st_expiry,
                                                    "QUANTITY": quantity,
                                                    "ENTRY_PRICE": self.getting_the_live_prices(
                                                        st_tradingsymbol
                                                    ),
                                                    "EXIT_PRICE": "NA",
                                                    "TOTAL_INVESETED_AMT": quantity
                                                    * st_lot_size
                                                    * st_entry_price,
                                                    "STATUS": "OPEN",
                                                    "ORDER_CATEGORY": "DELIVERY",
                                                    "TARGET_PRICE": target_price,
                                                    "STOP_LOSS": stop_loss,
                                                    "INSTRUMENT_TYPE": st_instrumenttype,
                                                    "EXCHANGE_SEGMENT": st_exch_seg,
                                                    "ORDER_TYPE": "SELL",
                                                }
                                elif client_id != client:
                                    for wizard in master_data:
                                        wizard_tradingsymbol = wizard["tradingsymbol"]
                                        if wizard_tradingsymbol == symbols:
                                            wizard_strikeprice = wizard["strikeprice"]
                                            wizard_expiry = wizard["expiry"]
                                            wizard_instrumenttype = wizard["instrumenttype"]
                                            wizard_lot_size = wizard["lot_size"]
                                            wizard_exch_seg = wizard["exch_seg"]
                                            wizard_name = wizard["name"]
                                            wizard_entry_price = self.getting_the_live_prices(
                                                wizard_tradingsymbol
                                            )
                                            new_order = {
                                                "CLIENT_ID": client_id,
                                                "STOCK_NAME": wizard_name,
                                                "TRADINGSYMBOL": wizard_tradingsymbol,
                                                "STRIKEPRICE": wizard_strikeprice,
                                                "EXPIRY": wizard_expiry,
                                                "QUANTITY": quantity,
                                                "ENTRY_PRICE": wizard_entry_price,
                                                "EXIT_PRICE": "NA",
                                                "TOTAL_INVESTED_AMT": wizard_lot_size
                                                * wizard_entry_price
                                                * quantity,
                                                "STATUS": "OPEN",
                                                "ORDER_CATEGORY": "DELIVERY",
                                                "TARGET_PRICE": target_price,
                                                "STOP_LOSS": stop_loss,
                                                "INSTRUMENT_TYPE": wizard_instrumenttype,
                                                "EXCHANGE_SEGMENT": wizard_exch_seg,
                                                "ORDER_TYPE": "SELL",
                                            }
                            elif instrument_type == "CE":
                                if client_id == client_id:
                                    if status == "OPEN":

                                        if old_order_type == order_type:
                                            if order_symbol == symbols:
                                                if master_quantity_ == quantity:
                                                    exit_price = self.getting_the_live_prices(
                                                        order_symbol
                                                    )
                                                    old["STATUS"] = "CLOSED"
                                                    old["EXIT_TIME"] = datetime.now().strftime(
                                                        "%H:%Y"
                                                    )
                                                    old["EXIT_PRICE"] = exit_price

                                    elif status == "CLOSED":
                                        for net in master_data:
                                            net_tradingsymbol = net["tradingsymbol"]

                                            if net_tradingsymbol == symbols:
                                                net_strikeprice = net["strikeprice"]
                                                net_expiry = net["expiry"]
                                                net_exch_seg = net["exch_seg"]
                                                net_lot_size = net["lot_size"]
                                                net_instrumenttype = net["instrumenttype"]

                                                nety_entry_price = self.getting_the_live_prices(
                                                    net_tradingsymbol
                                                )

                                                net_name = net["name"]

                                                new_orders = {
                                                    "CLIENT_ID": client_id,
                                                    "STOCK_NAME": net_name,
                                                    "TRADINGSYMBOL": net_tradingsymbol,
                                                    "STRIKEPRICE": net_strikeprice,
                                                    "EXPIRY": net_expiry,
                                                    "QUANTITY": quantity,
                                                    "ENTRY_PRICE": self.getting_the_live_prices(
                                                        net_tradingsymbol
                                                    ),
                                                    "EXIT_PRICE": "NA",
                                                    "TOTAL_INVESTED_AMT": (
                                                        net_lot_size
                                                        * quantity
                                                        * nety_entry_price
                                                    ),
                                                    "STATUS": "OPEN",
                                                    "ORDER_CATEGORY": "DELIVERY",
                                                    "TARGET_PRICE": target_price,
                                                    "STOP_LOSS": stop_loss,
                                                    "INSTRUMENT_TYPE": net_instrumenttype,
                                                    "EXCHANGE_SEGMENT": net_exch_seg,
                                                    "ORDER_TYPE": "SELL",
                                                }

                                elif client_id != client:
                                    for new_client in master_data:
                                        new_tradingsymbol = new_client["tradingsymbol"]

                                        if new_tradingsymbol == symbols:
                                            new_strikeprice = new_client["strikeprice"]
                                            new_expiry = new_client["expiry"]
                                            new_instrumenttype = new_client["instrumenttype"]
                                            new_lot_size = new_client["lot_size"]
                                            new_exch_seg = new_client["exch_seg"]

                                            new_entry_price = self.getting_the_live_prices(
                                                new_tradingsymbol
                                            )

                                            new_name = new_client["name"]

                                            new_orders = {
                                                "CLIENT_ID": client_id,
                                                "STOCK_NAME": new_name,
                                                "TRADINGSYMBOL": new_tradingsymbol,
                                                "STRIKEPRICE": new_strikeprice,
                                                "EXPIRY": new_expiry,
                                                "QUANTITY": quantity,
                                                "ENTRY_PRICE": new_entry_price,
                                                "EXIT_PRICE": "NA",
                                                "TOTAL_INVESTED_AMT": (
                                                    quantity
                                                    * new_lot_size
                                                    * new_entry_price
                                                ),
                                                "STATUS": "OPEN",
                                                "ORDER_CATEGORY": "DELIVERY",
                                                "TARGET_PRICE": target_price,
                                                "STOP_LOSS": stop_loss,
                                                "INSTRUMENTTYPE": new_instrumenttype,
                                                "EXCHANGE_SEGMENT": new_exch_seg,
                                                "ORDER_TYPE": "SELL",
                                            }
        session.add(new_orders)
        await session.commit()
        await session.refresh(new_orders)





    def processing_the_orders(self):
        main_orders=ORDER_DATABASE
        current_date_time=datetime.now().strftime("%H:%Y")
        current_date=''
        client_id=main_orders.CLIENT_ID
        order_category=main_orders.ORDER_CATEGORY
        order_type=main_orders.ORDER_TYPE
        stop_loss=main_orders.STOP_LOSS
        expiry=main_orders.EXPIRY
        target_price=main_orders.TARGET_PRICE
        tradingsymbol=main_orders.TRADINGSYMBOL
        if status=='OPEN':

            if order_category=='DELIVERY':
                if order_type=='SELL':

                    if stop_loss is not  None:
                        if target_price is not None:
                            if expiry is not None:
                                if expiry!=current_date:
                                    if current_date_time<'15:30':

                                        exit_price=self.getting_the_live_prices(tradingsymbol)
                                        if target_price<=exit_price:

                                            main_orders['STATUS']='CLOSED'
                                            main_orders['EXIT_PRICE']=exit_price

                                            main_orders['EXIT_TIME']=datetime.now().strftime("%H:%Y")
                                        elif exit_price>=stop_loss:
                                            main_orders['STATUS']='CLOSED'
                                            main_orders['EXIT_PRICE']=exit_price
                                            main_orders['EXIT_TIME']=datetime.now().strftime("%H:%Y")

                                elif expiry==current_date:
                                    if current_date_time<'15:30':
                                        exit_price=self.getting_the_live_prices(tradingsymbol)
                                        if exit_price<=target_price:
                                            main_orders['STATUS']="CLOSED"
                                            main_orders['EXIT_PRICE']=exit_price
                                            main_orders['EXIT_TIME']=datetime.now().strftime("%H:%Y")
                                        elif exit_price>= stop_loss:
                                            main_orders['STATUS']='CLOSED'
                                            main_orders['EXIT_PRICE']=exit_price
                                            main_orders['EXIT_TIME']=datetime.now().strftime("%H:%Y")

                                    elif current_date_time>='15:30':
                                        main_orders['STATUS']='CLOSED'
                                        main_orders['EXIT_PRICE']=exit_price
                                        main_orders['EXIT_TIME']=datetime.now().strftime("%H:%Y")
                    elif stop_loss is None:
                        if target_price is not None:
                            if expiry is not None:
                                exit_price=self.getting_the_live_prices(tradingsymbol)
                                if current_date!=expiry:
                                    if current_date_time<'15:30':

                                        if exit_price<=target_price:
                                            main_orders['STATUS']='CLOSED'
                                            main_orders['EXIT_PRICE']=exit_price

                                            main_orders['EXIT_TIME']=datetime.now().strftime("%H:%Y")
                                        elif exit_price>=stop_loss:
                                            main_orders['STATUS']='CLOSED'
                                            main_orders['EXIT_PRICE']=exit_price
                                            main_orders['EXIT_TIME']=datetime.now().strftime("%H:%Y")
                                elif current_date==expiry:
                                    if current_date_time<'15:30':
                                        if exit_price<=target_price:

                                            main_orders['EXIT_PRICE']=exit_price

                                            main_orders['STATUS']='CLOSED'
                                            main_orders['EXIT_TIME']=datetime.now().strftime("%H:%Y")
                                        elif exit_price>=stop_loss:
                                            main_orders['EXIT_PRICE']=exit_price
                                            main_orders['EXIT_TIME']=datetime.now().strftime('%H:%Y')
                                            main_orders['STATUS']='CLOSED'





                                    elif current_date_time>='15:30':
                                        main_orders['STATUS']='CLOSED'
                                        main_orders['EXIT_PRICE']=exit_price

                                        main_orders['EXIT_TIME']=datetime.now().strftime('%H:%Y')

                    elif stop_loss is None:
                        if target_price is None:
                            exit_price=self.getting_the_live_prices(tradingsymbol) 
                            if current_date==expiry:
                                if current_date_time>='15:30':
                                    main_orders['STATUS']='CLOSED'
                                    main_orders['EXIT_TIME']=datetime.now().strftime("%H:%Y")
                                    main_orders['EXIT_PRICE']=exit_price


                                    

                                        

                                                


            






                







    

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


@router.get('/stock/day/data/')
async def getting_the_daily_data(user_model:DATASET):
    
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








                            



                    


