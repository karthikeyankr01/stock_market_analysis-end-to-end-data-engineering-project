import json
import os
import boto3
from datetime import datetime
import pandas as pd 
import io
from io import StringIO

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = "stock-etl-project-kr"
    file_key="raw_data/to_processed/"
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=file_key)
    file_key=response[ 'Contents'][0]['Key']
    print(file_key)
    #print(response[ 'Contents'][0]['Key'])
    r = s3.get_object(Bucket = bucket_name, Key = file_key)
    content = r['Body']
    s = json.loads(content.read())
    #print(jsonObject)
    a=[]
    for i in s.keys():
        symbol=i
        date=s[i]['Global Quote']['07. latest trading day']
        stock_open=s[i]['Global Quote']['02. open']
        stock_high=s[i]['Global Quote']['03. high']
        stock_low=s[i]['Global Quote']['04. low']
        stock_close=s[i]['Global Quote']['05. price']
        previous_close=s[i]['Global Quote']['08. previous close']
        change=s[i]['Global Quote']['09. change']
        change_percent=s[i]['Global Quote']['10. change percent']
        stock_price={'symbol':symbol, 'stock_open':stock_open, 'date' : date, 'stock_open' :  stock_open, 'stock_high':stock_high, 'stock_low' : stock_low, 
                   'stock_close' :stock_close, 'previous_close' : previous_close, 'change' : change, 'change_percent' : change_percent }
        a.append(stock_price)
    
    stock_price_summary=pd.DataFrame.from_dict(a)
    #print(stock_price_summary)
    b=[]
    for i in s.keys():
        symbol=i
        date=s[i]['Global Quote']['07. latest trading day']
        volume=s[i]['Global Quote']['06. volume']
        price=s[i]['Global Quote']['05. price']
        value_traded=int(float(s[i]['Global Quote']['08. previous close']))*int(float(s[i]['Global Quote']['06. volume']))
        day_of_week=pd.to_datetime(s[i]['Global Quote']['07. latest trading day']).strftime("%A")
        stock_volume={'symbol':symbol, 'date' : date, 'volume' : volume, 'price' :price, 'value_traded' : value_traded, 
                   'day_of_week' : day_of_week }
        b.append(stock_volume)
    
    stock_volume_analysis=pd.DataFrame.from_dict(b)
    #print(stock_volume_analysis)

    c=[]
    for i in s.keys():
        symbol=i
        date=s[i]['Global Quote']['07. latest trading day']
        high_low_range=int(float(s[i]['Global Quote']['03. high']))-int(float(s[i]['Global Quote']['04. low']))
        high_low_percent=(int(float(s[i]['Global Quote']['03. high']))-int(float(s[i]['Global Quote']['04. low']))
                      /int(float(s[i]['Global Quote']['04. low']))*100)
        open_close_range=int(float(s[i]['Global Quote']['02. open']))-int(float(s[i]['Global Quote']['05. price']))
        open_close_percent=(int(float(s[i]['Global Quote']['02. open']))-int(float(s[i]['Global Quote']['05. price']))
                        /int(float(s[i]['Global Quote']['05. price']))*100)
        avg_price=(int(float(s[i]['Global Quote']['03. high']))+int(float(s[i]['Global Quote']['04. low'])))/2
        previous_close=s[i]['Global Quote']['08. previous close']
        is_green_day=int(float(s[i]['Global Quote']['05. price']))>int(float(s[i]['Global Quote']['02. open']))
        stock_volatility={'symbol':symbol, 'date' : date, 'high_low_range' : high_low_range, 'high_low_percent' : high_low_percent, 
                   'open_close_range' : open_close_range, 'open_close_percent' : open_close_percent, 'avg_price' : avg_price, 
                   'previous_close' : previous_close, 'is_green_day' : is_green_day }
        c.append(stock_volatility)

    stock_volatility_analysis=pd.DataFrame.from_dict(c)


    filename_stock_price = "transformed_data/processed/stock_price/stock_price_" + str(datetime.now()) + ".csv"
    stock_price_buffer=StringIO()
    stock_price_summary.to_csv(stock_price_buffer, index=False)
    stock_price_content = stock_price_buffer.getvalue()
    s3.put_object(Bucket=bucket_name, Key=filename_stock_price, Body=stock_price_content)

    filename_stock_volume = "transformed_data/processed/stock_volume/stock_volume_" + str(datetime.now()) + ".csv"
    stock_volume_buffer=StringIO()
    stock_volume_analysis.to_csv(stock_volume_buffer, index=False)
    stock_volume_content = stock_volume_buffer.getvalue()
    s3.put_object(Bucket=bucket_name, Key=filename_stock_volume, Body=stock_volume_content)

    filename_stock_volatility = "transformed_data/processed/stock_volatility/stock_volatility_" + str(datetime.now()) + ".csv"
    stock_volatility_buffer=StringIO()
    stock_volatility_analysis.to_csv(stock_volatility_buffer, index=False)
    stock_volatility_content = stock_volatility_buffer.getvalue()
    s3.put_object(Bucket=bucket_name, Key=filename_stock_volatility, Body=stock_volatility_content)

    s3_resource = boto3.resource('s3')
    destination_key = 'raw_data/processed/' + file_key.split('/')[-1]

    # Copy the file
    copy_source = {
        'Bucket': bucket_name,
        'Key': file_key
    }
    # Use resource method for copy
    s3_resource.Object(bucket_name, destination_key).copy_from(CopySource=copy_source)

    # Delete original
    s3_resource.Object(bucket_name, file_key).delete()
    
    

    

    

    
