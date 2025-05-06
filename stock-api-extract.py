import json
import os
import boto3
from datetime import datetime
import urllib.request
import urllib.parse

def lambda_handler(event, context):
    stocks = {
        'GOOGL': 'Alphabet Inc. (Google)',
        'META': 'Meta Platforms Inc. (Facebook)',
        'NVDA': 'NVIDIA Corporation',
        'TSLA': 'Tesla Inc.',
        'AMZN': 'Amazon.com Inc.',
        'AMD': 'Advanced Micro Devices Inc.',
        'INTC': 'Intel Corporation',
        'CRM': 'Salesforce Inc.',
        'ORCL': 'Oracle Corporation',
        'ADBE': 'Adobe Inc.',
        'JPM': 'JPMorgan Chase & Co.',
        'BAC': 'Bank of America Corporation',
        'WFC': 'Wells Fargo & Company'
    }
    
    api_key = "ULVMOOUODKJ0CUCP"
    stock_data = {}
    
    # Fetch data for each stock
    for symbol in stocks.keys():
        try:
            url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={api_key}'
            with urllib.request.urlopen(url) as response:
                data = response.read().decode('utf-8')
                stock_data[symbol] = json.loads(data)
            
            # Add delay to avoid API rate limiting
            import time
            time.sleep(0.3)
            
        except Exception as e:
            print(f"Error fetching data for {symbol}: {str(e)}")
            stock_data[symbol] = {"error": str(e)}
    
    # Save to S3
    try:
        client = boto3.client('s3')
        
        # Generate unique filename with timestamp
        current_time = datetime.now()
        filename = f"stock_raw_{current_time.strftime('%Y%m%d_%H%M%S')}.json"
        
        # Upload to S3
        client.put_object(
            Bucket="stock-etl-project-kr",
            Key=f"raw_data/to_processed/{filename}",
            Body=json.dumps(stock_data)
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps(f"Successfully saved data for {len(stock_data)} stocks to S3")
        }
        
    except Exception as e:
        print(f"Error saving to S3: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }