## Cloud functions 爬取證交所每日股價
import os 
import datetime
import logging
# from dotenv import load_dotenv

from module.stock_etl import request_stock
from module.gcp_server import upload_to_storage, publish_to_pubsub

# load_dotenv()

def main(request):
    date = request.args.get('date', default=datetime.datetime.now().date().strftime('%Y-%m-%d'))
    col_name = ['symbol_id','name','turnover','volume','open','high','low','close','stock_dt']
    stock_list = os.environ.get('stock_list', '').split(',')
    bucket_name = 'gcf-v2-sources-54990889377-us-west2'
    blob_name = f'Daily_stock/{date}.parquet'
    sub_project_id = 'dulcet-asset-405310'
    sub_topic_name = 'error_stock'

    try:
        file_path = request_stock(date, stock_list, col_name)
        upload_to_storage(file_path, bucket_name, blob_name)
        logging.info(f'Data to {file_path} success!')
        return f'stock_dt:{date} is success!'
    except Exception as e :
        logging.error(e)
        publish_to_pubsub(date, sub_project_id, sub_topic_name, str(e))
        return f'stock_dt:{date} is Error!'
    