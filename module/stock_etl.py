import json
import datetime 

import requests
import pandas as pd 


def request_stock(date:str, stock_list:list, col_name:list):
    # 證交所爬取股價
    result_list = []
    for stock in stock_list:
        result = dict()
        col = ['成交金額','成交股數','開盤價','最高價','最低價','收盤價','日期']
        res = requests.get(f'https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={date.replace("-","")}&stockNo={stock}')
        data = json.loads(res.text)
        df = pd.DataFrame(data=data['data'], columns=data['fields'])
        df['日期'] = df['日期'].apply(lambda x : x.replace(x.split('/')[0],str(int(x.split('/')[0]) + 1911)).replace('/','-'))
        result_df = df[df['日期'] == date][col].reset_index()
        result_df['日期'] = pd.to_datetime(result_df['日期'], format='%Y-%m-%d').dt.date
        result[col_name[0]] = data['title'].split(' ')[1]
        result[col_name[1]] = data['title'].split(' ')[2]
        for cn, c  in zip(col_name[2:], result_df[col].columns):
            result[cn] = result_df[c][0] if c == '日期' else float(result_df[c][0].replace(',',''))
        result['update_dt'] = datetime.datetime.now().date()
        result_list.append(result)
    file_path = f'./{date}.parquet' 
    pd.DataFrame(result_list).to_parquet(file_path)
    return file_path
 

if __name__ == '__main__':
    date = '2023-11-15'
    col_name = ['symbol_id','name','Turnover','volume','open','high','low','close','stock_dt']
    stock_list = ['2330','0050']
    result = request_stock(date, stock_list, col_name)
    print(result)

 