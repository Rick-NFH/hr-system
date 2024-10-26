import os
import sys
import time
import requests
import base64
import hmac
import hashlib
import json
from datetime import datetime, timezone, timedelta
import pandas as pd
from dotenv import load_dotenv
import logging
from tqdm import tqdm  # 引入 tqdm 以显示进度条
import urllib.parse

# 加载 .env 文件以保护敏感信息
load_dotenv()

# 配置日志记录，确保使用 UTF-8 编码
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 创建日志格式
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# 创建 FileHandler，使用 UTF-8 编码
file_handler = logging.FileHandler("okx_api.log", encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# 创建 StreamHandler，使用 UTF-8 编码
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# OKX API 配置信息
okx_api_key = "93a3ede6-ddb9-450a-a05d-7ddecf448dfe"
okx_secret_key = "0A8637937C982EF14733653451DEB824"
okx_passphrase = "0311!Nifflerhodl"
okx_base_url = 'https://www.okx.com'

# Bybit API 配置信息
bybit_api_key = 'zIcoPoDiQdfRyDfUrn'
bybit_secret_key = 'd6F9daPUFjYUqDjlMQe7jdKiYhifbPwo4ThD'
bybit_endpoint = 'https://api.bybit.com'
bybit_path = '/v5/execution/list'
recv_window = '5000'  # Bybit 接收窗口，默认为5000（毫秒）

# Bybit API 时间范围设置
total_days = 90  # 查询的总天数
total_time = total_days * 24 * 60 * 60 * 1000
max_time_range = 7 * 24 * 60 * 60 * 1000
current_time = int(time.time() * 1000)

# 获取 ISO8601 格式的时间戳
def get_timestamp():
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


# 生成签名函数 (OKX API)
def generate_signature(timestamp, method, request_path, body, secret_key):
    if body:
        body = json.dumps(body)
    else:
        body = ''
    message = timestamp + method + request_path + body
    mac = hmac.new(secret_key.encode('utf-8'), message.encode('utf-8'), hashlib.sha256)
    d = mac.digest()
    return base64.b64encode(d).decode('utf-8')


# 获取 OKX 账单流水归档函数
def get_bills_archive(after=None, before=None, limit=100, max_retries=5):
    params = {
        'limit': limit
    }
    if after:
        params['after'] = after
    if before:
        params['before'] = before

    method = 'GET'
    request_path = '/api/v5/account/bills-archive'
    if params:
        query_string = '&'.join([f'{key}={value}' for key, value in params.items()])
        request_path += '?' + query_string
    url = okx_base_url + request_path
    timestamp = get_timestamp()

    signature = generate_signature(timestamp, method, request_path, '', okx_secret_key)
    headers = {
        'OK-ACCESS-KEY': okx_api_key,
        'OK-ACCESS-SIGN': signature,
        'OK-ACCESS-TIMESTAMP': timestamp,
        'OK-ACCESS-PASSPHRASE': okx_passphrase,
        'Content-Type': 'application/json'
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            result = response.json()
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP 错误: {e}")
            return {'data': []}
        except requests.exceptions.RequestException as e:
            logging.error(f"请求异常: {e}")
            return {'data': []}
        except json.JSONDecodeError:
            logging.error("无法解析 JSON 响应。")
            return {'data': []}

        if result.get('code') == '0':
            return result
        elif result.get('code') == '50011':  # Too Many Requests
            wait_time = 2 ** attempt
            logging.warning(f"遇到限流错误，等待 {wait_time} 秒后重试... (尝试 {attempt + 1}/{max_retries})")
            time.sleep(wait_time)
        else:
            logging.error(f"错误: {result.get('code')} - {result.get('msg')}")
            return {'data': []}

    logging.error("超过最大重试次数，终止请求。")
    return {'data': []}


# 获取所有 OKX 账单流水
def fetch_all_okx_bills():
    all_bills = []
    more_data = True
    after = None

    with tqdm(total=0, unit='records', desc="Fetching OKX Bills") as pbar:
        while more_data:
            data = get_bills_archive(after=after)
            bills = data.get('data', [])
            if bills:
                all_bills.extend(bills)
                pbar.update(len(bills))
                after = bills[-1]['billId']
                time.sleep(1)
            else:
                more_data = False

    return all_bills


# 获取 Bybit API Funding记录函数
def fetch_bybit_executions():
    all_results = []
    symbol_funding_summary_bybit = []
    num_requests = (total_time + max_time_range - 1) // max_time_range

    with tqdm(total=0, unit='records', desc="Fetching Bybit Executions") as pbar:
        for i in range(int(num_requests)):
            end_time = current_time - i * max_time_range
            start_time = max(end_time - max_time_range, current_time - total_time)

            has_more = True
            cursor = ''

            while has_more:
                timestamp = str(int(time.time() * 1000))

                params = {
                    'category': 'linear',
                    'startTime': str(int(start_time)),
                    'endTime': str(int(end_time)),
                    'limit': '200'
                }

                if cursor:
                    params['cursor'] = cursor

                sorted_params = dict(sorted(params.items()))
                query_string = urllib.parse.urlencode(sorted_params)
                sign_str = timestamp + bybit_api_key + recv_window + query_string
                hash = hmac.new(bytes(bybit_secret_key, 'utf-8'), bytes(sign_str, 'utf-8'), hashlib.sha256)
                signature = hash.hexdigest()

                headers = {
                    'X-BAPI-API-KEY': bybit_api_key,
                    'X-BAPI-TIMESTAMP': timestamp,
                    'X-BAPI-SIGN': signature,
                    'X-BAPI-RECV-WINDOW': recv_window,
                    'Content-Type': 'application/json',
                }

                url = f"{bybit_endpoint}{bybit_path}?{query_string}"
                response = requests.get(url, headers=headers)

                if response.status_code != 200:
                    print(f"请求失败，状态码：{response.status_code}")
                    break

                try:
                    data = response.json()
                    if data.get('retCode') == 0:
                        result_list = data['result']['list']
                        all_results.extend(result_list)
                        pbar.update(len(result_list))

                        cursor = data['result'].get('nextPageCursor', '')
                        has_more = bool(cursor)

                        # 处理Funding类型的记录
                        for record in result_list:
                            exec_type = record.get('execType', '')
                            symbol = record.get('symbol', 'N/A').replace('USDT', '')  # 删除 'USDT'

                            # 仅抓取Funding类型的记录
                            if exec_type == 'Funding':
                                funding_fee_str = record.get('execFee', '0')
                                exec_time_str = record.get('execTime', '0')

                                try:
                                    funding_fee = float(funding_fee_str)
                                    exec_time = pd.to_datetime(int(exec_time_str), unit='ms', errors='coerce')  # 转换时间戳
                                    adjusted_funding_fee = -funding_fee
                                    symbol_funding_summary_bybit.append({'symbol': symbol, 'execTime': exec_time, 'fundingFee': adjusted_funding_fee})
                                except ValueError:
                                    continue
                    else:
                        print(f"请求错误，错误信息：{data.get('retMsg')}")
                        has_more = False
                except ValueError:
                    print("响应格式不是JSON。")
                    has_more = False

                time.sleep(0.1)

    return pd.DataFrame(symbol_funding_summary_bybit)


def process_data():
    # 获取今天的日期
    today = datetime.now(timezone.utc).date()

    # OKX 数据处理
    bills = fetch_all_okx_bills()
    if not bills:
        logging.info("没有获取到任何 OKX 账单记录。")
    else:
        df = pd.DataFrame(bills)

        required_columns = ['instId', 'fillTime', 'pnl', 'subType']
        available_columns = df.columns.tolist()
        missing_columns = [col for col in required_columns if col not in available_columns]

        if missing_columns:
            logging.error(f"缺少以下必要的列: {missing_columns}")
        else:
            df['fillTime'] = pd.to_numeric(df['fillTime'], errors='coerce')
            df['fillTime'] = pd.to_datetime(df['fillTime'], unit='ms', errors='coerce')
            df['subType'] = pd.to_numeric(df['subType'], errors='coerce')

            df_filtered = df[df['subType'].isin([173, 174])].copy()
            df_filtered.loc[:, 'pnl'] = pd.to_numeric(df_filtered['pnl'], errors='coerce')
            df_filtered = df_filtered.dropna(subset=['pnl'])

            df_filtered.loc[:, 'instId'] = df_filtered['instId'].str.replace('-USDT-SWAP', '')

            df_filtered['is_today'] = df_filtered['fillTime'].dt.date == today
            df_filtered['holding'] = df_filtered.apply(lambda x: '持仓中' if x['is_today'] and x['pnl'] != 0 else '', axis=1)

            pnl_summary_okx = df_filtered.groupby('instId')['pnl'].sum().reset_index()
            pnl_summary_okx.rename(columns={'pnl': 'OKEX'}, inplace=True)
            pnl_summary_okx = pnl_summary_okx.sort_values(by='OKEX', ascending=False)

            print("\nOKX 每个货币的总收益 (已合并并删除 '-USDT-SWAP'):")
            pnl_summary_okx['holding'] = df_filtered.groupby('instId')['holding'].first().reset_index(drop=True)
            print(pnl_summary_okx.to_string(index=False))

            df_filtered['date'] = df_filtered['fillTime'].dt.date
            daily_pnl_okx = df_filtered.groupby('date')['pnl'].sum().reset_index()
            daily_pnl_okx.rename(columns={'pnl': 'OKX Daily PnL'}, inplace=True)

    # Bybit 数据处理
    bybit_df = fetch_bybit_executions()

    pnl_summary_bybit = bybit_df.groupby('symbol')['fundingFee'].sum().reset_index()
    pnl_summary_bybit.rename(columns={'symbol': 'instId', 'fundingFee': 'Bybit'}, inplace=True)
    pnl_summary_bybit = pnl_summary_bybit.sort_values(by='Bybit', ascending=False)

    print("\nBybit 每个货币的总Funding费用 (已删除 'USDT'):")
    print(pnl_summary_bybit.to_string(index=False))

    # 合并 OKX 和 Bybit 数据
    combined_summary = pd.merge(pnl_summary_okx, pnl_summary_bybit, on='instId', how='outer')
    combined_summary.fillna(0, inplace=True)  # 用 0 填充缺失值
    combined_summary['Total'] = combined_summary['OKEX'] + combined_summary['Bybit']

    # 标记“持仓中”
    combined_summary['holding'] = combined_summary['holding'].fillna('')  # 处理 NaN 的持仓标记
    combined_summary['holding'] = combined_summary.apply(lambda row: '持仓中' if row['holding'] == '持仓中' else '', axis=1)

    # 重新排列列顺序，把 holding 移动到最后一列
    combined_summary = combined_summary[['instId', 'OKEX', 'Bybit', 'Total', 'holding']]

    # 计算所有货币的总计
    total_pnl = combined_summary['OKEX'].sum()
    total_funding = combined_summary['Bybit'].sum()
    total_combined = combined_summary['Total'].sum()

    # 打印合并后的总结果
    print("\nOKX 和 Bybit 相同货币的总和 (收益 + Funding费用, 并标记持仓中):")
    print(combined_summary.to_string(index=False))

    # 打印所有总计
    print("\n所有总计:")
    print(f"总收益 (OKEX): {total_pnl:.2f}")
    print(f"总Funding费用 (Bybit): {total_funding:.2f}")
    print(f"收益 + Funding费用 (Total): {total_combined:.2f}")

    # 合并每日收益
    bybit_df['date'] = bybit_df['execTime'].dt.date
    daily_fee_bybit = bybit_df.groupby('date')['fundingFee'].sum().reset_index()
    daily_fee_bybit.rename(columns={'fundingFee': 'Bybit Daily Funding Fee'}, inplace=True)

    daily_combined = pd.merge(daily_pnl_okx, daily_fee_bybit, on='date', how='outer')
    daily_combined.fillna(0, inplace=True)
    daily_combined['Total Daily PnL'] = daily_combined['OKX Daily PnL'] + daily_combined['Bybit Daily Funding Fee']

    # 打印每天的收益汇总
    print("\n每天的双平台收益 (OKX + Bybit):")
    print(daily_combined.to_string(index=False))


# 执行主程序
if __name__ == '__main__':
    process_data()
