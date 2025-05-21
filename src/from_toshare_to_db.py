from pathlib import Path    
import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
# 导入tushare
import tushare as ts
from loguru import logger

# 配置loguru
logger.add("logs/tushare_to_db_{time}.log", rotation="500 MB", level="INFO")

# 加载环境变量
load_dotenv('instance.env')

# 获取Tushare API key
tushare_api_key = os.getenv('tushare_api_key')
if not tushare_api_key:
    raise ValueError("未找到Tushare API key，请在.env文件中设置tushare_api_key")

# 数据库配置
db_config = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'database': os.getenv('DB_NAME', 'taoquant'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', '')
}

# 初始化pro接口
pro = ts.pro_api(tushare_api_key)

def get_daily_basic(end_date="", ts_code=""):
    df = pro.daily_basic(end_date=end_date, ts_code=ts_code, fields=[
            "free_share",
            "ts_code",
            "trade_date",
            "close"
        ])
    return df

def get_daily_data(end_date="", ts_code=""):
    df = pro.daily(**{
        "ts_code": ts_code,
        "trade_date": "",
        "start_date": "",
        "end_date": end_date,
        "offset": "",
        "limit": ""
    }, fields=[
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount"
    ])
    return df

def get_stock_basic():
    # 拉取数据
    df = pro.stock_basic(**{
        "ts_code": "",
        "name": "",
        "exchange": "",
        "market": "",
        "is_hs": "",
        "list_status": "",
        "limit": "",
        "offset": ""
    }, fields=[
        "ts_code",
        "symbol",
        "name",
        "market",
        "list_date"
    ])
    return df

def get_limit_list_d():
    # 拉取数据
    df = pro.limit_list_d(**{
        "trade_date": "",
        "ts_code": "",
        "limit_type": "",
        "exchange": "",
        "start_date": "",
        "end_date": "",
        "limit": "",
        "offset": ""
    }, fields=[
        "trade_date",
        "ts_code",
        "industry",
        "name",
        "close",
        "pct_chg",
        "amount",
        "limit_amount",
        "float_mv",
        "total_mv",
        "turnover_ratio",
        "fd_amount",
        "first_time",
        "last_time",
        "open_times",
        "up_stat",
        "limit_times",
        "limit"
    ])
    return df

def create_tables():
    try:
        # 首先尝试连接MySQL服务器（不指定数据库）
        conn = mysql.connector.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password']
        )
        cursor = conn.cursor()
        
        # 创建数据库（如果不存在）
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_config['database']}")
        cursor.close()
        conn.close()
        
        # 重新连接到新创建的数据库
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # 创建stock_basic表
        cursor.execute('''CREATE TABLE IF NOT EXISTS stock_basic 
                    (ts_code VARCHAR(10) PRIMARY KEY, 
                    symbol VARCHAR(6), 
                    name VARCHAR(50), 
                    market VARCHAR(20), 
                    list_date DATE)''')
        
        # 创建daily_data表
        cursor.execute('''CREATE TABLE IF NOT EXISTS daily_data 
                    (id VARCHAR(20) PRIMARY KEY, 
                    ts_code VARCHAR(10), 
                    trade_date DATE, 
                    open DECIMAL(10,2), 
                    high DECIMAL(10,2), 
                    low DECIMAL(10,2), 
                    close DECIMAL(10,2), 
                    pre_close DECIMAL(10,2), 
                    price_change DECIMAL(10,2), 
                    pct_chg DECIMAL(10,2), 
                    vol DECIMAL(20,2), 
                    amount DECIMAL(20,2),
                    INDEX idx_ts_code_trade_date (ts_code, trade_date))''')
        
        # 创建daily_basic表
        cursor.execute('''CREATE TABLE IF NOT EXISTS daily_basic 
                    (id VARCHAR(20) PRIMARY KEY, 
                    ts_code VARCHAR(10), 
                    trade_date DATE, 
                    free_share DECIMAL(20,2), 
                    close DECIMAL(10,2),
                    INDEX idx_ts_code_trade_date (ts_code, trade_date))''')
        
        conn.commit()
        logger.info("数据库表创建成功")
        
    except Error as e:
        logger.error(f"创建表时出错: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def save_stock_basic_to_db(df):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # 准备数据
        data = df.to_records(index=False).tolist()
        
        # 使用INSERT OR REPLACE语句
        query = '''INSERT INTO stock_basic 
                (ts_code, symbol, name, market, list_date) 
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                symbol = VALUES(symbol),
                name = VALUES(name),
                market = VALUES(market),
                list_date = VALUES(list_date)'''
                
        cursor.executemany(query, data)
        conn.commit()
        logger.info("股票基础数据保存成功")
        
    except Error as e:
        logger.error(f"保存股票基础数据时出错: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def save_daily_data_to_db(df):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # 添加id列
        df['id'] = df['trade_date'] + df['ts_code']
        
        # 重命名change列为price_change
        df = df.rename(columns={'change': 'price_change'})
        
        # 准备数据
        data = df[['id', 'ts_code', 'trade_date', 'open', 'high', 'low', 
                  'close', 'pre_close', 'price_change', 'pct_chg', 'vol', 'amount']].to_records(index=False).tolist()
        
        # 使用INSERT OR REPLACE语句
        query = '''INSERT INTO daily_data 
                (id, ts_code, trade_date, open, high, low, close, 
                pre_close, price_change, pct_chg, vol, amount) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                open = VALUES(open),
                high = VALUES(high),
                low = VALUES(low),
                close = VALUES(close),
                pre_close = VALUES(pre_close),
                price_change = VALUES(price_change),
                pct_chg = VALUES(pct_chg),
                vol = VALUES(vol),
                amount = VALUES(amount)'''
                
        cursor.executemany(query, data)
        conn.commit()
        logger.info("日线数据保存成功")
        
    except Error as e:
        logger.error(f"保存日线数据时出错: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def save_daily_basic_to_db(df):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # 添加id列
        df['id'] = df['trade_date'] + df['ts_code']
        
        # 准备数据
        data = df[['id', 'ts_code', 'trade_date', 'free_share', 'close']].to_records(index=False).tolist()
        
        # 使用INSERT OR REPLACE语句
        query = '''INSERT INTO daily_basic 
                (id, ts_code, trade_date, free_share, close) 
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                free_share = VALUES(free_share),
                close = VALUES(close)'''
                
        cursor.executemany(query, data)
        conn.commit()
        logger.info("每日指标数据保存成功")
        
    except Error as e:
        logger.error(f"保存每日指标数据时出错: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def main():
    # 创建数据库表
    create_tables()
    
    # 获取股票基础数据
    df = get_stock_basic()
    # 保存到数据库
    save_stock_basic_to_db(df)
    logger.info("股票基础数据已保存")

    end_date=os.getenv('TUSHARE_GET_DATA_FIRST_ENDDAY', "")
    day_length = int(os.getenv('DAY_LENGTH', 30))
    logger.info(f"正在获取{day_length}天的数据")
    for i in range(day_length):
        logger.info(f"正在获取第 {i+1} 天的数据，结束日期: {end_date}")

        df = get_daily_basic(end_date=end_date)
        logger.debug(f"daily_basic数据长度: {len(df)}")
        save_daily_basic_to_db(df)
        
        df = get_daily_data(end_date=end_date)     
        logger.debug(f"daily_data数据长度: {len(df)}")
        save_daily_data_to_db(df)

        end_date = df['trade_date'].iloc[-1]

def test():
    ts_code="920799.BJ"
    df=get_daily_basic( ts_code=ts_code)
    save_daily_basic_to_db(df)
    df=get_daily_data( ts_code=ts_code)
    df=df.iloc[:-1]
    logger.info(df)
    save_daily_data_to_db(df)

if __name__ == "__main__":
    main()
    #