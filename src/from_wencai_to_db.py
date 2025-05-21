import sys
from pathlib import Path
import concurrent.futures

# 将src目录添加到Python路径中
src_dir = Path(__file__).parent.parent
sys.path.append(str(src_dir))

from loguru import logger
import pywencai
import pandas as pd
import mysql.connector
from mysql.connector import Error
from utils import get_trade_dates
from dotenv import load_dotenv
import os

# 加载环境变量
load_dotenv("instance.env")

# 设置项目根目录路径
project_root = src_dir.parent
data_dir = project_root / 'data'
log_dir = project_root / 'logs'

# MySQL配置
MYSQL_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'database': os.getenv('DB_NAME', 'taoquant'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', '')
}

def get_up_limit_from_wencai(trade_date="20250322"):
    """
    从wencai获取指定交易日期涨停的股票
    """
    q0=f"{trade_date},涨停,涨幅前100"
    df0 = pywencai.get(query=q0)
    q1=f"{trade_date},涨停,涨幅后100"
    df1 = pywencai.get(query=q1)
    df = pd.concat([df0, df1])
    # 按股票代码列去重
    df = df.drop_duplicates(subset=['股票代码'])
    return df

def save_df_to_db(df, trade_date):
    """
    将df保存到MySQL数据库
    
    Args:
        df: 包含涨停股票数据的DataFrame
        trade_date: 交易日期
    """
    try:
        # 连接到MySQL数据库
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        
        # 创建表的SQL语句
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS up_limit_stocks (
            id VARCHAR(20) PRIMARY KEY,
            trade_date DATE,
            ts_code VARCHAR(10),
            stock_name VARCHAR(50),
            up_limit_time TIME,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # 创建表
        cursor.execute(create_table_sql)
        
        # 将日期转换为字符串格式 YYYYMMDD
        trade_date_str = trade_date.strftime('%Y%m%d')
        
        # 准备数据
        df['id'] = trade_date_str + df['股票代码']
        df['trade_date'] = trade_date_str
        
        # 重命名列以匹配数据库表结构
        column_mapping = {
            '股票代码': 'ts_code',
            '股票简称': 'stock_name',
            f'首次涨停时间[{trade_date_str}]': 'up_limit_time'
        }
        
        df = df.rename(columns=column_mapping)
        
        # 选择需要的列
        columns = ['id', 'trade_date', 'ts_code', 'stock_name', 'up_limit_time']
        
        # 将数据转换为元组列表
        data = df[columns].to_records(index=False).tolist()
        
        # 构建插入语句
        placeholders = ','.join(['%s' for _ in columns])
        insert_sql = f"""
        INSERT INTO up_limit_stocks 
        ({','.join(columns)})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
        trade_date = VALUES(trade_date),
        ts_code = VALUES(ts_code),
        stock_name = VALUES(stock_name),
        up_limit_time = VALUES(up_limit_time)
        """
        
        # 执行插入
        cursor.executemany(insert_sql, data)
        conn.commit()
        
        logger.info(f"成功保存 {len(data)} 条涨停股票数据到数据库")
        
    except Error as e:
        logger.error(f"保存数据到数据库时出错: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def get_trade_dates_already_done():
    """
    从MySQL数据库中获取已经处理过的交易日期
    """
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        
        # 检查表是否存在
        cursor.execute("SHOW TABLES LIKE 'up_limit_stocks'")
        if not cursor.fetchone():
            return []
            
        cursor.execute("SELECT DISTINCT trade_date FROM up_limit_stocks")
        trade_dates = cursor.fetchall()
        return [trade_date[0] for trade_date in trade_dates]
        
    except Error as e:
        logger.error(f"从数据库获取交易日期时出错: {e}")
        return []
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def process_single_date(trade_date):
    """
    处理单个交易日期的数据
    
    Args:
        trade_date: 交易日期
    """
    try:
        logger.debug(f"开始处理日期: {trade_date}")
        df = get_up_limit_from_wencai(trade_date)
        logger.info(f"\n{df}")
        save_df_to_db(df, trade_date)
        logger.debug(f"完成处理日期: {trade_date}")
    except Exception as e:
        logger.error(f"处理日期 {trade_date} 时出错: {e}")

def work():
    # 从数据库中获取已经处理过的交易日期
    trade_dates_already_done = get_trade_dates_already_done()
    trade_dates = get_trade_dates()    
    trade_dates = [trade_date for trade_date in trade_dates if trade_date not in trade_dates_already_done]
    
    # 获取要处理的日期数量
    day_length = int(os.getenv('DAY_LENGTH', 30))
    trade_dates = trade_dates[:day_length]
    
    # 使用线程池并行处理
    max_workers = min(day_length, os.cpu_count() * 2)  # 设置最大线程数为CPU核心数的2倍
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        futures = [executor.submit(process_single_date, trade_date) for trade_date in trade_dates]
        
        # 等待所有任务完成
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # 获取结果，如果有异常会在这里抛出
            except Exception as e:
                logger.error(f"任务执行出错: {e}")

if __name__ == "__main__":
    work()
