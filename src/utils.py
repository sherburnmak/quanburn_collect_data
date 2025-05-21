import mysql.connector
from loguru import logger
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv("instance.env")

# 数据库配置
db_config = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'database': os.getenv('DB_NAME', 'taoquant'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', '')
}

def get_trade_dates() -> list:
    """
    从MySQL中获取不同的trade date
    select distinct trade_date from daily_data;
    :return: 交易日期列表
    """
    try:
        # 连接到MySQL数据库
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # 执行SQL查询
        cursor.execute("SELECT DISTINCT trade_date FROM daily_data ORDER BY trade_date DESC")
        # 获取所有查询结果
        results = cursor.fetchall()
        # 提取交易日期并存储在列表中
        trade_dates = [row[0] for row in results]
        return trade_dates
    except mysql.connector.Error as e:
        logger.error(f"数据库查询出错: {e}")
        return []
    finally:
        # 关闭游标和数据库连接
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close() 