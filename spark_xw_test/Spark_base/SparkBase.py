# coding=utf-8
#Date Revised    Revised by     Revision Note
#--------------- -------------- --------------------------------------------------------------------
# 20221101          xiaowen      初始版本

from pyspark.sql import SparkSession
from settings.Enviroment1 import MYSQL_CONF,SPARK_CONF
import pandas
'''
笔记本建的三台虚拟机内存不够，故不用hdfs+spark集群跑 用mysql代替数据环境
数据为脱敏测试数据
'''
class SparkBase():

    '''
        测试数据不用hdfs，用mysql代替
    '''
    # def read_jdbc(self):
    #     raise NotImplementedError
    def init_spark(self):
        raise NotImplementedError


class SparkSql(SparkBase):

    def __init__(self):
        self.spark = None
        self.jdbc = None

        self.init_spark()
        self.init_jdbc()

    def init_spark(self):
        '''
        初始化spark
        :return:
        '''
        self.spark = SparkSession.builder \
            .appName(SPARK_CONF['app_name']) \
            .master(SPARK_CONF['master']) \
            .getOrCreate()


    def init_jdbc(self):
        '''
        初始化jdbc链接
        :return:
        '''
        jdbc_url = 'jdbc:mysql://{0}:{1}/{2}'.format(
            MYSQL_CONF['host'],
            MYSQL_CONF['port'],
            MYSQL_CONF['db']
        )
        self.spark.read.format('jdbc')\
            .option('url',jdbc_url)\
            .option('user',MYSQL_CONF['user'])\
            .option('passwrod',MYSQL_CONF['password'])


    def load_data_pandas(self,path,sheet_name,schema):
        '''
        :param path:  数据文件路径(excel)
        :param sheet_name: 数据文件页签
        :param schema: 数据存储格式
        :return: rdd dataframe
        '''
        pdf =  pandas.read_excel(path,sheet_name,dtype=str)
        pdf.fillna('',inplace=True)   #NaN数据
        return self.spark.createDataFrame(pdf,schema=schema)

    def load_data_jdbc(self,table_name):
        '''
        返回mysql数据库数据
        :param table_name: 查询表名
        :return:
        '''
        url = 'jdbc:mysql://' + MYSQL_CONF['host']+':'+str(MYSQL_CONF['port']) \
              +'/'+ MYSQL_CONF['db']+'?useSSL=false&Unicode=true&serverTimezone=UTC'
        return self.spark.read.format('jdbc') \
            .option('url',url) \
            .option('dbtable',table_name) \
            .option('user',MYSQL_CONF['user']) \
            .option('password',MYSQL_CONF['password']).load()


    def write_to_jdbc(self,df,table_name):
        '''
        加载数据文件至mysql
        :param df: 读取数据文件数据
        :param table_name: mysql表名
        :return:
        '''
        url = 'jdbc:mysql://' + MYSQL_CONF['host']+':'+str(MYSQL_CONF['port'])\
              +'/'+ MYSQL_CONF['db']+'?useSSL=false&Unicode=true&serverTimezone=UTC'

        df.write.mode('overwrite').format('jdbc') \
            .option('url',url) \
            .option('dbtable',table_name) \
            .option('user',MYSQL_CONF['user']) \
            .option('password',MYSQL_CONF['password']) \
            .save()
