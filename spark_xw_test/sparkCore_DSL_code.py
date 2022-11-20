# coding=utf-8
#Date Revised    Revised by     Revision Note
#--------------- -------------- --------------------------------------------------------------------
# 20221120          xiaowen      初始版本
from pyspark.sql import SparkSession,Window
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from Spark_base import SparkBase
import os
os.environ['PYSPARK_PYTHON']= 'E:\study\pyspark_anoconda\envs\pyspark\python.exe'
spark = SparkSession.builder.appName('test').master('local[*]').getOrCreate()


if __name__ == '__main__':
    sparkobj = SparkBase.SparkSql()

    tmp1_df = sparkobj.load_data_jdbc('XW_PBCPC_DRCR_ACCT_TB')
    tmp2_df = sparkobj.load_data_jdbc('XW_PBCPC_MAIN_TB')
    # 开窗函数，其中11 和 13 三个月内算一个账户
    window = Window.partitionBy(['RPT_ID', 'BUS_MAG_INST_CD',
                                 F.when((tmp1_df['BUS_CATEG_CD'] == '11') | (tmp1_df['BUS_CATEG_CD'] == '13'), '11_13')
                                .otherwise(tmp1_df['BUS_CATEG_CD'])
                                 ]).orderBy('OPEN_DT')
    # FLAG 当前时点向后推三个月内
    tmp1_df2 = tmp1_df.filter(  (tmp1_df['ETL_JOB'] == 'PRM_PBCPC_DRCR_ACCT_TB_S75') &
                                (tmp1_df['DT'] == '20221001') &
                                (tmp1_df['IN_OTH_BNK'] == '02' )  ) \
        .select(['RPT_ID','INQD_CERT_NUM','IN_OTH_BNK','BUS_CATEG_CD',
                 'BIZ_CATEG_PRO','BUS_MAG_INST_CD','OPEN_DT',
                 'ETL_JOB','DT','PRMS_VAL_ESTI_MTGG_SPE','ACCT_NO',
                 'ACCT_TYPE_LOAN_C_CARD','ACCT_STS_CD_NEW_PF','ACCT_STS_PRO']) \
        .withColumn('FLAG',F.when(F.abs(F.months_between(to_date(tmp1_df['OPEN_DT']),
                                                  to_date(F.lead('OPEN_DT', 1, '').over(window))
                                                  )) <=3
                                  ,'Y'
                                  ).otherwise('N')
                    )
    # FLAG2 当前时点向前推三个月内
    tmp1_dfs = tmp1_df2.withColumn('FLAG2',F.when(F.abs(F.months_between(to_date(tmp1_df2['OPEN_DT']),
                                                                  to_date(F.lag('OPEN_DT', 1, '').over(window))
                                                                  )) <=3
                                                 ,'Y'
                                                ).otherwise('N')
                                   )

    # tmp1_dfs.show(1)

    window2 = Window.partitionBy(['INQD_CERT_NUM','SPOUSE_CERT_NM']).orderBy(tmp2_df.rpt_dt.desc(),tmp2_df.rpt_id.asc())
    tmp2_dfs = tmp2_df.filter( (tmp2_df['dt'] >= '20220401') &
                               (tmp2_df['dt'] <= '20221001') ).withColumn('rn',F.row_number().over(window2))\
        .select('RPT_ID','INQD_CERT_NUM','SPOUSE_CERT_NM','rn')



    tmp2_dfs.join( tmp1_dfs,[ ((tmp2_dfs.INQD_CERT_NUM == tmp1_dfs.INQD_CERT_NUM )
                               &(tmp2_dfs.RPT_ID == tmp1_dfs.RPT_ID)
                               |(tmp2_dfs.SPOUSE_CERT_NM == tmp1_dfs.INQD_CERT_NUM))
                         ,tmp1_dfs.ETL_JOB =='PRM_PBCPC_DRCR_ACCT_TB_S75'
                         ,tmp1_dfs.DT =='20221001'
                             ]
                   ,'left_outer')\
        .groupby(tmp2_dfs.RPT_ID,tmp1_dfs.BIZ_CATEG_PRO
                ,F.when((tmp1_dfs.BUS_CATEG_CD == '11')| (tmp1_dfs.BUS_CATEG_CD == '13'),'11_13')
                  .otherwise(tmp1_dfs.BUS_CATEG_CD).alias('BUS_CATEG_CD')
                ,F.when((tmp1_dfs.FLAG == 'Y')| (tmp1_dfs.FLAG2 == 'Y'),F.concat(tmp1_dfs.BUS_MAG_INST_CD,F.when((tmp1_dfs.BUS_CATEG_CD == '11')| (tmp1_dfs.BUS_CATEG_CD == '13'),'11_13')
                                                                                .otherwise(tmp1_dfs.BUS_CATEG_CD)))
                .otherwise(tmp1_dfs.ACCT_NO).alias('DISTINCT_FIELD')

                ,F.when(tmp1_dfs.RPT_ID == tmp2_dfs.RPT_ID,'Y').otherwise('N').alias('IND_FLAG'))\
        .agg(
              min(tmp1_dfs.ACCT_STS_PRO).alias('first_flag')
              ,sum(F.when( (tmp1_dfs.ACCT_TYPE_LOAN_C_CARD=='01' )
                          & (tmp1_dfs.IN_OTH_BNK =='02')
                          &( ( tmp1_dfs.BUS_CATEG_CD == '11') | (tmp1_dfs.BUS_CATEG_CD == '13'))
                        ,tmp1_dfs.PRMS_VAL_ESTI_MTGG_SPE).otherwise(0)
                  ).alias('PRMS_VAL_ESTI_MTGG_SPE')
              ,sum(F.when( (tmp1_dfs.ACCT_TYPE_LOAN_C_CARD == '01')  #贷款 #--账户类型(加工)
                            & (tmp1_dfs.IN_OTH_BNK == '02' )        ##--他行
                            & (tmp1_dfs.BUS_CATEG_CD == '12'),tmp1_dfs.PRMS_VAL_ESTI_MTGG_SPE)
                          .otherwise(0)
                 ).alias("BS_PRMS_VAL_ESTI_MTGG_SPE")                  #商业房产价值估算(房贷特有)
                ,sum(F.when( (tmp1_dfs.ACCT_TYPE_LOAN_C_CARD == '01' )     #贷款 #--账户类型(加工)
                          & (tmp1_dfs.IN_OTH_BNK == '02' )        #他行
                          & (tmp1_dfs.BIZ_CATEG_PRO =='01') , 1).otherwise(0)
                  ).alias("CAR_NUM")
              ,count(F.when(tmp1_dfs.BUS_CATEG_CD=='11', 1)).alias("IS_11_FLAG")
              ,count(F.when(tmp1_dfs.BUS_CATEG_CD!='11' ,1)).alias("IS_NOT_11_FLAG")
            ).alias('tmp1_dfs') \
        .groupby('RPT_ID') \
        .agg(
        sum(F.when( (F.col('BIZ_CATEG_PRO') =='02' )
                    & (F.col('BUS_CATEG_CD') == '11_13')
                    & (F.col('IND_FLAG') == 'Y'),F.when( (F.col('IS_11_FLAG') >F.col('IS_NOT_11_FLAG')), F.col('IS_11_FLAG'))
                    .otherwise(F.col('IS_NOT_11_FLAG'))
                    )).alias('LN_BUY_HOUS_NUM_OBANK')     ##--贷款购房套数(住房)_他行
        ,count(F.when(( F.col('BIZ_CATEG_PRO') =='02' )
                      & (F.col('BUS_CATEG_CD') == '12')
                      & (F.col('IND_FLAG') =='Y') ,1 )).alias('LN_BUY_HOUS_NUM_COM_OBANK') #--贷款购房套数(商业用房)_他行
        ,sum(F.when( (F.col('BIZ_CATEG_PRO')=='02' )
                     & (F.col('BUS_CATEG_CD') == '11_13'),F.when(  (F.col('IS_11_FLAG') >F.col('IS_NOT_11_FLAG')), F.col('IS_11_FLAG'))
                     .otherwise(F.col('IS_NOT_11_FLAG'))
                     )).alias('LN_BUY_HOUS_NUM_FMI_OBANK')        #--贷款购房套数(住房)_家庭_他行
        ,count(F.when( (F.col('BIZ_CATEG_PRO')=='02')
                       &  (F.col('BUS_CATEG_CD') == '12') , 1 )
               ).alias('LN_BUY_HOUS_NUM_COM_FMI_OBANK')    #--贷款购房套数(商业用房)_家庭_他行
        ,sum(F.when( (F.col('BIZ_CATEG_PRO')=='02' )
                     & (F.col('BUS_CATEG_CD') == '11_13')
                     & (F.col('first_flag') == '01')
                     & (F.col('IND_FLAG') == 'Y' )
                           , F.col('PRMS_VAL_ESTI_MTGG_SPE')
                     ).otherwise(0)
             ).alias('LN_BUY_HOUS_VAL_OBANK')             #--贷款购房价值(住房)_他行
        ,sum(F.when( (F.col('BIZ_CATEG_PRO')=='02')
                     & (F.col('BUS_CATEG_CD') == '12')
                     &( F.col('first_flag') == '01' )
                     & (F.col('IND_FLAG') =='Y')
                          ,  F.col('BS_PRMS_VAL_ESTI_MTGG_SPE')
                     ).otherwise(0)
             ).alias('LN_BUY_HOUS_VAL_COM_OBANK')         #--贷款购房价值(商业用房)_他行
        ,sum(F.when( (F.col('BIZ_CATEG_PRO')=='02')
                     &  (F.col('BUS_CATEG_CD') == '11_13')
                     & (F.col('first_flag') == '01'),
                         F.col('PRMS_VAL_ESTI_MTGG_SPE')
                     ).otherwise(0)
                     ).alias('LN_BUY_HOUS_VAL_FMI_OBANK')        #--贷款购房价值(住房)_家庭_他行
        ,sum(F.when( (F.col('BIZ_CATEG_PRO')=='02')
                     & (F.col('BUS_CATEG_CD') == '12')
                     & (F.col('first_flag') == '01')
                        ,F.col('BS_PRMS_VAL_ESTI_MTGG_SPE')
                    ).otherwise(0)
                     ).alias('LN_BUY_HOUS_VAL_COM_FMI_OBANK')   #--贷款购房价值(商业用房)_家庭_他行
        ,count(F.when( (F.col('BIZ_CATEG_PRO')=='01')
                       & (F.col('IND_FLAG')=='Y' ), 1 )
               ).alias('LN_BUY_CAR_NUM_OBANK')          #--贷款购车数量_他行
        ,count(F.when( F.col('BIZ_CATEG_PRO')=='01' , 1 )
               ).alias('LN_BUY_CAR_NUM_FMI_OBANK')      #--贷款购车数量_家庭_他行
    ).show()




