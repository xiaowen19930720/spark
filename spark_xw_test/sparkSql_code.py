# coding=utf-8
#Date Revised    Revised by     Revision Note
#--------------- -------------- --------------------------------------------------------------------
# 20221101          xiaowen      初始版本

from Spark_base import SparkBase
import os
os.environ['PYSPARK_PYTHON']= 'E:\study\pyspark_anoconda\envs\pyspark\python.exe'

if __name__ == '__main__':
    sparkobj = SparkBase.SparkSql()
    '''
        从本地mysql取数创建临时表，代替从hadoop集群上取表数据 的过程
    '''
    #借贷信息账户信息表
    sparkobj.load_data_jdbc('XW_PBCPC_DRCR_ACCT_TB').createGlobalTempView('XW_PBCPC_DRCR_ACCT_TB')
    #公积金表
    sparkobj.load_data_jdbc('XW_PBCPC_HOUSE_HSFD_RECD').createGlobalTempView('XW_PBCPC_HOUSE_HSFD_RECD')
    #人行征信主表2.0
    sparkobj.load_data_jdbc('XW_PBCPC_MAIN_TB').createGlobalTempView('XW_PBCPC_MAIN_TB')
        # sparkobj.spark.sql("""
        # select * from global_temp.XW_PBCPC_DRCR_ACCT_TB where rpt_id = '2022100107313285887695'
        #                                                   and nvl(recycal_loan_id,'') = ''
        # """).show()  #just for test

    '''  ***STEP1***
       1.只要他行数据
       2.LEAD，LAG上下偏移函数，判断每条数据 ’开立日期为3个月内‘ 
        业务代码 11-个人住房商业贷款  12-个人商用房（含商住两用）贷款 13-个人住房公积金贷款  
        合并11和13为一个账户
    '''
    sparkobj.spark.sql('''SELECT 
           T.RPT_ID,
           T.INQD_CERT_NUM,
           T.IN_OTH_BNK,
           T.BUS_CATEG_CD,
           T.BIZ_CATEG_PRO ,
           T.BUS_MAG_INST_CD,
           T.OPEN_DT,
           T.ETL_JOB,
           T.DT,
           T.PRMS_VAL_ESTI_MTGG_SPE,
           T.ACCT_NO,
           T.ACCT_TYPE_LOAN_C_CARD,
           T.ACCT_STS_CD_NEW_PF,
           t.ACCT_STS_PRO,
           CASE WHEN 
            ABS(  MONTHS_BETWEEN(CAST(T.OPEN_DT AS TIMESTAMP),CAST(LEAD(T.OPEN_DT,1,NULL) OVER(PARTITION BY T.RPT_ID,T.BUS_MAG_INST_CD,CASE WHEN T.BUS_CATEG_CD = '11' OR T.BUS_CATEG_CD = '13'
                                                                       THEN '11_13' ELSE T.BUS_CATEG_CD 
                                                                  END ORDER BY T.OPEN_DT ) AS TIMESTAMP)) ) <=3 THEN 'Y' ELSE 'N' END AS FLAG,
           CASE WHEN 
            ABS(  MONTHS_BETWEEN(CAST(T.OPEN_DT AS TIMESTAMP),CAST(LAG(T.OPEN_DT,1,NULL) OVER(PARTITION BY T.RPT_ID,T.BUS_MAG_INST_CD,CASE WHEN T.BUS_CATEG_CD = '11' OR T.BUS_CATEG_CD = '13'
                                                                       THEN '11_13' ELSE T.BUS_CATEG_CD 
                                                                  END ORDER BY T.OPEN_DT ) AS TIMESTAMP)) ) <=3 THEN 'Y' ELSE 'N' END AS FLAG2
          FROM global_temp.XW_PBCPC_DRCR_ACCT_TB T  --借贷信息账户信息表
          WHERE  T.ETL_JOB='PRM_PBCPC_DRCR_ACCT_TB_S75'
          AND  T.DT='20221001'  
          AND T.IN_OTH_BNK = '02'  --他行数据
                    ''').createGlobalTempView('tmp1')

    '''   
          *** STEP2 ***
          取出人行征信半年内，对应个人最新的数据（一个人可能查几次征信）
          ps: impala和spark对于日期处理不同 
          原impala语法   DT>=REGEXP_REPLACE(CAST(MONTHS_add(CONCAT(SUBSTR('20221001',1,4),'-',SUBSTR('20221001',5,2),'-',SUBSTR('20221001',7,2)),-6) 
				                       AS VARCHAR ),'-| .*','')
    '''
    sparkobj.spark.sql(''' 
          SELECT T.RPT_ID
                 ,T.INQD_CERT_NUM    --证件号
                 ,T.SPOUSE_CERT_NM   --配偶证件号
                 ,ROW_NUMBER() OVER(PARTITION BY T.INQD_CERT_NUM,T.SPOUSE_CERT_NM ORDER BY T.RPT_DT DESC,T.RPT_ID ASC ) RN
                 FROM global_temp.XW_PBCPC_MAIN_TB T       --人行征信2.0主表 增量表
                WHERE ETL_JOB = 'CDA_PBCPC_MAIN_TB_S75' 
				AND DT >= date_format(add_months( to_date('20221001','yyyyMMdd') ,-6 ),'yyyyMMdd')    --半年内       
                AND DT <= '20221001'
                    ''').createGlobalTempView('tmp2')

    '''   
      *** STEP3 ***  
      计算房贷相关标签
      1)DISTINCT_FIELD+ MIN(t.ACCT_STS_PRO) --> 账户状态(加工)为未结清（01最小） (若开立日期为3个月内，且业务管理机构代码相同，且业务种类分别为11和13，
      算一个账户，其中一笔未结清，两笔均为未结清)  且业务种类为11/13 
      2)IS_11_FLAG & IS_NOT_11_FLAG -->  存在普通贷 和公积金贷两种情况，存在交叉重算，故用其区分取其中一个
      3)IND_FLAG --> Y用来计算本人的，Y+N用来计算本人及其配偶的
    '''
    sparkobj.spark.sql('''  
    SELECT 
      T1.RPT_ID AS RPT_ID ,
      SUM(CASE WHEN T1.BIZ_CATEG_PRO='02'  AND T1.BUS_CATEG_CD = '11_13' 
                                           AND T1.IND_FLAG='Y' 
               THEN  CASE WHEN  T1.IS_11_FLAG >T1.IS_NOT_11_FLAG 
                          THEN T1.IS_11_FLAG
                          ELSE T1.IS_NOT_11_FLAG
                     END
          END)              AS LN_BUY_HOUS_NUM_OBANK    ,--贷款购房套数(住房)_他行
      COUNT(CASE WHEN T1.BIZ_CATEG_PRO='02'  AND T1.BUS_CATEG_CD = '12'    
                                             AND T1.IND_FLAG='Y' THEN  1 
           END)             AS LN_BUY_HOUS_NUM_COM_OBANK,--贷款购房套数(商业用房)_他行
      SUM(CASE WHEN T1.BIZ_CATEG_PRO='02'  AND T1.BUS_CATEG_CD = '11_13' 
               THEN   CASE WHEN  T1.IS_11_FLAG >T1.IS_NOT_11_FLAG 
                           THEN T1.IS_11_FLAG
                           ELSE T1.IS_NOT_11_FLAG
                       END
          END)              AS LN_BUY_HOUS_NUM_FMI_OBANK    ,--贷款购房套数(住房)_家庭_他行
	  COUNT(CASE WHEN T1.BIZ_CATEG_PRO='02' AND  T1.BUS_CATEG_CD = '12'   
	             THEN  1 
	        END)            AS LN_BUY_HOUS_NUM_COM_FMI_OBANK,--贷款购房套数(商业用房)_家庭_他行                             
      SUM(CASE WHEN T1.BIZ_CATEG_PRO='02' AND T1.BUS_CATEG_CD = '11_13'  
                                          AND first_flag = '01' 
                                          AND T1.IND_FLAG='Y' 
               THEN NVL(T1.PRMS_VAL_ESTI_MTGG_SPE,0) 
               ELSE 0 
          END)              AS LN_BUY_HOUS_VAL_OBANK    ,--贷款购房价值(住房)_他行
      SUM(CASE WHEN T1.BIZ_CATEG_PRO='02' AND T1.BUS_CATEG_CD = '12'     
                                          AND first_flag = '01' 
                                          AND T1.IND_FLAG='Y' 
               THEN  NVL(T1.BS_PRMS_VAL_ESTI_MTGG_SPE,0) 
               ELSE 0 
         END)               AS LN_BUY_HOUS_VAL_COM_OBANK,--贷款购房价值(商业用房)_他行
      SUM(CASE WHEN T1.BIZ_CATEG_PRO='02' AND  T1.BUS_CATEG_CD = '11_13' 
                                          AND first_flag = '01' 
               THEN NVL(T1.PRMS_VAL_ESTI_MTGG_SPE,0) 
               ELSE 0 
          END)              AS LN_BUY_HOUS_VAL_FMI_OBANK     ,--贷款购房价值(住房)_家庭_他行
      SUM(CASE WHEN T1.BIZ_CATEG_PRO='02' AND  T1.BUS_CATEG_CD = '12'    
                                          AND first_flag = '01' 
               THEN  NVL(T1.BS_PRMS_VAL_ESTI_MTGG_SPE,0) 
               ELSE 0
           END)              AS LN_BUY_HOUS_VAL_COM_FMI_OBANK ,--贷款购房价值(商业用房)_家庭_他行
      COUNT(CASE WHEN T1.BIZ_CATEG_PRO='01' AND T1.IND_FLAG='Y'  
                 THEN 1 
           END)              AS LN_BUY_CAR_NUM_OBANK          ,--贷款购车数量_他行
      COUNT(CASE WHEN T1.BIZ_CATEG_PRO='01'  THEN 1 
             END)            AS LN_BUY_CAR_NUM_FMI_OBANK       --贷款购车数量_家庭_他行
                  
          FROM     (  SELECT 
                                MAIN.RPT_ID           AS RPT_ID         , --贷款人报告号
                                T.BIZ_CATEG_PRO       AS BIZ_CATEG_PRO  , --业务种类(加工) 01车贷 02房贷
                                CASE WHEN T.BUS_CATEG_CD = '11' OR T.BUS_CATEG_CD = '13'
                                     THEN '11_13' 
                                     ELSE T.BUS_CATEG_CD
                                END                   AS BUS_CATEG_CD   ,--合并普通房贷与公积金贷
                                CASE WHEN T.FLAG = 'Y' OR T.FLAG2 = 'Y'
                                     THEN CONCAT(T.BUS_MAG_INST_CD , CASE WHEN T.BUS_CATEG_CD = '11' OR T.BUS_CATEG_CD = '13'
                                                                          THEN '11_13' ELSE T.BUS_CATEG_CD 
                                                                      END
                                                 )
                                      ELSE T.ACCT_NO    
                                 END                      AS   DISTINCT_FIELD,  --去重字段
                                MIN(t.ACCT_STS_PRO)       as   first_flag,
                                SUM(CASE WHEN T.ACCT_TYPE_LOAN_C_CARD = '01'  --贷款 --账户类型(加工)  
                                              AND T.IN_OTH_BNK = '02'         --他行
                                              AND (T.BUS_CATEG_CD = '11' OR T.BUS_CATEG_CD = '13')
                                         THEN T.PRMS_VAL_ESTI_MTGG_SPE 
                                         ELSE 0 
                                    END  
                                    )                     AS  PRMS_VAL_ESTI_MTGG_SPE ,--房产价值估算(房贷特有)
                            
                                SUM(CASE WHEN T.ACCT_TYPE_LOAN_C_CARD = '01'  --贷款 --账户类型(加工)  
                                              AND T.IN_OTH_BNK = '02'         --他行
                                              AND T.BUS_CATEG_CD = '12' 
                                         THEN T.PRMS_VAL_ESTI_MTGG_SPE ELSE 0 END  
                                   )                  AS  BS_PRMS_VAL_ESTI_MTGG_SPE ,--商业房产价值估算(房贷特有)
                                SUM(CASE WHEN T.ACCT_TYPE_LOAN_C_CARD = '01'  --贷款 --账户类型(加工)  
                                              AND T.IN_OTH_BNK = '02'         --他行
                                              AND T.BIZ_CATEG_PRO ='01' THEN 1 ELSE 0 END
                                              )      AS CAR_NUM  ,
                                CASE WHEN T.RPT_ID = MAIN.RPT_ID THEN 'Y' ELSE 'N' END    AS IND_FLAG   ,
                                COUNT(CASE WHEN T.BUS_CATEG_CD='11' THEN 1 END)  AS IS_11_FLAG,
                                COUNT(CASE WHEN T.BUS_CATEG_CD<>'11' THEN 1 END)  AS IS_NOT_11_FLAG
                          FROM  global_temp.tmp2  MAIN
                          LEFT JOIN global_temp.tmp1 T
                                 ON   (MAIN.INQD_CERT_NUM       = T.INQD_CERT_NUM AND MAIN.RPT_ID=T.RPT_ID) -- 贷款人
                                 OR   MAIN.SPOUSE_CERT_NM      = T.INQD_CERT_NUM  -- 贷款人配偶
                                AND  T.ETL_JOB='PRM_PBCPC_DRCR_ACCT_TB_S75'
                                AND  T.DT='20221001'
                          WHERE MAIN.RN=1       --取最新一条
                           GROUP BY 
                                     MAIN.RPT_ID          ,
                                    T.BIZ_CATEG_PRO      ,
                                    CASE WHEN T.BUS_CATEG_CD = '11' OR T.BUS_CATEG_CD = '13'
                                         THEN '11_13' ELSE T.BUS_CATEG_CD
                                    END                 ,
                                    CASE WHEN T.FLAG = 'Y' OR T.FLAG2 = 'Y'
                                         THEN CONCAT(T.BUS_MAG_INST_CD , CASE WHEN T.BUS_CATEG_CD = '11' OR T.BUS_CATEG_CD = '13'
                                                                           THEN '11_13' ELSE T.BUS_CATEG_CD 
                                                                      END)
                                         ELSE T.ACCT_NO    
                                    END   ,
                                          CASE WHEN T.RPT_ID = MAIN.RPT_ID THEN 'Y' ELSE 'N' END                   
             ) T1
         GROUP BY T1.RPT_ID
                        ''').show()

