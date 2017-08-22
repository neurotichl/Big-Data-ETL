from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

def to_csv_hdfs(df,path,table_name,mode='overwrite'):
    '''
    Write to HDFS in csv
    Parameters: - df: dataframe
                - path: output path 
                - table_name: output file name
                - mode: writing mode (default:'overwrite')
    '''
    df.write.mode(mode).csv(path+table_name,header=True)
    
def to_parquet_hdfs(df,path,table_name,mode='overwrite'):
    '''
    Write to HDFS in parquet
    Parameters: - df: dataframe
                - path: output path 
                - table_name: output file name
                - mode: writing mode (default:'overwrite')
    '''
    df.write.mode(mode).parquet(path+table_name)

def to_postgres(df,postgres_add,db_name,table_name,username,password,mode='append'):
    '''
    Write to POSTGRESQL in parquet
    Parameters: - df: dataframe
                - postgres_add: postgresql IP
                - db_name: database name
                - table_name: output file name
                - username: username
                - password: password
                - mode: writing mode (default:'append')
    '''
    df.write.jdbc(url='jdbc:postgresql://{}/{}'.format(postgres_add, db_name), 
                  table="{}".format(table_name),  
                  mode="{}".format(mode),
                  properties={'user':'{}'.format(username),
                              'password':'{}'.format(password),
                              'driver': "org.postgresql.Driver"})
