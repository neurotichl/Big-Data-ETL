
from configparser import RawConfigParser

def setspark(sp):
    '''
    Pass in spark context and set it as global variable
    '''
    global spark
    spark = sp

class ReadConf:
    '''
    Read the config file from the path and parse into a dictionary
  
       myconf = ReadConf('path/config.ini')
       myconf.KEY
       myconf('KEY')
    '''
    def __init__(self,path):
        confs = self._readconf(path)
        for c in confs:
            if c == 'DEFAULT': continue
            setattr(self, c, dict(confs[c]))
            
    def _readconf(self,path):
        confs = RawConfigParser()
        confs.optionxform = str
        read = confs.read(path)
        
        if read == []:
            raise IOError('Config not read. Please check path.')
        else:
            return confs
    
    def __call__(self,name):
        return getattr(self,name)

def read_csv(path,fname,sep=',',inferSchema=False,**kwargs):
    '''
    Read csv file(s)
    Parameters: - path : path of the file to be read
                - fname: filename
                - sep  : seperator (default: ',')
                - inferSchema: infer file schema (default:False)
                - kwargs : keyword arguments. Refer to spark 2 read.csv documentation
    Return    : Dataframe
    '''
    return spark.read.csv(path+fname,header=True,sep=sep,inferSchema=inferSchema,**kwargs)

def read_parquet(path, fname):
    '''
    Read parquet file(s)
    Parameters: - path : path of the file to be read
                - fname: filename
    Return    : Dataframe
    '''
    return spark.read.parquet(path+fname)

def data_profile(df,sc):
    data_details = {}
    for i in df.schema:
        col_name = i.name
        datatype = i.simpleString().split(':')[1]
        distinct_len = df.select(col_name).distinct().count() 
        if datatype == 'string':
            desc = [df.select(col_name).count() ,'','','','']
        elif datatype == 'timestamp':
            desc = [df.select(col_name).count() ,'','','','']
        else:
            desc = df.describe(col_name).rdd.map(lambda x: x[1]).collect()
        count_null = df.agg(sum(df[col_name].isNull().cast('integer')).alias(col_name)).rdd.map(lambda x: x[0]).collect()
        data_details[col_name] =[datatype] + [distinct_len] + desc + count_null

    column = ['column','datatype','dist len','count','mean','s.d.','min','max','null']
    df_summary = sc.parallelize([k]+v for k,v in data_details.items()).toDF(column)
    return df_summary

def check_null(df, filter_col, show_cols = None, t = 'show'):
    if t == 'show':
        df.filter(col(filter_col).isNull()).select(*show_cols).distinct().show()
    elif t == 'count':
        df.agg(countDistinct(col(filter_col).isNull()).alias('null {} count'.format(filter_col))).show()
        
def check_duplicate_col(col):
    '''
    Check if duplicated column name exist
    Parameters: col: dataframe's column
    Return : list of column name(s) that is duplicated
    '''
    a = []
    b = []
    for c in col:
        if c not in a:
            a.append(c)
        else:
            b.append(c)
    return b

def compare_col_schema(df,schema_col):
    df_col = set(df.columns)
    schema_col = set(schema_col.keys())
    a = len(df_col)
    b = len(schema_col)
    c = len(df_col.intersection(schema_col))
    d = len(schema_col.difference(df_col))
    e = df_col.difference(schema_col)
    if (a == c) & (a == b):
        print('Matched {} columns perfectly'.format(a))
       
    else:
        print('''
        Dataframe Col No.: {0}
        Schema Col No.   : {1}
        Intersected      : {2}
        =======================
        {3} Extras in Schema
        Not in Schema    = {4}
        '''.format(a,b,c,d,e))
