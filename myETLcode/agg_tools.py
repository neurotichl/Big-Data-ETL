from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

def rename_col(df,rename_map):
    '''
    Rename columns of dataframe
    Parameters : - df: dataframe
                 - rename_map: dictionary of old column name(key) and new column name (value)
    Return : dataframe
    '''
    for old, new in rename_map.items():
        df = df.withColumnRenamed(old, new)
    return df

def iter_drop(df, drop_cols):
    '''
    Drop the columns from dataframe
    Parameters  : - df: dataframe
                  - drop_cols: list of columns to be dropped 
    Return : dataframe
    '''
    for d in drop_cols:
        df = df.drop(d)
    return df

def cast_schema(df,schema,time_format='dd/MM/yyyy',specify_format={}):
    '''
    Cast schema to the dataframe 
    Parameters  : - df : dataframe
                  - schema: dictionary of column name as key and datatype as value
                  - time_format: date format for all column with datatype of timestamp (default : 'dd/MM/yyyy')
                  - specify_format: date format for specific column with datatype of timestamp 
    Return : dataframe
    '''
    for i in df.columns:
        if i in schema.keys():
            if schema[i] == 'timestamp':
                if i in specify_format:
                    unix_format = specify_format[i]
                else:
                    unix_format = time_format
                df  = df.withColumn(i,unix_timestamp(i,unix_format))
            df = df.withColumn(i, col(i).cast(schema[i]))
    return df


#########################################     DECORATOR    ###########################################
global_space = dict() 
class Dispatcher:
    
    def __init__(self):
        self.space = dict()
        
    def register(self, t, func):
        self.space[t] = func
        
    def __call__(self, x, *args, **kwargs):
        try:
            return self.space[type(x)](x,*args, **kwargs)
        except KeyError:
            raise NotImplementedError("Function of type {} is not implemented".format(type(x)))
            
def dispatch(t):
    
    def wrapper(func):
        func_name = func.__name__
        if func_name not in global_space:
            global_space[func_name] = Dispatcher()
        d = global_space[func_name]
        d.register(t, func)
        return d
    
    return wrapper

        
def readin_monthly(path, table,start_month=1, end_month=12):
    '''
    Read in the table month by month to be processed 
    Parameters: - path: path to file to be read in
                - table: file name to be read in
                - start_month: starting month in integer (default:1)
                - end_month: ending month in integer (default:12)
    '''
    def wrap_month_out(func):
        def wrap_month(table_name,output_dir):
            for i in range(start_month,end_month+1):
                mo = str(i).zfill(2)
                print(mo, end=' ')
                month_df = read_parquet(spark,path,'{}_{}'.format(table,mo))
                month_df = func(month_df)
                month_df = rename_col_hdfs(month_df)
                to_parquet_hdfs(month_df,output_dir,table_name+'_'+mo)
            print('\n')
        return wrap_month
    return wrap_month_out
    
#########################################     PRIVATE    ###########################################    
def _agg_type(c,t):
    '''
    Return aggregation object according to the type
    Parameters : -c : column
                 -t : type (sum, count, min, max, mean, countDistinct)
    Return : type_of_aggregation(col)
    '''
    if t == 'sum':
        return sum(c)
    elif t == 'count':
        return count(c)
    elif t == 'min':
        return min(c)
    elif t == 'max':
        return max(c)
    elif t == 'mean':
        return avg(c)
    elif t == 'COL':
        return col(c)
    elif t == 'countDistinct':
        return countDistinct(c)

def _select_alias(rename_dict):
    '''
    Return a list of selected columns with alias new name
    Parameters  : - rename_dict : a dictionary of column rename
    Return : [col(OLD_NAME).alias(NEW_NAME) ...] {OLD_NAME: NEW_NAME , ... }
    -------------------
    From: select_join()
    '''
    alias_list = []
    for k,v in rename_dict.items():
        alias_list.extend(_match_agg_type({v:'COL'},k))
    return alias_list

@dispatch(dict)
def _match_agg_type(agg_opt,c): #agg_opt come first due to the dispatcher
    return [_agg_type(c,t).alias(a) for a, t in agg_opt.items()]

@dispatch(str)        
def _match_agg_type(agg_opt,c):
    return [_agg_type(c,agg_opt).alias(c)]

########################################       PUBLIC      ######################################

def select_join(sel='*', rj=None,**kwargs):
    '''
    Select the columns for joining based on selected column and renaming dictionary
    Parameters  : - sel : selected columns [list]
                  - rj : rename columns [dict]
    Return : - renamed sel 
    ---------------------
    Dependencies: _select_alias(rj)
    '''
    if not rj:
        return sel
    elif sel == '*' and rj:
        return _select_alias(rj)
    else:
        return _select_alias(rj) + [r for r in sel if r not in rj]
    
def join_alias(jo, rj=None,**kwargs):
    '''
    Join the columns 
    Parameters  : - jo = join-on columns [str, list or dict]
                  - rj = rename columns [dict]
    Return : Dictionary of left join columns as key and right join columns as value
    
    If the join-on column for right join table is in the rename dictionary, the right table column will be renamed.
    '''
    if not rj or isinstance(jo, list) or isinstance(jo, str):
        return jo
    for k,v in jo.items():
        if v in rj:
            jo[k] = rj[v]
    return jo

@dispatch(str)
def joining(x,df1,df2,how='left'):
    '''
    Join two dataframes on ONE SAME column
    
    joining(df1 = A, 
            df2 = B.select(*select_join(**kwargs)),
            x   = join_alias(**kwargs))
    '''
    return df1.join(df2, on=x,how=how)

@dispatch(list)
def joining(x,df1,df2,how='left'):
    '''
    Join two dataframe on a list of columns
    
    joining(df1 = A, 
            df2 = B.select(*select_join(**kwargs)),
            x   = join_alias(**kwargs))
    '''
    return df1.join(df2, on=x,how=how)

@dispatch(dict)
def joining(x,df1,df2,how='left',drop=True):
    '''
    Join two dataframe w.r.t left and right table's columns given in a dictionary
    - Perform renaming when same column name detected in the dictionary
    - Perform renaming when the column name in right table found in left table
    Drop : dropping the right table columns (optional)
     
    joining(df1  = A, 
            df2  = B.select(*select_join(**kwargs)),
            x    = join_alias(**kwargs),
            drop = False)
    '''
    join_lr = []
    for k,v in x.items() :
        if (k == v) or (k != v and v in df1.columns):
            x[k] = v+'_'
            df2 = df2.withColumnRenamed(v,v+'_')   
    j_df = df1.join(df2, on=[df1[l]==df2[r] for l,r in x.items()],how=how)
    if drop == True: return iter_drop(j_df, x.values())
    else: return j_df

def aggregation(df, group_col, agg_cols):
    '''
    Perform aggregation on the dataframe
    Parameters  : - df: dataframe
                  - group_col: groupby column [list]
                  - agg_cols: aggregation options [dict/nested dict(with new columns name)]
          e.g. 
              {'amount':'sum'}
              or
              {'amount':{'Total Amount':'sum',
                         'Minimum Amount':'min'}}
    Return : dataframe
    '''
    agg_list = []
    for c, agg_opt in agg_cols.items():
        agg_list.extend(_match_agg_type(agg_opt,c))
    
    return df.groupBy(group_col).agg(*agg_list)

def recursive_cond(df, conditions,NA ='NA'):
    '''
    Return when().otherwise() for multiple if-else
    
    '''
    if not conditions:
        return NA
    cond = conditions.pop(0)
    else_value = recursive_cond(df, conditions,NA)
    return when(cond[0](df),cond[1]).otherwise(else_value)



