import sqlalchemy as sa
import pandas as pd
from datetime import datetime

class LakeHook:
    def __init__(self):
        pass
        
    def df_extract(self,sql, params={}):
        engine = sa.create_engine("databasetype://user:senha@database_endpoint:port/database_name")
        df = pd.read_sql(sql, params=params, con=engine)
        engine.dispose()
        return df

    def manipulation_query(self,sql):
        engine = sa.create_engine("databasetype://user:senha@database_endpoint:port/database_name")
        conn = engine.connect()
        conn.execute(sql)
    
    def construct_sql(self,df,table_name):
        df = self.rm_quotation_mark(df)
        if len(df) > 0:
            row_values = []
            columns = ','.join(df.columns.tolist())
            for row in list(range(len(df))):
                values = "','".join(map(str,df.iloc[row].tolist()))
                values = values.replace('NaT','null')
                values = values.replace('NaN','null')
                values = values.replace("'nat'",'null')
                values = values.replace('None','null')
                values = "('" + values + "')"
                values = values.replace('"null"', 'null')
                values = values.replace('"nan"','null')
                row_values.append(values)
            row_values = ','.join(row_values)
            request = f""" insert into {table_name} ({columns}) values {row_values}"""
            return request

    def construct_sql_log(self,df,table_name):
        df = self.rm_quotation_mark(df)
        lst_values = []
        for col in df.columns.tolist():
            value = df[col][0]
            validations = [type(value) == datetime,type(value) == str]
            if any(validations):
                value = "'" + str(value) + "'"
            elif value is None:
                value = 'null'
            lst_values.append(value)
        values = ','.join(map(str,lst_values))
        columns = ','.join(df.columns.tolist())
        sql = f"""
            insert into {table_name} ({columns}) values ({values})
        """
        return sql

    def remove_special_caracteres(self,row):
        if (pd.notnull(row)):
            row = str(row).replace('"',"")
            row = str(row).replace("'","")
            row = str(row).replace('\\','')
        return row

    def rm_quotation_mark(self,df):
        for i in df.columns.tolist():
            df[i] = df[i].apply(lambda row: self.remove_special_caracteres(row))
        return df