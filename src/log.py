import pandas as pd
import os
import sys
from datetime import datetime,timedelta

module_root = os.path.join(os.path.dirname(__file__),'..')
sys.path.insert(0,os.path.abspath(module_root))

from src.extract import Extract

class Log(Extract):
    msg_error = None
    response = True
    def __init__(self):
        pass

    def construct_df_log(self):
        dct = {'order_id': self.data['id']}
        dct.update({'created_at':datetime.now() - timedelta(hours=3)})
        dct.update({'unidade':self.unidade})
        dct.update({'response':self.response})
        dct.update({'msg_error':self.msg_error})
        return pd.DataFrame([dct])
