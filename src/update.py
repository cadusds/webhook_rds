import pandas as pd
import os
import sys
import json
from datetime import date, timedelta
from flask import request

module_root = os.path.join(os.path.dirname(__file__),'..')
sys.path.insert(0,os.path.abspath(module_root))

from hooks.lake_hook import LakeHook
from src.log import Log
from hooks.tqr_hook import TqrHook
from hooks.frg_hook import FrgHook

class Update(Log,LakeHook):
    def __init__(self,unidade,data):
        self.tqr = TqrHook()
        self.frg = FrgHook()
        self.data = data
        self.unidade = unidade
        if self.unidade == 'freguesia':
            self.table_name = 'rei_dos_salgados.frg_pedidos'
        elif self.unidade == 'taquara':
            self.table_name = 'rei_dos_salgados.tqr_pedidos'


    def att_table_pedidos(self):
        if request.method == 'POST':
            df = self.get_data()
            self.check_update_insert(df)
        dct = {'Resposta':200}
        resposta = json.dumps(dct)
        print('Pedido atualizado: ', self.data['id'])
        return resposta
    
    def check_update_insert(self,df):
        data_entrega = df.agendado_para[0]
        status = df.status[0]
        if (data_entrega == date.today() - timedelta(hours=3)) & (status == 'Aguardando'):
            order_id = df.order_id[0]
            if self.unidade == 'freguesia':
                self.frg.update_order(order_id,{'status':'enc-producao'})
            if self.unidade ==  'taquara':
                self.tqr.update_order(order_id,{'status':'enc-producao'})

        sql = f" select order_id from {self.table_name} where order_id = {df.order_id[0]}"
        df_data = self.df_extract(sql)
        if len(df_data) > 0:
            sql_delete = f"delete from {self.table_name} where order_id = {df.order_id[0]} "
            self.manipulation_query(sql_delete)

            sql_insert = self.construct_sql(df,self.table_name)
            self.manipulation_query(sql_insert)

        elif len(df_data) == 0:
            sql_insert = self.construct_sql(df,self.table_name)
            self.manipulation_query(sql_insert)
        else:
            raise Exception('Error no update')
    
    def create_log(self):
        table_log = 'rei_dos_salgados.webhook_logs'
        df = self.construct_df_log()
        sql = self.construct_sql_log(df,table_log)
        self.manipulation_query(sql)
        return json.dumps({'Resposta':200})