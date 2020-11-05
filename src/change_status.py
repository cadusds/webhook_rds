import pandas as pd
import os
import sys
from datetime import datetime, timedelta
from time import sleep

module_root = os.path.join(os.path.dirname(__file__),'..')
sys.path.insert(0,os.path.abspath(module_root))

from src.update import Update
from hooks.tqr_hook import TqrHook
from hooks.lake_hook import LakeHook
from hooks.frg_hook import FrgHook

db = LakeHook()
tqr = TqrHook()
frg = FrgHook()

tabelas_unidades = ['frg_pedidos','tqr_pedidos']
for table in tabelas_unidades:
    request = f"""
        select
            order_id, agendado_para,status
        from rei_dos_salgados.{table}
        where agendado_para::date = (current_date - interval '3 hours')::date
        and status =  'Aguardando'
    """
    df = db.df_extract(request)
    print(datetime.now() - timedelta(hours=3),'Total de pedidos atualizados para a unidade da', 'Freguesia:' if table == 'frg_pedidos' else 'Taquara:',
    len(df))
    if len(df) > 0:
        list_orders = df.order_id.tolist()
        for order in list_orders:
            if table == 'frg_pedidos':
                frg.update_order(order,{'status':'enc-producao'})
            elif table == 'tqr_pedidos':
                tqr.update_order(order,{'status':'enc-producao'})
            sleep(3)
