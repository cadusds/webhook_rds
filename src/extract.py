import pandas as pd
import os
import sys
from flask import request
from datetime import datetime, timedelta

module_root = os.path.join(os.path.dirname(__file__),'..')
sys.path.insert(0,os.path.abspath(module_root))

from hooks.tqr_hook import TqrHook
from hooks.frg_hook import FrgHook

class Extract:
    
    def __init__(self,unidade,data):
        self.data = data
        self.unidade = unidade
        self.msg_error = 'Erro - Favor ligar para o cliente!!!'

    
    def get_data(self):
        dct = {'order_id':self.data['id']}
        dct.update({'status':self.acertar_status()})
        dct.update(self.tp_retirada())
        dct.update({'link_order':f'https://{self.unidade}.reidossalgados.com/wp-admin/post.php?post=' + str(self.data['id']) + '&action=edit'})
        dct.update(self.payment())

        if self.data['payment_method'] not in ['pos_cash','pos_card']:
            dct.update(self.data_entrega())
            dct.update({'taxa_entrega':self.data['shipping_total']})
            dct.update({'motoboy':self.motoboy()})
            dct.update({'client_name':self.data['billing']['first_name'] + ' ' + self.data['billing']['last_name']})
            dct.update({'telefone':self.data['billing']['phone']})
            dct.update(self.endereco())
        else:
            dct.update({'agendado_para':datetime.now() - timedelta(hours=3)})
        return pd.DataFrame([dct])
    
    def acertar_status(self):
        status = self.data['status']
        if status == 'on-hold':
            return 'Aguardando'
        elif status == 'cancelled':
            return 'Cancelado'
        elif status == 'producao':
            return 'Producao'
        elif status == 'completed':
            return 'Concluido'
        elif status == 'processing':
            return 'Processando'
        elif status == 'enc-producao':
            return 'Enc. Producao'
        elif status == 'saiu-entrega':
            return 'Saido para entrega'
        elif status == 'entrega-concluida':
            return 'Entrega Concluída'
        else:
            return status
        
    def endereco(self):
        billing_info = self.data['billing']
        address_1 = billing_info['address_1'] + ','
        number = billing_info['number'] + ','
        address_2 = billing_info['address_2'] + ' - '
        bairro = billing_info['neighborhood'].title()
        cep = ' | cep:' + billing_info['postcode']
        if number in address_1:
            dct = {'endereco': address_1 + address_2 + bairro + cep}
        else:
            dct = {'endereco': address_1 + number + address_2 + bairro + cep}
        return dct
    
    def tp_retirada(self):
        if self.data['payment_method'] in ['pos_cash','pos_card']:
            method_id = 'Estabelecimento'
            return {'tipo_retirada':method_id}
        shipping_lines = self.data['shipping_lines']
        if len(shipping_lines) == 0:
            if self.data['billing']['phone'] is None:
                telefone = ''
            return {'tipo_retirada':self.msg_error + 'Tel: ' + telefone}
        elif len(shipping_lines) > 0:
            method_id = shipping_lines[0]['method_id']
            if  method_id == 'local_pickup':
                method_id = 'Estabelecimento'
            elif method_id == 'distance_rate':
                method_id = 'Casa do cliente'
            return {'tipo_retirada':method_id}
        
    def data_entrega(self):
        df = pd.DataFrame(self.data['meta_data'])
        delivery_time = df[df['key'] == 'delivery_time']['value'].tolist()[0]
        delivery_time = [int(i) for i in delivery_time.split(',')][0]
        delivery_date = df[df['key'] == 'delivery_date']['value'].tolist()[0]
        delivery_date = [int(i) for i in delivery_date.split(',')][0]
        agendado_para = (datetime.fromtimestamp(delivery_date) + timedelta(minutes=delivery_time))
        return {'agendado_para':agendado_para - timedelta(hours=3)}
    
    def payment (self):
        if self.data['payment_method'] == 'woo_payment_on_delivery':
            return {'payment_method':'Pagamento na hora',
                'forma_pagamento':self.forma_pagamento(),
                'created_via':self.data['created_via']}

        elif self.data['payment_method'] == '':
            return {'payment_method':None,
                'forma_pagamento':None,
                'created_via':self.data['created_via']}

        elif self.data['payment_method'] == 'pos_cash':
            return {'payment_method':'Balcão',
                'forma_pagamento':'Dinheiro',
                'created_via':self.data['created_via']}
                
        elif self.data['payment_method'] == 'pos_card':
            return {'payment_method': 'Balcão',
                'forma_pagamento':'Cartão',
                'created_via':self.data['created_via']}
        elif self.data['payment_method'] == 'pagseguro':
            return {'payment_method':'PagSeguro',
                'forma_pagamento':'Cartão',
                'created_via':self.data['created_via']}
        else:
            raise Exception(self.data['payment_method'] +' | ' + str(self.data['id']))
             
    def forma_pagamento (self):
        if self.unidade == 'freguesia':
            order_notes = FrgHook().list_order_notes(self.data['id'])
            df = pd.DataFrame(order_notes)
        elif self.unidade == 'taquara':
            order_notes = TqrHook().list_order_notes(self.data['id'])
            df = pd.DataFrame(order_notes)
        try:
            note = df[df['author'] == 'WooCommerce']['note'].tolist()
            note = [ self.filter_forma_pagamento(i) for i in note if self.filter_forma_pagamento(i) is not None]
        except:
            return order_notes
        if len(note) == 0:
            if self.data['billing']['phone'] is None:
                telefone = ''
            return self.msg_error + 'Tel: ' + telefone
        return note[0]

    def filter_forma_pagamento(self,note):
        if 'crédito' in note.lower():
            n_start = len('Enviar a Máquina de Cartão: Cartão de Crédito -> ')
            return 'Cartão de crédito: '+ note[n_start:]
        elif 'débito' in note.lower():
            n_start = len('Enviar a Máquina de Cartão: Cartão de Débito -> ')
            return 'Cartão de débito: ' + note[n_start:]
        elif 'leve troco' in note.lower():
            return 'Dinheiro: ' + note.lower().replace('\n','|')
        elif 'nenhum troco' in note.lower():
            return 'Dinheiro: ' + note.lower()
        else:
            return None
    
    def motoboy(self):
        df = pd.DataFrame(self.data['meta_data'])
        try:
            motoboy = df[df['key'] == 'retirada_ou_motoboy'].value.tolist()[0]
            return motoboy
        except:
            return ''