
import requests
import sqlalchemy as sa
import pandas as pd
from woocommerce.api import API
from woocommerce.oauth import OAuth
import psycopg2
import datetime


class TqrHook:
    def __init__(self):
        self.wcapi = API(
            url=  # Your store URL,
            consumer_key=  # Your consumer key,
            consumer_secret=  # Your consumer secret,
            wp_api=True,  # Enable the WP REST API integration
            version="wc/v3",  # WooCommerce WP REST API version
            timeout=10000,
            query_string_auth=True
        )
    def get_order(self,order_id):
        return self.wcapi.get(f'orders/{order_id}').json()

    def list_orders(self,npage):
        r = self.wcapi.get('orders', params = {'page':npage, 'order':'asc','orderby':'id', 'per_page':100}).json()
        return r
    
    def list_order_notes(self,order_id):
        r = self.wcapi.get(f'orders/{order_id}/notes').json()
        return r
    
    def list_products(self):
        r = self.wcapi.get('products').json()
        return r
    
    def update_order(self,order_id,data):
        return self.wcapi.put(f'orders/{order_id}',data).json()
    
    def list_products_variations(self,product_id):
        return self.wcapi.get(f'products/{product_id}/variations',params= {'per_page':100}).json()
    
    def delete_orders(self,lst_order_id):
        return self.wcapi.post(f'orders/batch',{'delete':lst_order_id}).json()
