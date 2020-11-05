import os
import sys
from flask import Flask,render_template,request

module_root = os.path.join(os.path.dirname(__file__),'..')
sys.path.insert(0,os.path.abspath(module_root))

from src.update import Update


app =  Flask(__name__)

app.secret_key = 'yourappsecretkey'

@app.route("/")
def index():
    return render_template('home.html')

@app.route('/taquara', methods=['POST'])
def tqr_order():
    unidade = 'taquara'
    data = request.get_json()
    update_order = Update(unidade,data)
    try:
        update_order.att_table_pedidos()
        return update_order.create_log()
    except Exception as error:
        update_order.response = False
        update_order.msg_error = error
        return update_order.create_log()


@app.route('/freguesia',methods=['POST'])
def frg_order():
    unidade = 'freguesia'
    data = request.get_json()
    update_order = Update(unidade,data)
    try:
        update_order.att_table_pedidos()
        return update_order.create_log()
    except Exception as error:
        update_order.response = False
        update_order.msg_error = error
        return update_order.create_log()