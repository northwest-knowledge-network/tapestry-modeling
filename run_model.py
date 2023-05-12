# -*- coding: utf-8 -*-

"""

@author: Tanner Varrelman tvarrelman@uidaho.edu

This code pulls an impedance matrix and supply/demand data from a postgres database,
and runs the gravity model on the data. The output is a matrix of shipments between
trade places.

"""

import psycopg2
from psycopg2 import sql
import numpy as np
import pandas as pd
from gravity_trade import GravityModel
import argparse
from dotenv import load_dotenv
import os

load_dotenv()

def create_connection():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port = os.getenv("DB_PORT"),
        database=os.getenv("DB"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS")
    )
    return conn

def get_impedance_matrix(cursor, impedance_var):
    query = sql.SQL("SELECT fromplaceid, toplaceid, {} FROM us2012.fact_impedances_4135;")
    cursor.execute(query.format(sql.Identifier(impedance_var)))
    df = pd.DataFrame(cur.fetchall(), columns=['fromplaceid', 'toplaceid', impedance_var])
    if len(df['fromplaceid'].unique()) != len(df['toplaceid'].unique()):
        print('ERROR: FIPS MATRIX NOT SQUARE')
        exit()
    df = df.pivot_table(values='hwyimpedence', index='fromplaceid', columns='toplaceid')
    matrix = df.to_numpy()
    return matrix

def get_supply_demand(cursor, commodity_id):
    query = sql.SQL("SELECT tradeplace_id, supply_amount, demand_amount FROM us2012.fact_masses_409x4135 WHERE trade_commodity_id = {};")
    cursor.execute(query.format(sql.Literal(commodity_id)))
    supp_demand_df = pd.DataFrame(cur.fetchall(), columns=['tradeplace_id', 'supply_amount', 'demand_amount'])
    return supp_demand_df

if __name__ == '__main__':
    conn = create_connection()
    cur = conn.cursor()
    imp_mat = get_impedance_matrix(cur, 'hwyimpedence')
    supp_demand_df = get_supply_demand(cur, 5030)
    conn.close()
    conn = None
    if len(supp_demand_df) != imp_mat.shape[0] or len(supp_demand_df) != imp_mat.shape[1]:
        print('ERROR: SUPPLY DEMAND AND IMPEDANCE MATRIX NOT SAME SIZE')
        exit()
    gravity_model = GravityModel(imp_mat, supp_demand_df)
    gravity_model.format_data()
    gravity_model.calculate_cost_matrix()
    gravity_model.calculate_attraction_matrices()
    gravity_model.calculate_A_and_B()
    gravity_model.calculate_prob_matrix()
    gravity_model.calculate_shipping_matrix()
    print('TOTAL SHIPPED SUPPLY: ', gravity_model.total_shipped_supply)
    print('TOTAL SHIPPED DEMAND: ', gravity_model.total_shipped_demand)
    pd.DataFrame(data=gravity_model.S).to_csv('S_shipments_trip_matrix_v2.csv')
