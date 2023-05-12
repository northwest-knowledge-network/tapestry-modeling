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
from concurrent.futures import ProcessPoolExecutor as Pool

# load environment variables
load_dotenv()

# define command line arguments
parser = argparse.ArgumentParser(description='This program pulls an impedance matrix and supply/demand data from a postgres database, and runs the gravity model on the data. The output is a matrix of shipments between trade palces.')
parser.add_argument('-impedance', help="The name of the impedance that you wish to use as input to the gravity model. This parameter is required.", required=True)
parser.add_argument('-trade_commodity_id', help="The trade commodity index that you wish to calculate. If this parameter is not specified, the program will be ran for every commodity", required=False)

# establish connection to database
def create_connection():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port = os.getenv("DB_PORT"),
        database=os.getenv("DB"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS")
    )
    return conn

# get impedance matrix from database
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

# get supply and demand data from database
def get_supply_demand(cursor, commodity_id):
    query = sql.SQL("SELECT tradeplace_id, supply_amount, demand_amount FROM us2012.fact_masses_409x4135 WHERE trade_commodity_id = {};")
    cursor.execute(query.format(sql.Literal(commodity_id)))
    supp_demand_df = pd.DataFrame(cur.fetchall(), columns=['tradeplace_id', 'supply_amount', 'demand_amount'])
    return supp_demand_df

# get list of trade commodity ids
def get_trade_commodity_id(cursor):
    query = sql.SQL("SELECT DISTINCT trade_commodity_id FROM us2012.fact_masses_409x4135;")
    cursor.execute(query)
    trade_commodity_id = pd.DataFrame(cur.fetchall(), columns=['trade_commodity_id'])['trade_commodity_id'].unique().tolist()
    return trade_commodity_id

# run the gravity model
def main(trade_id):
    supp_demand_df = get_supply_demand(cur, trade_id)
    if len(supp_demand_df) != imp_matrix.shape[0] or len(supp_demand_df) != imp_matrix.shape[1]:
        print('ERROR: SUPPLY DEMAND AND IMPEDANCE MATRIX NOT SAME SIZE')
        print(len(supp_demand_df))
        print(imp_matrix.shape)
        return

    gravity_model = GravityModel(imp_matrix, supp_demand_df)
    gravity_model.format_data()
    gravity_model.calculate_cost_matrix()
    gravity_model.calculate_attraction_matrices()
    gravity_model.calculate_A_and_B()
    gravity_model.calculate_prob_matrix()
    gravity_model.calculate_shipping_matrix()
    print()
    print('TOTAL SHIPPED SUPPLY: ', gravity_model.total_shipped_supply)
    print('TOTAL SHIPPED DEMAND: ', gravity_model.total_shipped_demand)
    pd.DataFrame(data=gravity_model.S).to_csv('/Users/tvarrelman/Documents/gravity_model/example_data/example_output/S_shipments_trip_matrix_{0}.csv'.format(trade_id))
    conn.close()

if __name__ == '__main__':
    # get command line arguments
    args = parser.parse_args()
    impedance_var = args.impedance
    conn = create_connection()
    cur = conn.cursor()
    global imp_matrix
    imp_matrix = get_impedance_matrix(cur, impedance_var)
    #trade_list = get_trade_commodity_id(cur)
    #trade_list = [5030]
    trade_list = [5041, 5348, 5134, 5120]
    #print(imp_matrix.shape)
    with Pool(max_workers = 4) as pool:
        pool.map(main, trade_list)
    #for trade_id in trade_list:
    #    main(trade_id)

    conn.close()