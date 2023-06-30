# -*- coding: utf-8 -*-

"""

This code pulls an impedence matrix and supply/demand data from a postgres database,
and runs the gravity model on the data. The output is a matrix of shipments between
trade places.

"""

#from psycopg2 import sql
import pandas as pd
from gravity_trade import GravityModel
import argparse
from dotenv import load_dotenv
import os
from concurrent.futures import ThreadPoolExecutor as Pool
#from multiprocessing import Pool
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from itertools import repeat
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm.session import close_all_sessions
import time 
import numpy as np
import sys 
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from models import Masses, Impedances

# load environment variables
load_dotenv()

db_info = os.getenv('DATABASE_URI')
engine = create_engine(db_info, echo=False)
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

# define command line arguments
parser = argparse.ArgumentParser(description='This program pulls an impedence matrix and supply/demand data from a postgres database, and runs the gravity model on the data. The output is a matrix of shipments between trade palces.')
parser.add_argument('-impedence', help="The name of the impedence that you wish to use as input to the gravity model. This parameter is required. Options include: hwyimpedence, rrimpedence, waterimpedence, and comboimpedence.", required=True)
parser.add_argument('-trade_commodity_id', help="The trade commodity index that you wish to calculate. If this parameter is not specified, the program will be ran for every commodity.", required=False)

impedence_vars = {
    'hwyimpedence': Impedances.hwyimpedence,
    'rrimpedence': Impedances.rrimpedence,
    'waterimpedence': Impedances.waterimpedence,
    'comboimpedence': Impedances.comboimpedence
}

# get impedence matrix from database
def get_impedence_matrix(impedence_var):
    session = Session()
    impedence = impedence_vars.get(impedence_var)
    if not impedence:
        print('ERROR: INVALID IMPEDENCE VARIABLE')
        exit()
    query = session.query(Impedances.fromplaceid, Impedances.toplaceid, impedence).all()
    df = pd.DataFrame(query, columns=['fromplaceid', 'toplaceid', impedence_var])
    if len(df['fromplaceid'].unique()) != len(df['toplaceid'].unique()):
        print('ERROR: FIPS MATRIX NOT SQUARE')
        exit()
    df = df.pivot_table(values=impedence_var, index='fromplaceid', columns='toplaceid')
    matrix = df.to_numpy()
    print('finished matrix')
    session.close()
    return matrix

# get supply and demand data from database
def get_supply_demand(commodity_id):
    session = Session()
    query = session.query(Masses.tradeplace_id, Masses.supply_amount, Masses.demand_amount).filter(Masses.trade_commodity_id == commodity_id).all()
    supp_demand_df = pd.DataFrame(query, columns=['tradeplace_id', 'supply_amount', 'demand_amount'])
    session.close()
    return supp_demand_df

# get list of trade commodity ids
def get_trade_commodity_id():
    session = Session()
    query = session.query(Masses.trade_commodity_id).distinct().all()
    trade_commodity_id = pd.DataFrame(query, columns=['trade_commodity_id'])['trade_commodity_id'].unique().tolist()
    session.close()
    return trade_commodity_id

# run the gravity model
def main(imp_matrix, trade_id):
    start_time = time.time()
    supp_demand_df = get_supply_demand(trade_id)
    # check that the impedence matrix and supply/demand data are the same size
    if len(supp_demand_df) != imp_matrix.shape[0] or len(supp_demand_df) != imp_matrix.shape[1]:
        print('ERROR: SUPPLY DEMAND AND IMPEDENCE MATRIX NOT SAME SIZE')
        print(len(supp_demand_df))
        print(imp_matrix.shape)
        return

    # run the gravity model found in gravity_trade.py
    gravity_model = GravityModel(imp_matrix, supp_demand_df)
    gravity_model.format_data()
    gravity_model.calculate_cost_matrix()
    gravity_model.calculate_attraction_matrices()
    gravity_model.calculate_A_and_B()
    gravity_model.calculate_prob_matrix()
    gravity_model.calculate_shipping_matrix()
    # make sure that supply and demand are balanced
    if abs(gravity_model.total_shipped_supply - gravity_model.total_shipped_demand) > 1:
        print('ERROR: SUPPLY AND DEMAND NOT BALANCED FOR: TRADE ID {0}'.format(trade_id))
    # save the output to a csv file in the output folder (this directory isn't tracked by git)
    pd.DataFrame(data=gravity_model.S).to_csv('./output/S_shipments_trip_matrix_tci_{0}.csv'.format(trade_id))
    end_time = time.time()
    print('Runtime: ', end_time - start_time, ' seconds')
    return end_time - start_time

# only run the program if this file is called directly
if __name__ == '__main__':
    # get command line arguments
    args = parser.parse_args()
    impedence_var = args.impedence
    start_time = time.time()
    imp_matrix = get_impedence_matrix(impedence_var)
    end_time = time.time()
    print('Matrix time: ', end_time - start_time, ' seconds')
    trade_comm_id_list = get_trade_commodity_id()
    if args.trade_commodity_id:
        try:
            int(args.trade_commodity_id)
        except ValueError:
            print('ERROR: INVALID TRADE COMMODITY ID')
            exit()
        if int(args.trade_commodity_id) in trade_comm_id_list:
            trade_list = [int(args.trade_commodity_id)]
        else:
            print('ERROR: INVALID TRADE COMMODITY ID')
            exit()
    else:
        trade_list = get_trade_commodity_id()
    #trade_list = [5041, 5348, 5134, 5120]
    if len(trade_list) == 1:
        process_count = 1
    else:
        process_count = 2
    with Pool(process_count) as pool:
        results = list(pool.map(main, repeat(imp_matrix), trade_list))
    average_run_time = np.mean(results)
    print('Average runtime: ', average_run_time)
    close_all_sessions()


  
