# -*- coding: utf-8 -*-

"""

@author: Tanner Varrelman tvarrelman@uidaho.edu

This code pulls an impedence matrix and supply/demand data from a postgres database,
and runs the gravity model on the data. The output is a matrix of shipments between
trade places.

"""

from psycopg2 import sql
import pandas as pd
from gravity_trade import GravityModel
import argparse
from dotenv import load_dotenv
import os
from concurrent.futures import ThreadPoolExecutor as Pool
#from multiprocessing import Pool
from models import Impedances, Masses
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from itertools import repeat
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm.session import close_all_sessions
import time 
import numpy as np

# load environment variables
load_dotenv()

db_info = os.getenv('DATABASE_URI')
engine = create_engine(db_info, echo=False)
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

# define command line arguments
parser = argparse.ArgumentParser(description='This program pulls an impedence matrix and supply/demand data from a postgres database, and runs the gravity model on the data. The output is a matrix of shipments between trade palces.')
parser.add_argument('-impedence', help="The name of the impedence that you wish to use as input to the gravity model. This parameter is required.", required=True)
parser.add_argument('-trade_commodity_id', help="The trade commodity index that you wish to calculate. If this parameter is not specified, the program will be ran for every commodity", required=False)

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
    if len(supp_demand_df) != imp_matrix.shape[0] or len(supp_demand_df) != imp_matrix.shape[1]:
        print('ERROR: SUPPLY DEMAND AND IMPEDENCE MATRIX NOT SAME SIZE')
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
    #print()
    #print('TOTAL SHIPPED SUPPLY: ', gravity_model.total_shipped_supply)
    #print('TOTAL SHIPPED DEMAND: ', gravity_model.total_shipped_demand)
    if abs(gravity_model.total_shipped_supply - gravity_model.total_shipped_demand) > 1:
        print('ERROR: SUPPLY AND DEMAND NOT BALANCED FOR: TRADE ID {0}'.format(trade_id))
    #pd.DataFrame(data=gravity_model.S).to_csv('/Users/tvarrelman/Documents/gravity_model/example_data/example_output/S_shipments_trip_matrix_{0}.csv'.format(trade_id))
    end_time = time.time()
    print('Runtime: ', end_time - start_time, ' seconds')

if __name__ == '__main__':
    # get command line arguments
    args = parser.parse_args()
    impedence_var = args.impedence
    start_time = time.time()
    imp_matrix = get_impedence_matrix(impedence_var)
    end_time = time.time()
    print('Matrix time: ', end_time - start_time, ' seconds')
    trade_list = get_trade_commodity_id()
    #trade_list = [5041, 5348, 5134, 5120]
    with Pool(2) as pool:
        results = list(pool.map(main, repeat(imp_matrix), trade_list))
    average_run_time = np.mean(results)
    print(average_run_time)
    close_all_sessions()


  