from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
import os 
import sys 
import argparse
import pandas as pd
from itertools import product
# models are in the parent directory, so we need to adjust the path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from models import QCEWComp, NAICStoBEA

# load environment variables
load_dotenv()

# initialize database connection
db_info = os.getenv('DATABASE_URI2')
engine = create_engine(db_info, echo=False)
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

# define command line arguments
parser = argparse.ArgumentParser(description='This program is used to convert QCEW data from 6 digit NAICS codes to BEA sectors.')
parser.add_argument('-ownership', help="The QCEW ownership code is required to run the program. Allowed values include 0, 1, 2, 3, 5.", required=True)
parser.add_argument('-year', help="A year corresponding to QCEW data must be provided. Currently supported years include 2001-2021", required=True)
parser.add_argument('-employer_wage', help="The employer wage number that you wish to convert. Allowed values include 1, 2, 3, 4, 5.", required=True)

# given employer wage number, create field list
def create_field_list(emp_wage_number):
    # base field list
    field_list = [
        QCEWComp.area_fips,
        QCEWComp.own_code,
        QCEWComp.bls_own_code.label('naics_code'),
        QCEWComp.year,
        QCEWComp.tap_estabs_count,
    ]
    # dictionaries for wage and employer fields
    wage_dict = {
        1: [QCEWComp.tap_wages_est_1, 'tap_wages_est_1'],
        2: [QCEWComp.tap_wages_est_2, 'tap_wages_est_2'],
        3: [QCEWComp.tap_wages_est_3, 'tap_wages_est_3'],
        4: [QCEWComp.tap_wages_est_4, 'tap_wages_est_4'],
        5: [QCEWComp.tap_wages_est_5, 'tap_wages_est_5']
    }
    emp_dict = {
        1: [QCEWComp.tap_emplvl_est_1, 'tap_emplvl_est_1'],
        2: [QCEWComp.tap_emplvl_est_2, 'tap_emplvl_est_2'],
        3: [QCEWComp.tap_emplvl_est_3, 'tap_emplvl_est_3'],
        4: [QCEWComp.tap_emplvl_est_4, 'tap_emplvl_est_4'],
        5: [QCEWComp.tap_emplvl_est_5, 'tap_emplvl_est_5']
    }
    wage_info = wage_dict.get(int(emp_wage_number))
    emp_info = emp_dict.get(int(emp_wage_number))
    field_list.append(wage_info[0])
    field_list.append(emp_info[0])
    return field_list, wage_info[1], emp_info[1]

# given a list of tuples, find outliers
def find_outliers(list1, list2):
    t_length = len(list2[0])
    set2 = {tuple(t[:4]) for t in list2}
    outliers = []
    for tuple1 in list1:
        if tuple(tuple1[:4]) not in set2:
            outlier = tuple1 + (0,) * (t_length - 4)  # Add zeros to make same length as tuple1
            outliers.append(outlier)
    return outliers

# given user provided arguments, pull the QCEW data, generate all possible value combinations, and fill missing combinations with zeros
def get_QCEW(args):
    session = Session()
    field_list, wage_col_name, emp_col_name = create_field_list(args.employer_wage)
    query = session.query(*field_list).filter(QCEWComp.year == args.year).filter(QCEWComp.own_code == args.ownership).all()
    csv_data = pd.DataFrame.from_records(query, columns=['area_fips', 'own_code', 'naics_code', 'year', 'tap_estabs_count', wage_col_name, emp_col_name])
    fips = csv_data.area_fips.unique()
    naics = csv_data.naics_code.unique()
    ownership = csv_data.own_code.unique()
    year = csv_data.year.unique()
    csv_data = None
    param_combos = list(product(fips, ownership, naics, year))
    outliers = find_outliers(param_combos, query)
    out_data = query + outliers
    csv_data = pd.DataFrame(out_data, columns=['area_fips', 'own_code', 'naics_code', 'year', 'tap_estabs_count', wage_col_name, emp_col_name])
    print(len(csv_data) == len(param_combos))

if __name__ == '__main__':
    session = Session()
    args = parser.parse_args()
    get_QCEW(args)