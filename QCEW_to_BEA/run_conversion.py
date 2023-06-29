from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
import os 
import sys 
import argparse
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
    return field_list

if __name__ == '__main__':
    session = Session()
    args = parser.parse_args()
    emp_wage_num = args.employer_wage
    field_list = create_field_list(emp_wage_num)
    query = session.query(*field_list).filter(QCEWComp.year == args.year).first()
    print(query)