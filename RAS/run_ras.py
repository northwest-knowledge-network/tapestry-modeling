# -*- coding: utf-8 -*-
"""
    Describe input arguments and run the RAS algorithm
"""
# this is our RAS module
from ras_calculator import RASProcessor
from decouple import Config, RepositoryEnv
import argparse

def main(job_id):
    # get the db URl (formatted consistent with: https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls)
    # this accesses a .env file with our database secrets
    DOTENV_FILE = './.env'
    env_config = Config(RepositoryEnv(DOTENV_FILE))
    db_info = env_config.get('DATABASE_URI2')

    # initialize the RASProcessor with the job_id and db connection info
    ras_processor = RASProcessor(db_info, job_id)
    # this is using the ORM defined in ras_models.py
    job_props = ras_processor.get_job_properties(job_id)
    extant_matrix = ras_processor.get_extant_matrix(job_id)
    bt_rows = ras_processor.get_bt_rows(job_id)
    bt_cols = ras_processor.get_bt_cols(job_id)

    # prepare data for RAS
    ras_bt_rows, ras_bt_cols, ras_mat = ras_processor.freeze_negatives(bt_rows, bt_cols, extant_matrix)

    # perform checks on unfrozen data
    ras_processor.qc_check1(bt_rows, bt_cols)
    ras_processor.qc_check2(ras_bt_rows, ras_bt_cols, ras_mat)
    ras_processor.qc_check3(bt_rows, extant_matrix)
    ras_processor.qc_check4(bt_cols, extant_matrix)

    # after running the checks, perform the ras
    result = ras_processor.perform_ras(ras_bt_rows, ras_bt_cols, ras_mat)
    print(result)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='This script runs RAS given data that is stored in the PostgreSQL database.')
    # for testing, we have id 519 in the db
    parser.add_argument('--job_id', '-j', type=int, help='Specify the job ID (must exist in the database).', required=True)
    args = parser.parse_args()
    job_id = args.job_id
    main(job_id)