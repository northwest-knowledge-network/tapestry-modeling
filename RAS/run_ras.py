# -*- coding: utf-8 -*-
"""
    Describe input arguments and run the RAS algorithm
"""
# this is our RAS module
from ras_calculator import RASProcessor
from decouple import Config, RepositoryEnv
import argparse

def main(job_id, iterations=None, epsilon=None):
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
    ras_bt_rows, ras_bt_cols, ras_mat, frozen_bt_rows, frozen_bt_cols, frozen_mat = ras_processor.freeze_negatives(bt_rows, bt_cols, extant_matrix)

    # perform checks on unfrozen data
    ras_processor.qc_check1(bt_rows, bt_cols)
    ras_processor.qc_check2(ras_bt_rows, ras_bt_cols, ras_mat)
    ras_processor.qc_check3(bt_rows, extant_matrix)
    ras_processor.qc_check4(bt_cols, extant_matrix)

    # after running the checks, perform the ras
    if iterations and epsilon:
        result = ras_processor.perform_ras(ras_bt_rows, ras_bt_cols, ras_mat, frozen_mat, iterations, epsilon)
    elif iterations:
        result = ras_processor.perform_ras(ras_bt_rows, ras_bt_cols, ras_mat, frozen_mat, iterations, 0.00001)
    elif epsilon:
        result = ras_processor.perform_ras(ras_bt_rows, ras_bt_cols, ras_mat, frozen_mat, 10000, epsilon)
    else:
        result = ras_processor.perform_ras(ras_bt_rows, ras_bt_cols, ras_mat, frozen_mat)
    print(result)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='This script runs RAS given data that is stored in the PostgreSQL database.')
    # for testing, we have id 519 in the db
    parser.add_argument('--job_id', '-j', type=int, help='Specify the job ID (must exist in the database).', required=True)
    parser.add_argument('--iterations', '-i', type=int, help='Specify the max iterations for RAS. The default value is 10000.')
    parser.add_argument('--epsilon', '-e', type=float, help='Error threshold for the RAS algorithm.')
    args = parser.parse_args()
    job_id = args.job_id
    it_number = args.iterations
    eps = args.epsilon
    if it_number and eps:
        main(job_id, it_number, eps)
    elif it_number:
        main(job_id, it_number, None)
    elif eps:
        main(job_id, None, eps)
    else:
        main(job_id)