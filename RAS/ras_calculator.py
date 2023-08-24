import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from decouple import Config, RepositoryEnv
from ras_models import ExtantMatrix, ExtantRows, ExtantColumns, JobProperties 

class RASProcessor:
    def __init__(self, database_uri):
        self.database_uri = database_uri
        self._create_session()

    def _create_session(self):
        engine = create_engine(self.database_uri, echo=False)
        session_factory = sessionmaker(bind=engine)
        self.db_session = scoped_session(session_factory)

    def get_job_properties(self, job_id):
        query = self.db_session.query(JobProperties.max_ras_iterations).filter(JobProperties.ras_job_id == job_id).all()
        return query

    def get_extant_matrix(self, job_id):
        query = self.db_session.query(ExtantMatrix.row_id, ExtantMatrix.col_id, ExtantMatrix.amt_original).filter(ExtantMatrix.ras_job_id == job_id).all()
        df = pd.DataFrame(query, columns=['row_id', 'col_id', 'matrix'])
        df = df.pivot_table(values='matrix', index='row_id', columns='col_id')
        ex_matrix = df.to_numpy()
        return ex_matrix

    def get_bt_rows(self, job_id):
        query = self.db_session.query(ExtantRows.amt_original).filter(ExtantRows.ras_job_id == job_id).order_by(ExtantRows.row_id).all()
        row_totals = np.array(query).flatten()
        return row_totals

    def get_bt_cols(self, job_id):
        query = self.db_session.query(ExtantColumns.amt_original).filter(ExtantColumns.ras_job_id == job_id).order_by(ExtantColumns.col_id).all()
        col_totals = np.array(query).flatten()
        return col_totals
    
    def bordertotal_qc(self, row_bt, col_bt):
        if np.sum(row_bt) != np.sum(col_bt):
            exit("error: row border totals != column border totals")
        else:
            print("success: border totals match")

    def matrix_qc(self, mat_data):
        for val in np.sum(mat_data, axis=1):
            if val == 0:
                exit("error: matrix row sum == 0")

    def perform_ras(self, mat_data, row_totals, col_totals, max_iterations=10000, epsilon=0.00001):
        result_array = np.copy(mat_data)
        row_n = result_array.shape[0]
        col_n = result_array.shape[1]
        success_message = {'success': 'RAS completed, max iterations reached'}
        for i in range(max_iterations):
            
            row_sums = np.sum(result_array, axis=1)
            col_sums = np.sum(result_array, axis=0)
            R = np.ones(row_n)
            S = np.ones(col_n)
            non_zero_rows = row_sums > 0
            non_zero_cols = col_sums > 0 

            R[non_zero_rows] = row_totals[non_zero_rows] / row_sums[non_zero_rows]
            S[non_zero_cols] = col_totals[non_zero_cols] / col_sums[non_zero_cols]
           
            result_array = result_array * np.outer(R, S)

            row_sums = np.sum(result_array, axis=1)
            col_sums = np.sum(result_array, axis=0)
            row_error = np.max(np.abs(row_totals - row_sums))
            col_error = np.max(np.abs(col_totals - col_sums))
            if row_error < epsilon and col_error < epsilon:
                success_message['success'] = 'RAS completed, threshold reached at {0} iterations'.format(i)
                break
        print(success_message['success'])
        return result_array

# Example usage:
DOTENV_FILE = './.env'
env_config = Config(RepositoryEnv(DOTENV_FILE))
db_info = env_config.get('DATABASE_URI2')
ras_processor = RASProcessor(db_info)
job_id = 519
job_props = ras_processor.get_job_properties(job_id)
extant_matrix = ras_processor.get_extant_matrix(job_id)
bt_rows = ras_processor.get_bt_rows(job_id)
bt_cols = ras_processor.get_bt_cols(job_id)

# remove negatives
frozen_matrix = np.copy(extant_matrix)
frozen_matrix[frozen_matrix < 0] = 0
frozen_matrix[frozen_matrix == 0] = 0

frozen_bt_rows = np.copy(bt_rows)
frozen_bt_rows[frozen_bt_rows < 0] = 0
frozen_bt_rows[frozen_bt_rows == 0] = 0

frozen_bt_cols = np.copy(bt_cols)
frozen_bt_cols[frozen_bt_cols < 0] = 0
frozen_bt_cols[frozen_bt_cols == 0] = 0

ras_processor.bordertotal_qc(bt_rows, bt_cols)

result = ras_processor.perform_ras(frozen_matrix, frozen_bt_rows, frozen_bt_cols)
print(result)