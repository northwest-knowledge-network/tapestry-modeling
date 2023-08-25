import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from decouple import Config, RepositoryEnv
from ras_models import ExtantMatrix, ExtantRows, ExtantColumns, JobProperties 

class RASProcessor:
    def __init__(self, database_uri, job_id):
        self.database_uri = database_uri
        self._create_session()
        self.job_id = job_id

    def _create_session(self):
        engine = create_engine(self.database_uri, echo=False)
        session_factory = sessionmaker(bind=engine)
        self.db_session = scoped_session(session_factory)

    def get_job_properties(self, job_id):
        query = self.db_session.query(JobProperties.max_ras_iterations).filter(JobProperties.ras_job_id == self.job_id).all()
        return query

    def get_extant_matrix(self, job_id):
        query = self.db_session.query(ExtantMatrix.row_id, ExtantMatrix.col_id, ExtantMatrix.amt_original).filter(ExtantMatrix.ras_job_id == self.job_id).all()
        df = pd.DataFrame(query, columns=['row_id', 'col_id', 'matrix'])
        df = df.pivot_table(values='matrix', index='row_id', columns='col_id')
        ex_matrix = df.to_numpy()
        return ex_matrix

    def get_bt_rows(self, job_id):
        query = self.db_session.query(ExtantRows.amt_original).filter(ExtantRows.ras_job_id == self.job_id).order_by(ExtantRows.row_id).all()
        row_totals = np.array(query).flatten()
        return row_totals

    def get_bt_cols(self, job_id):
        query = self.db_session.query(ExtantColumns.amt_original).filter(ExtantColumns.ras_job_id == self.job_id).order_by(ExtantColumns.col_id).all()
        col_totals = np.array(query).flatten()
        return col_totals

    def freeze_negatives(self, bt_rows, bt_cols, extant_mat):
        frozen_matrix = np.copy(extant_mat)
        frozen_matrix[frozen_matrix < 0] = 0

        frozen_bt_rows = np.copy(bt_rows)
        frozen_bt_rows[frozen_bt_rows < 0] = 0

        frozen_bt_cols = np.copy(bt_cols)
        frozen_bt_cols[frozen_bt_cols < 0] = 0

        return frozen_bt_rows, frozen_bt_cols, frozen_matrix
    
    def qc_check1(self, row_bt, col_bt):
        if np.sum(row_bt) != np.sum(col_bt):
            exit("error: row border totals != column border totals")
        else:
            print("success: border totals match")

    def qc_check2(self, ras_row_bt, ras_col_bt, ras_mat):
        row_mask = ras_row_bt < 0
        col_mask = ras_col_bt < 0
        ras_mask = ras_mat < 0

        negative_row = ras_row_bt[row_mask]
        negative_col = ras_col_bt[col_mask]
        negative_mat = ras_mat[ras_mask]

        if len(negative_row) > 0:
            exit("row border total includes negative values")
        if len(negative_col) > 0:
            exit("col border total incldues negative values")
        if len(negative_mat) > 0:
            exit("matrix contains negative values")
        print("success: input data do not contain negative values")

    def qc_check3(self, row_bt, extant_mat):
        qc_row_sums = np.sum(extant_mat, axis=1)
        if len(row_bt) != len(qc_row_sums):
            exit("matrix row count != border total row count")
        for i in range(0, len(qc_row_sums)):
            if qc_row_sums[i] > 0 and row_bt[i] == 0:
                exit("row border total == 0 and matrix row sum > 0")
        print("success: matrix rows and border total rows consistent")

    def qc_check4(self, col_bt, extant_mat):
        qc_col_sums = np.sum(extant_mat, axis=0)
        if len(col_bt) != len(qc_col_sums):
            exit("matrix col count != border total col count")
        for i in range(0, len(qc_col_sums)):
            if qc_col_sums[i] > 0 and col_bt[i] == 0:
                exit("col border total == 0 and matrix col sum > 0")
        print("success: matrix cols and border total cols consistent")

    def perform_ras(self, row_totals, col_totals, mat_data, max_iterations=10000, epsilon=0.00001):
        result_array = np.copy(mat_data)
        row_n = result_array.shape[0]
        col_n = result_array.shape[1]
        success_message = {'success': 'RAS completed, max iterations reached'}
        for i in range(max_iterations):
            
            row_sums = np.sum(result_array, axis=1)
            col_sums = np.sum(result_array, axis=0)
            # logic to only perform scaling on non-zero values 
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
job_id = 519
ras_processor = RASProcessor(db_info, job_id)
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