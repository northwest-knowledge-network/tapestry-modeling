# -*- coding: utf-8 -*-
"""
Created on Thu May  4 10:04:35 2023
 
@author: Philip Watson, pwatson@uidaho.edu & Tanner Varrelman, tvarrelman@uidaho.edu
 
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from decouple import Config, RepositoryEnv
from ras_models import ExtantMatrix, ExtantRows, ExtantColumns, JobProperties 
import csv
import os

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
        bt_row_totals = np.array(query).flatten()
        return bt_row_totals

    def get_bt_cols(self, job_id):
        query = self.db_session.query(ExtantColumns.amt_original).filter(ExtantColumns.ras_job_id == self.job_id).order_by(ExtantColumns.col_id).all()
        bt_col_totals = np.array(query).flatten()
        return bt_col_totals

    def freeze_negatives(self, bt_rows, bt_cols, extant_mat):
        ras_matrix = np.copy(extant_mat)
        ras_matrix[ras_matrix < 0] = 0

        ras_bt_rows = np.copy(bt_rows)
        ras_bt_rows[ras_bt_rows < 0] = 0

        ras_bt_cols = np.copy(bt_cols)
        ras_bt_cols[ras_bt_cols < 0] = 0

        neg_values_matrix = np.copy(extant_mat)
        neg_values_matrix[neg_values_matrix >= 0] = 0

        neg_values_bt_rows = np.copy(bt_rows)
        neg_values_bt_rows[neg_values_bt_rows >= 0] = 0

        neg_values_bt_cols = np.copy(bt_cols)
        neg_values_bt_cols[neg_values_bt_cols >= 0] = 0

        return ras_bt_rows, ras_bt_cols, ras_matrix, neg_values_bt_rows, neg_values_bt_cols, neg_values_matrix
    
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

    def perform_ras(self, bt_row_totals, bt_col_totals, mat_data, frozen_mat, original_mat, max_iterations=1000, epsilon=0.00001):
        result_array = np.copy(mat_data)
        row_n = result_array.shape[0]
        col_n = result_array.shape[1]
        success_message = {'success': 'RAS completed, max iterations reached'}
        if not os.path.exists('./RAS_logs'):
            os.makedirs('./RAS_logs')
        file = open('./RAS_logs/epsilon_log_{0}.csv'.format(self.job_id), 'w', newline='')
        writer = csv.writer(file)
        field = ['iteration', 'row_epsilon', 'column_epsilon']
        writer.writerow(field)
        R = np.zeros(row_n, dtype=float)
        S = np.zeros(col_n, dtype=float)
        for i in range(max_iterations):
            row_sums = np.sum(result_array, axis=1, dtype=float)
            col_sums = np.sum(result_array, axis=0, dtype=float)
       
            # mask out zeros
            non_zero_rows = row_sums > 0
            non_zero_cols = col_sums > 0 
            # row and column multipliers for non-zero values, otherwise the scaler is 1
            R[non_zero_rows] = bt_row_totals[non_zero_rows] / row_sums[non_zero_rows]
            S[non_zero_cols] = bt_col_totals[non_zero_cols] / col_sums[non_zero_cols]
            
            result_array *= np.outer(R, S)

            row_sums_final = np.sum(result_array, axis=1, dtype=float)
            col_sums_final = np.sum(result_array, axis=0, dtype=float)
            row_error = np.max(np.abs(bt_row_totals - row_sums_final))
            col_error = np.max(np.abs(bt_col_totals - col_sums_final))
            writer.writerow([i, row_error, col_error])
            if row_error < epsilon and col_error < epsilon:
                success_message['success'] = 'RAS completed, threshold reached at {0} iterations'.format(i)
                break
        file.close()
        final_out = np.add(result_array, frozen_mat)
        print(success_message['success'])
        original_df = pd.DataFrame(original_mat)
        original_df['row_id'] = original_df.index + 1
        original_df = original_df.melt(id_vars='row_id', var_name='col_id', value_name='amt_original')
        original_df['col_id'] = original_df['col_id'] + 1

        output_df = pd.DataFrame(final_out)
        output_df['row_id'] = output_df.index + 1
        output_df = output_df.melt(id_vars='row_id', var_name='col_id', value_name='amt_after_ras')
        output_df['col_id'] = output_df['col_id'] + 1 

        frozen_df = pd.DataFrame(frozen_mat)
        frozen_df['row_id'] = frozen_df.index + 1
        frozen_df = frozen_df.melt(id_vars='row_id', var_name='col_id', value_name='amt_frozen')
        frozen_df['col_id'] = frozen_df['col_id'] + 1

        final_df = pd.merge(original_df, output_df, how='inner',
                    left_on=['row_id', 'col_id'],
                    right_on=['row_id', 'col_id'])
        final_df = pd.merge(final_df, frozen_df, how='inner',
                    left_on=['row_id', 'col_id'],
                    right_on=['row_id', 'col_id'])
        for index, row in final_df.iterrows():
            row_id = row['row_id']
            col_id = row['col_id']
            matrix_value = row['amt_original']
            new_matrix_value = row['amt_after_ras']
            frozen_val = row['amt_frozen']
            db_record = self.db_session.query(ExtantMatrix).filter_by(row_id=row_id, col_id=col_id).first()
            if db_record:
                db_record.amt_after_ras = new_matrix_value
                db_record.amt_frozen = frozen_val
            else:
                print('Unable to find data; row_id: {0}, col_id: {1}, amt_original: {2}'.format(row_id, col_id, matrix_value))
        self.db_session.commit()
        return final_out
