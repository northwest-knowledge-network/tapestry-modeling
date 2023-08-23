# -*- coding: utf-8 -*-
"""
Created on Thu May  4 10:04:35 2023
 
@author: Philip Watson, pwatson@uidaho.edu & Tanner Varrelman, tvarrelman@uidaho.edu
 
"""
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from ras_models import ExtantMatrix, ExtantRows, ExtantColumns, JobProperties 

def db_session():
    # initialize database connection
    db_info = os.getenv('DATABASE_URI2')
    engine = create_engine(db_info, echo=False)
    session_factory = sessionmaker(bind=engine)
    Session = scoped_session(session_factory)
    return Session

def get_job_properties(db_session, job_id):
    query = db_session.query(JobProperties).filter(JobProperties.ras_job_id == job_id).all()
    return query

def get_extant_matrix(db_session, job_id):
    # need to format as a numpy matrix
    query = db_session.query(ExtantMatrix).filter(ExtantMatrix.ras_job_id == job_id).all()
    return query

def get_bt_rows(db_session, job_id):
    query = db_session.query(ExtantRows).filter(ExtantRows.ras_job_id == job_id).all()
    return query

def get_bt_cols(db_session, job_id):
    query = db_session.query(ExtantColumns).filter(ExtantColumns.ras_job_id == job_id).all()
    return query
    
# Define seed array and control totals
seed_ary = np.array([[1.1, 2.1, 1.1],
                     [3.1, 5.1, 5.1],
                     [6.1, 2.1, 2.1]])
 
row_totals = np.array([5.00, 15.00, 8.00])
col_totals = np.array([11.00, 9.00, 8.00])
 
 
# Initialize result array with seed array values
result_ary = np.copy(seed_ary)
 
# Define convergence criteria
epsilon = 0.00001
max_iterations = 1000
 
# Iterate until convergence or maximum iterations reached
for i in range(max_iterations):
    # Calculate row and column multipliers
    row_multipliers = row_totals / np.sum(result_ary, axis=1)
    col_multipliers = col_totals / np.sum(result_ary, axis=0)
   
    # Apply row and column multipliers to result array
    result_ary = result_ary * row_multipliers[:, np.newaxis] * col_multipliers
   
    # Check for convergence
    row_sums = np.sum(result_ary, axis=1)
    col_sums = np.sum(result_ary, axis=0)
    row_error = np.max(np.abs(row_totals - row_sums))
    col_error = np.max(np.abs(col_totals - col_sums))
    if row_error < epsilon and col_error < epsilon:
        break
 
# Print result array
print(result_ary)