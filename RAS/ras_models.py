# -*- coding: utf-8 -*-
"""
    ORM for the PostgreSQL database tables that will be used by the program
"""

from sqlalchemy.orm import sessionmaker, relationship, backref, selectinload
from sqlalchemy import Table, Column, Integer, String, DateTime, Text, Float, ForeignKey, create_engine, Enum
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ExtantMatrix(Base):
    __tablename__ = 'fact_extant_matrix'
    id = Column(Integer, primary_key = True)
    ras_id = Column(Integer)
    row_id = Column(Integer)
    col_id = Column(Integer)
    amt_original = Column(Float)
    amt_to_freeze_adhoc = Column(Float)
    amt_after_ras = Column(Float)
    amt_frozen = Column(Float)
    __table_args__ = {'schema': 'ras'}

class ExtantRows(Base):
    __tablename__ = 'fact_extant_bordertotals_rows'
    id = Column(Integer, primary_key = True)
    ras_id = Column(Integer)
    row_id = Column(Integer)
    amt_original = Column(Float)
    amt_after_ras = Column(Float)
    __table_args__ = {'schema': 'ras'}
    
class ExtantColumns(Base):
    __tablename__ = 'fact_extant_bordertotals_columns'
    id = Column(Integer, primary_key = True)
    ras_id = Column(Integer)
    col_id = Column(Integer)
    amt_original = Column(Float)
    amt_after_ras = Column(Float)
    __table_args__ = {'schema': 'ras'}

class JobProperties(Base):
    __tablename__ = 'properties_ras_jobs'
    id = Column(Integer, primary_key = True)
    external_ras_job_id = Column(Integer)
    bordertotal_control_total = Column(Float)
    bordertotal_tolerance_absolutevalue = Column(Float)
    matrix_solution_tolerance_absolutevalue = Column(Float)
    max_ras_iterations = Column(Float)
    status = Column(Enum('running', 'completed', 'pending'))
    __table_args__ = {'schema': 'ras'}
