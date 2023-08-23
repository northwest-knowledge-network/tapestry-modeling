from sqlalchemy.orm import sessionmaker, relationship, backref, selectinload
from sqlalchemy import Table, Column, Integer, String, DateTime, Text, Float, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ExtantMatrix(Base):
    __tablename__ = 'fact_extant_matrix'
    ras_job_id = Column(Integer)
    row_id = Column(Integer)
    col_id = Column(Integer)
    amt_original = Column(Float)
    amt_to_freeze_adhoc = Column(Float)
    amt_after_ras = Column(Float)
    __table_args__ = {'schema': 'qcew'}

class ExtantRows(Base):
    __tablename__ = 'fact_extant_bordertotals_rows'
    ras_job_id = Column(Integer)
    row_id = Column(Integer)
    amt_original = Column(Float)
    amt_after_ras = Column(Float)
    __table_args__ = {'schema': 'qcew'}
    
class ExtantColumns(Base):
    __tablename__ = 'fact_extant_bordertotals_columns'
    ras_job_id = Column(Integer)
    col_id = Column(Integer)
    amt_original = Column(Float)
    amt_after_ras = Column(Float)
    __table_args__ = {'schema': 'qcew'}

class JobProperties(Base):
    __tablename__ = 'properties_ras_jobs'
    ras_job_id = Column(Integer)
    bordertotal_control_total = Column(Float)
    bordertotal_tolerance_absolutevalue = Column(Float)
    matrix_solution_tolerance_absolutevalue = Column(Float)
    max_ras_iterations = Column(Float)
    __table_args__ = {'schema': 'qcew'}