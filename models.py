from sqlalchemy.orm import sessionmaker, relationship, backref, selectinload
from sqlalchemy import Table, Column, Integer, String, DateTime, Text, Float, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Masses(Base):
    __tablename__ = 'fact_masses_409x4135'
    id = Column(Integer, primary_key=True)
    tradeplace_id = Column(Integer)
    supply_amount = Column(Float)
    demand_amount = Column(Float)
    trade_commodity_id = Column(Integer)
    __table_args__ = {'schema': 'us2012'}

class Impedances(Base):
    __tablename__ = 'fact_impedances_4135'
    id = Column(Integer, primary_key=True)
    fromplaceid = Column(Integer)
    toplaceid = Column(Integer)
    hwyimpedence = Column(Float)
    rrimpedence = Column(Float)
    waterimpedence = Column(Float)
    comboimpedence = Column(Float)
    __table_args__ = {'schema': 'us2012'}

### QCEW Tables ###
class QCEWComp(Base):
    __tablename__ = 'qcew_processed'
    id = Column(Integer, primary_key=True)
    area_fips = Column(String(255))
    own_code = Column(Integer)
    bls_own_code = Column(String(255))
    year = Column(Integer)
    tap_estabs_count = Column(Integer)
    tap_emplvl_est_1 = Column(Integer)
    tap_emplvl_est_2 = Column(Integer)
    tap_emplvl_est_3 = Column(Integer)
    tap_emplvl_est_4 = Column(Integer)
    tap_emplvl_est_5 = Column(Integer)
    tap_wages_est_1 = Column(Integer)
    tap_wages_est_2 = Column(Integer)
    tap_wages_est_3 = Column(Integer)
    tap_wages_est_4 = Column(Integer)
    tap_wages_est_5 = Column(Integer)
    __table_args__ = {'schema': 'qcew'}

class NAICStoBEA(Base):
    __tablename__ = 'dim_bridge_qnaics6_to_ind409'
    id = Column(Integer, primary_key=True)
    bls_own_code = Column(Integer)
    r_seq = Column(Integer)
    r_naics6_code = Column(String(255))
    c_seq = Column(Integer)
    c_bea_code_2_lvl_6 = Column(String(255))
    naics_to_io_coeffs = Column(Float)
    __table_args__ = {'schema': 'qcew'}