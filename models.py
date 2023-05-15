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