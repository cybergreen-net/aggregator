from sqlalchemy import Column, String, Integer, BigInteger, Boolean, Float, Text, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class LogEntry(Base):
    __tablename__ = 'logentry'

    id = Column(Integer)
    date = Column(TIMESTAMP)
    ip = Column(String(32))
    risk = Column(Integer)
    asn = Column(BigInteger)
    country = Column(String(2))


class Risk(Base):
    __tablename__ = "dim_risk"

    id = Column(Integer)
    slug = Column(String(32))
    title = Column(String(32))
    is_archived = Column(Boolean)
    taxonomy = Column(String(16))
    measurement_units = Column(String(32))
    amplification_factor = Column(Float)
    description = Column(Text)


class Count(Base):
    __tablename__ = "count"

    date = Column(TIMESTAMP)
    risk = Column(Integer)
    country = Column(String(2))
    asn = Column(BigInteger)
    count = Column(Integer)
    count_amplified = Column(Float)
