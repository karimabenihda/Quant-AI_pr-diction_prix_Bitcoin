from sqlalchemy import Column, Integer, String,DateTime,Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class User(Base):

    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    password = Column(String, nullable=False)

class Btc(Base):
    __tablename__ = "predictions"
    open_time=Column(DateTime),
    open=Column(Float),
    high=Column(Float),
    low=Column(Float),
    close=Column(Float),
    volume=Column(Float),
    close_time=Column(Float),
    quote_asset_volume=Column(DateTime),
    number_of_trades=Column(Float),
    taker_buy_base_volume=Column(Float),
    taker_buy_quote_volume=Column(Float),
    # close_t_plus_10=Column(Float),
    ma_05=Column(Float),
    ma_10=Column(Float),
    taker_ratio=Column(Float),
       