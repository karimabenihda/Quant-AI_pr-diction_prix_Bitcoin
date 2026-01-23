from pydantic import BaseModel
from datetime import datetime
 

class UserCreate(BaseModel):
    email: str
    password: str

class UserLogin(BaseModel):
    email: str
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str 


class BtcInput(BaseModel):

    open:float
    high:float
    low:float
    close:float
    volume:float
    quote_asset_volume:datetime
    number_of_trades:float
    taker_buy_base_volume:float
    taker_buy_quote_volume:float
    returns_col:float
    ma_05:float
    ma_10:float
    taker_ratio:float
       
class Prediction(BaseModel):
    open_time: datetime
    open:float
    high:float
    low:float
    close:float
    volume:float
    close_time:float
    quote_asset_volume:datetime
    number_of_trades:float
    taker_buy_base_volume:float
    taker_buy_quote_volume:float
    close_t_plus_10:float
    returns_col:float
    ma_05:float
    ma_10:float
    taker_ratio:float


