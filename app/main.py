from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from passlib.context import CryptContext
from fastapi.security import OAuth2PasswordRequestForm
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

from schemas import UserLogin, Token,BtcInput
from model import User
import joblib
import numpy as np
from jose import jwt

load_dotenv() 

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM")
ACCESS_TOKEN_EXPIRE_MINUTES =  os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES")


DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
print("DATABASE_URL:", DATABASE_URL)

engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

app=FastAPI()

Base.metadata.create_all(bind=engine)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)


def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (
        expires_delta if expires_delta else timedelta(minutes=int(ACCESS_TOKEN_EXPIRE_MINUTES))
    )
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


@app.post("/signup")
def create_user(user: UserLogin, db: Session = Depends(get_db)):
    hashed_password = get_password_hash(user.password)
    db_user = User(email=user.email, password=hashed_password)
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return user

# 🔹 Endpoint pour login

@app.post("/login", response_model=Token)
def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_db)
):
    user = db.query(User).filter(User.email == form_data.username).first()

    if not user or not verify_password(form_data.password, user.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token = create_access_token(data={"sub": user.email})
    return {
        "access_token": access_token,
        "token_type": "bearer"
    }

MODEL_PATH = "../Machine_Learning/random_forest_model.joblib"
rf_model = joblib.load(MODEL_PATH)


@app.post("/predict_close_t_plus_10")
def predict_close(input_data: BtcInput):
    # convert datetime to float (timestamp)
    quote_ts = input_data.quote_asset_volume.timestamp()
    
    # create feature array in the same order as training
    X = np.array([[ 
        input_data.open,
        input_data.high,
        input_data.low,
        input_data.close,
        input_data.volume,
        quote_ts,  # converted datetime
        input_data.number_of_trades,
        input_data.taker_buy_base_volume,
        input_data.taker_buy_quote_volume,
        input_data.returns_col,
        input_data.ma_05,
        input_data.ma_10,
        input_data.taker_ratio
    ]])
    
    # predict
    pred = rf_model.predict(X)
    
    return {"close_t_plus_10": float(pred[0])}