import numpy as np
import pandas as pd
import psycopg2 as psql
import sqlalchemy as sql

from fastapi import FastAPI, Depends, HTTPException, status, UploadFile
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from pydantic.schema import Optional
from typing import Annotated
from passlib.context import CryptContext
from jose import JWTError, jwt
from datetime import datetime, timedelta

SECRET_KEY = "6d6c17e2c3b2875911ffdb76d47e9efbdd6748b58b430b6d403414bf7d89b0ec"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 10

users_db = {
    "globant": {
        "username": "globant",
        "hashed_password": "$2b$12$D3ORv9BVZd4Nze/wzb9.3.urjphiQnYZ9fcKzU.nsGpEr1OsG/llK",
        "disabled": False,
    },
    "dev_user": {
        "username": "dev_user",
        "hashed_password": "$2b$12$RTbxQ4p/JlhAW3b2n832xOcBHq/ZhIL7W5Ja5sckAdxhMDQ/QsRk.",
        "disabled": False,
    },
}

app = FastAPI()

class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: Optional[str]

class UserID(BaseModel):
    username: str
    disabled: bool | None = None

class UserInDB(UserID):
    hashed_password: str

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

def get_user(db, username: str):
    if username in db:
        user_dict = db[username]
        return UserInDB(**user_dict)

def authenticate_user(users_db, username: str, password: str):
    user = get_user(users_db, username)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user

def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception
    user = get_user(users_db, username=token_data.username)
    if user is None:
        raise credentials_exception
    return user

async def get_current_active_user(current_user: Annotated[UserID, Depends(get_current_user)]):
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user


@app.post("/token", response_model=Token)
async def login_for_access_token(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):
    user = authenticate_user(users_db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/users/me/", response_model=UserID)
async def read_users_me(current_user: Annotated[UserID, Depends(get_current_active_user)]):
    return current_user


@app.post("/check")
def foo(file: UploadFile):
    df = pd.read_csv(file.file)
    return len(df)

@app.post("/upload_jobs")
def foo(file: UploadFile, active_user: Annotated[str, Depends(get_current_active_user)]):
    df = pd.read_csv(file.file)
    df.columns = ['id', 'job']
    df2 = df[~(df.id.isin([np.NaN]))]
    df_error = df[(df.id.isin([np.NaN]))]
    try:
        conn_string = 'postgresql://user:password@db:5432/globant_challenge'
        db = sql.create_engine(conn_string)
        conn = db.connect()
        connection = db.raw_connection()
        cursor = connection.cursor()

        cursor.execute('SELECT distinct id FROM jobs')
        rows = cursor.fetchall()
        jobs = []
        for row in rows:
            jobs.append(int(row[0]))

        cursor.close()

        df_final = df2[~(df2.id.isin(jobs))]
        df_dupl = df2[(df2.id.isin(jobs))]

        df_final.to_sql('jobs', con=conn, if_exists='append', index=False)
        df_dupl.to_sql('error_jobs', con=conn, if_exists='append', index=False)
        df_error.to_sql('error_jobs', con=conn, if_exists='append', index=False)
        conn.close()

        return (f"Se insertaron {len(df_final)} registros en la tabla jobs. Registros con PK duplicada: {len(df_dupl)}. Registros con error: {len(df_error)}")
    except:
        print('Connection failed')

@app.post("/upload_departments")
def foo(file: UploadFile, active_user: Annotated[str, Depends(get_current_active_user)]):
    df = pd.read_csv(file.file)
    df.columns = ['id', 'department']
    df2 = df[~(df.id.isin([np.NaN]))]
    df_error = df[(df.id.isin([np.NaN]))]
    try:
        conn_string = 'postgresql://user:password@db:5432/globant_challenge'
        db = sql.create_engine(conn_string)
        conn = db.connect()
        connection = db.raw_connection()
        cursor = connection.cursor()

        cursor.execute('SELECT distinct id FROM departments')
        rows = cursor.fetchall()
        depart = []
        for row in rows:
            depart.append(int(row[0]))

        cursor.close()

        df_final = df2[~(df2.id.isin(depart))]
        df_dupl = df2[(df2.id.isin(depart))]

        df_final.to_sql('departments', con=conn, if_exists='append', index=False)
        df_dupl.to_sql('error_departments', con=conn, if_exists='append', index=False)
        df_error.to_sql('error_departments', con=conn, if_exists='append', index=False)
        conn.close()

        return (f"Se insertaron {len(df_final)} registros en la tabla departments. Registros con PK duplicada: {len(df_dupl)}. Registros con error: {len(df_error)}")
    except:
        print('Connection failed')


@app.post("/upload_hired_employees")
def foo(file: UploadFile, active_user: Annotated[str, Depends(get_current_active_user)]):
    df = pd.read_csv(file.file)
    df.columns = ['id', 'name', 'datetime', 'department_id', 'job_id']

    try:
        conn_string = 'postgresql://user:password@db:5432/globant_challenge'
        db = sql.create_engine(conn_string)
        conn = db.connect()
        connection = db.raw_connection()
        cursor = connection.cursor()

        cursor.execute('SELECT distinct id FROM jobs')
        rows = cursor.fetchall()
        jobs = []
        for row in rows:
            jobs.append(int(row[0]))

        cursor.execute('SELECT distinct id FROM departments')
        rows = cursor.fetchall()
        depart = []
        for row in rows:
            depart.append(int(row[0]))

        cursor.execute('SELECT distinct id FROM hired_employees')
        rows = cursor.fetchall()
        hired = []
        for row in rows:
            hired.append(int(row[0]))

        cursor.close()

        df_final = df[~(df.id.isin(hired)) & ~(df.id.isin([np.NaN])) & (df.job_id.isin(jobs)) & (df.department_id.isin(depart)) & ~(df.datetime.isin([np.NaN]))]
        df_dupl = df[(df.id.isin(hired))]
        df_error = df[(df.id.isin([np.NaN])) | (df.job_id.isin([np.NaN])) | (df.department_id.isin([np.NaN])) | (df.datetime.isin([np.NaN])) ]
        
        df_final.to_sql('hired_employees', con=conn, if_exists='append', index=False)
        df_dupl.to_sql('error_hired_employees', con=conn, if_exists='append', index=False)
        df_error.to_sql('error_hired_employees', con=conn, if_exists='append', index=False)
        conn.close()

        return (f"Se insertaron {len(df_final)} registros en la tabla departments. Registros con PK duplicada: {len(df_dupl)}. Registros con error: {len(df_error)}")
    except:
        print('Connection failed')

