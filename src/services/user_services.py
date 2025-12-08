from datetime import datetime, timedelta, timezone
from jose import JWTError, jwt, ExpiredSignatureError
from src.core.config import settings
import httpx
from fastapi import HTTPException
from passlib.context import CryptContext
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import Depends, HTTPException
from sqlalchemy.orm import Session
from src.db.database import get_db
from src.models.auth import User
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from src.constants import central_server_login
from src.logger.logger_setup import logger

auth_scheme = HTTPBearer()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def create_access_token(data: dict, expires_delta: timedelta | None = None):
    try:
        print("TOKEN EXP MINUTES:", settings.ACCESS_TOKEN_EXPIRE_MINUTES, type(settings.ACCESS_TOKEN_EXPIRE_MINUTES))

        to_encode = data.copy()
        expire = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES))
        to_encode.update({"exp": expire})
        
        return jwt.encode(to_encode, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)
    except Exception as e:
        logger.exception(f"Create_access_token: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})

def create_refresh_token(data: dict, expires_delta: timedelta | None = None):
    try:
        to_encode = data.copy()
        expire = datetime.now(timezone.utc) + (expires_delta or timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS))
        to_encode.update({"exp": expire})
        return jwt.encode(to_encode, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)
    except Exception as e:
        logger.exception(f"create_refresh_token api: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})


def decode_access_token(token: str):
    try:
        payload = jwt.decode(token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
        return payload
    except ExpiredSignatureError:
        raise HTTPException(
            status_code=401,
            detail={"status": "error", "message": "Token has expired"}
        )
    except JWTError:
        logger.exception(f"decode_access_token function: {JWTError}")
        raise HTTPException(
            status_code=401,
            detail={"status": "error", "message": "Invalid token"}
        )


async def decode_refresh_token(token:str):
    try:
        decoded_token = jwt.decode(
            token,
            settings.JWT_SECRET_KEY,
            algorithms=[settings.JWT_ALGORITHM]
        )
        return decoded_token
    except ExpiredSignatureError:
        logger.warning("Refresh token expired")
        raise HTTPException(
            status_code=401,
            detail={"status": "error", "message": "Refresh token expired, please login again"}
        )
    except JWTError:
        logger.warning("Invalid refresh token")
        raise HTTPException(
            status_code=401,
            detail={"status": "error", "message": "Invalid refresh token"}
        )
    except Exception as e:
        logger.error(f"From decode_refresh_token Function: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})




async def central_login(credentials):
    try: 
        payload = {
            "userName": credentials.username,
            "password": credentials.password
        }
        headers = {"Content-Type": "application/json"}

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(f"{central_server_login}/v2/erp/login", json=payload, headers=headers)
                data = response.json()
            except Exception as e:
                logger.error("Central server login failed wit error: {e}")
                raise HTTPException(status_code=404, detail={"status":"error","message":"Invalid Credentials","error_code":str(e)})

        if "status" not in data:
            logger.error("Invalid response from Central Server Login ERP API")
            raise HTTPException(status_code=502, detail={"message":"Invalid response from ERP API: 'status' field missing"})

        # Check the response body for success or failure
        if data.get("status") == "error":
            logger.error("Invalid credentials, Please use correct credentials")
            raise HTTPException(status_code=404, detail={"message": "Invalid Credentials, Please use correct credentials ", "error_code": data.get("error")}
        )
        hashed_password = pwd_context.hash(credentials.password)
        
        inner_data = data.get("data")
        if not inner_data:
            logger.error("Missing data from central server : missing 'data' key")
            raise HTTPException(status_code=502, detail={"message":"Invalid response: missing 'data' key"})
        
        provider = inner_data.get("provider")
        if not provider:
            logger.error("Missing data from central server : missing 'provider' key")
            raise HTTPException(status_code=502, detail={"message":"Invalid response: missing 'provider' info"})
        
        return (hashed_password, provider, data)
    
    except Exception as e:
        logger.error(f"From Central Login Function: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})



async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(auth_scheme),
    db: AsyncSession = Depends(get_db)
) -> User:
    try:
        token = credentials.credentials
        payload = decode_access_token(token)
        if not payload or "sub" not in payload:
            logger.exception("in get_current_user: Invalid credentials")
            raise HTTPException(status_code=401, detail={"status":"error","message":"Invalid credentials"})

        # user = db.query(User).filter(User.username == payload["sub"]).first()
        result = await db.execute(select(User).where(User.username == payload["sub"]))
        user = result.scalars().first()  # get the User object
        
        if not user:
            logger.error("get_current_user: User not found")
            raise HTTPException(status_code=401, detail={"message":"User not found", "status":"error"})
        
        return user
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"get_current_user function: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})
        
        