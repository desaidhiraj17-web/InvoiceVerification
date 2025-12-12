from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from src.services.user_services import create_access_token, create_refresh_token,central_login,decode_refresh_token
from src.db.database import get_db
from src.models.auth import User
from src.schemas.auth import TokenResponse, LoginUser, RegisterUserSchema,RefreshTokenRequest
from sqlalchemy.future import select
from sqlalchemy import or_, text
import httpx
from src.logger.logger_setup import logger
from src.constants import central_server_login

router = APIRouter(tags=["Authentication"])


@router.post("/login")
async def login(credentials: LoginUser, db: AsyncSession = Depends(get_db)):
    """Authenticates user via central login server and validates existence in local database.
    Creates a new user locally if not found and returns JWT access and refresh tokens.
    Returns appropriate error message if authentication fails at the central server."""
    try:
        logger.info("login api started")
        hashed_password,provider, data= await central_login(credentials)
        
        first_name = provider.get("name")
        username = provider.get("userName")
        email = provider.get("email1")
        erp_id = provider.get("erpCode")
        
        # result = await db.execute(select(User).filter(User.email == email))
        # existing_user = result.scalar_one_or_none()
        user_exists_query = """
            SELECT *
            FROM users
            WHERE email = :email
            LIMIT 1;
            """
        result = await db.execute(text(user_exists_query), {"email": email})
        existing_user = result.mappings().first()
        
        if existing_user:
            logger.info("User already exists")
            access_token = create_access_token({"sub": username})
            logger.info("Access token created")
            refresh_token = create_refresh_token({"sub": username})
            logger.info("Refresh token created")
            data["message"] = "User already exists"
            data["data"]["access_token"] = access_token
            data["data"]["refresh_token"]=  refresh_token
            data["data"]["user_id"] = existing_user.id
            data["data"]["email"] = existing_user.email
            logger.info("Succfully run login api")
            return  data
        
        # user_obj = User(first_name=first_name,erp_id=erp_id,email=email,username=username,hashed_password=hashed_password)
        insert_user_query = """
            INSERT into users(
                first_name, username, email, hashed_password,
                erp_id, erp_name, user_type, active, created_at, updated_at
            )
            VALUES (
                :first_name, :username, :email, :hashed_password,
                :erp_id, :erp_name, :user_type, :active, :created_at, :updated_at
            )
            """
        now = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        await db.execute(
            text(insert_user_query),
            {
                "first_name": first_name,
                "username": username,
                "email": email,
                "hashed_password": hashed_password,
                "erp_id": erp_id,
                "erp_name": None,
                "user_type": None,     # You can set default enum if needed
                "active": True,
                "created_at": now,
                "updated_at": now,
            },
        )
        await db.commit()

        logger.info("New User created")
        # Step 5: Fetch newly created user
        result = await db.execute(text(user_exists_query), {"email": email})
        new_user = result.mappings().first()
        
        access_token = create_access_token({"sub": username})
        refresh_token = create_refresh_token({"sub": username})
        
        data["message"] = "New User created"
        data["data"]["access_token"] = access_token
        data["data"]["refresh_token"] = refresh_token
        data["data"]["user_id"] = new_user.id
        data["data"]["email"] = new_user.email
        logger.info("Successfully run login api")
        return data
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error in login api: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})
        
        
@router.post("/register")
async def register(data: RegisterUserSchema, db: AsyncSession = Depends(get_db)):
    try:
        """ Register user on central server"""
            
        headers = {"Content-Type": "application/json"}

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(f"{central_server_login}/v2/erp/register", json=data.model_dump(), headers=headers)
                response_data = response.json()
            except Exception as e:
                logger.error("Central server register failed with error: {e}")
                raise HTTPException(status_code=404, detail={"status":"error","message":f"Invalid Credentials {str(e)}","error_code":str(e)})
            
        if response_data.get("status") == "error":
            logger.error(f"Registration failed: {response.get('error')}")
            raise HTTPException(status_code=404, detail={"message": f"Registration failed: {response.get('error')}", "error_code":response.get("error")})  
        
        logger.info(f"{data.userName} User registered successfully")
        return {
            "status":"success",
            "message":"User registered successfully"
        }  
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error in Register api: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})
        
        
@router.post("/refresh-token")
async def refresh_access_token(
    payload: RefreshTokenRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Return refresh token that is generated from access token.
    If refresh token is also expired then send appropriate error message.
    """
    try:
        decoded_token = await decode_refresh_token(payload.refresh_token)

        username = decoded_token.get("sub")
        if not username:
            raise HTTPException(
                status_code=401,
                detail={"status": "error", "message": "Invalid refresh token payload"}
            )

        # Check user still exists and is active
        user_query = text("""
            SELECT id, username, email, active
            FROM users
            WHERE username = :username
            LIMIT 1
        """)

        result = await db.execute(user_query, {"username": username})
        user = result.mappings().first()

        if not user:
            raise HTTPException(
                status_code=401,
                detail={"status": "error", "message": "User not found"}
            )
        if not user["active"]:
            raise HTTPException(
                status_code=401,
                detail={"status": "error", "message": "User is inactive"}
            )

        # NEW access token
        new_access_token = create_access_token({"sub": username})

        logger.info("Refresh token api completed successfully")
        return {
            "status": "success",
            "message": "Access token refreshed successfully",
            "data": {
                "access_token": new_access_token
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error in refresh token api: {e}")
        raise HTTPException(
            status_code=500,
            detail={"status": "error", "message": str(e).split("\n")[0][:100]}
        )