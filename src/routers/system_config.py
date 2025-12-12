import qrcode, socket, io
from fastapi.responses import StreamingResponse
from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from src.models.system_config import SystemConfig
from src.models.auth import User
from src.services.user_services import get_current_user
from src.db.database import get_db
from sqlalchemy.future import select
from sqlalchemy import or_, text
import httpx
from src.schemas.system_config import SystemConfigSchema, SystemConfigUpdateSchema
from typing import List
from src.logger.logger_setup import logger

router = APIRouter(tags=["System Config"])

@router.get("/")
async def get_settings(db: AsyncSession = Depends(get_db)):
    """ Fetches the system configuration settings from the database.
        Returns a single active configuration record. """
    try:
        logger.info("get settings api started")

        query = text("SELECT * FROM system_config LIMIT 1")
        result = await db.execute(query)

        row = result.mappings().first()  

        if not row:
            logger.exception("setting Object doesn't exist")
            raise HTTPException(status_code=404, detail={"status":"error","message": "Object doesn't exist"})

        config_obj = SystemConfigSchema(**row)

        return {"status":"success","message":"settting fetched successfully","data":config_obj}

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"get_settings api: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})


@router.put("/", response_model=dict)
async def update_settings(data: SystemConfigUpdateSchema, db: AsyncSession = Depends(get_db)):
    """ Updates the system configuration settings with only the provided fields.
        Returns the updated configuration values upon successful modification. """
    try:
        logger.info("update settings api started")

        settings_query = "select * from system_config LIMIT 1;"
        result = await db.execute(text(settings_query))
        config = result.mappings().first()

        if not config:
            logger.error("SystemConfig object not found")
            raise HTTPException(status_code=404, detail={"status":"error","message":"SystemConfig object not found"})

        # update only provided fields
        update_data = data.model_dump(exclude_unset=True)
        if not update_data:
            logger.error("No fields provided for update")
            raise HTTPException(status_code=400, detail={"message": "No fields provided for update","status":"error"})
        
        set_clause = ", ".join([f"{key} = :{key}" for key in update_data.keys()])

        update_query = f"""
            UPDATE system_config
            SET {set_clause}, updated_at = :updated_at
            WHERE id = :id;
        """

        update_params = {
            **update_data,
            "updated_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
            "id": config["id"]
        }
        await db.execute(text(update_query), update_params)
        await db.commit()
        logger.info("System configuration updated successfully")
        
        return {"status":"success","message": "System configuration updated successfully", "data": update_data}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("update_settings api {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})
        
        

@router.get("/health", response_model=dict) 
async def health_view():
    return { 
            "status": "success",
            "version": "1.0.0",
            "time": "2025-10-08T09:52:32Z",
            "service": "auth|lan|admin",
            "message": "success"
            }


@router.get("/generate-qr")
async def generate_qr(request: Request,db: AsyncSession = Depends(get_db),current_user: User = Depends(get_current_user)):
    """ Generates a QR code containing the server IP address and port.
        Returns the QR image as a PNG streaming response. """
    try:
        client_ip = request.client.host
        
        server_ip = socket.gethostbyname(socket.gethostname())
        
        print("client_ip",client_ip,"server_ip",server_ip)
        # if client_ip != server_ip:
        #     logger.exception("client and server not in same LAN")
        #     raise HTTPException(status_code=400, detail={"status":"error","message":"client and server not in same LAN"})
        

        port = request.url.port or 8000
        qr_data = {"port":f"{port}", "ip_address":f"{server_ip}"}
        # qr_data = f"http://{ip_address}:{port}"

        qr_img = qrcode.make(qr_data)

        # Save to buffer
        buffer = io.BytesIO()
        qr_img.save(buffer, format="PNG")
        buffer.seek(0)
        logger.info("QR code generated")
        # Return as image response
        return StreamingResponse(buffer, media_type="image/png")
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("QR code api failed: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})