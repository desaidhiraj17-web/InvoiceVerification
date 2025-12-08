from fastapi import HTTPException
from src.logger.logger_setup import logger
from sqlalchemy import text


# async def check_email_exists(db, email: str) -> bool:
#     try:
#         query = """
#             SELECT 1
#             FROM users
#             WHERE email = :email
#             LIMIT 1
#         """
#         result = await db.execute(text(query), {"email": email})
#         row = result.first()
#         if row:
#             raise HTTPException(status_code=409, detail={"status":"error", "message": f"Email already exists {email}"}) 
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.exception(f"inside check_email_exists: {e}")
#         raise HTTPException(status_code=404, detail={"status" : "error",
#                 "message" : str(e).split("\n")[0][:100]})  