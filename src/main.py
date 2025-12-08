from fastapi import FastAPI
from src.routers import auth,system_config, invoices, products
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from fastapi.openapi.utils import get_openapi
from src.constants import api_prefix
from src.db.database import async_session, engine, Base
from sqlalchemy import text
import uuid
from datetime import datetime

@asynccontextmanager
async def lifespan(app: FastAPI):
    # #  Create tables if not exist
    # async with engine.begin() as conn:
    #     await conn.run_sync(Base.metadata.create_all)

    #  Insert one default row in system_config if table empty
    async with async_session() as session:
        system_config_id = str(uuid.uuid4())
        updated_at = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

        await session.execute(
            text("""
                INSERT INTO system_config (
                    id, update_quantity_enabled, picker_enabled,
                    checker_enabled, packed_enabled, rack_enabled, show_actual_qty, updated_at
                )
                SELECT :id, 1, 0, 1, 0, 0, 0, :updated_at
                WHERE NOT EXISTS (SELECT 1 FROM system_config)
            """),
            {"id": system_config_id, "updated_at": updated_at}
        )
        await session.commit()

    yield  # Hand control back to FastAPI runtime

    # Optional cleanup after shutdown
    pass


app = FastAPI(title="Invoice Verification", lifespan=lifespan)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] ,  # or ["*"] to allow all (not recommended for production)
    allow_credentials=True,
    allow_methods=["*"],    # GET, POST, PUT, DELETE
    allow_headers=["*"],    # allow all headers
)


app.include_router(auth.router, prefix=f"{api_prefix}/auth")
app.include_router(system_config.router, prefix=f"{api_prefix}/settings")
app.include_router(invoices.router, prefix=f"{api_prefix}/invoices")
app.include_router(products.router, prefix=f"{api_prefix}/products")
# app.include_router(auth.router, prefix=f"/auth")
# app.include_router(system_config.router, prefix=f"/settings")
# app.include_router(invoices.router, prefix=f"/invoices")

