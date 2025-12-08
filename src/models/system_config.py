from sqlalchemy import Column, String, Float, ForeignKey, Integer, Boolean
from sqlalchemy.orm import relationship
from src.db.database import Base
from datetime import datetime
import uuid


 # or import from your database.py

class SystemConfig(Base):
    __tablename__ = "system_config"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    update_quantity_enabled = Column(Boolean, default=False)
    picker_enabled = Column(Boolean, default=False)
    checker_enabled = Column(Boolean, default=False)
    packed_enabled = Column(Boolean, default=False)
    rack_enabled = Column(Boolean, default=False)
    show_actual_qty = Column(Boolean, default=False)
    updated_at = Column(
        String,
        default=lambda: datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
        onupdate=lambda: datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    )
    
    
