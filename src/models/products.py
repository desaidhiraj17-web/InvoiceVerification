from sqlalchemy import Column, String, Float, ForeignKey, Integer, UniqueConstraint
from sqlalchemy.orm import relationship
from src.db.database import Base
import datetime

class ProductMaster(Base):
    __tablename__ = "product_master"

    id = Column(String, primary_key=True, index=True)
    item_code = Column(String, nullable=False)
    product_name = Column(String, nullable=False)
    batch_number = Column(String, nullable=False)
    expiry_date = Column(String, nullable=False)  # Format: MM-YYYY
    mfg_date = Column(String, nullable=True)     # Format: MM-YYYY
    mrp = Column(Float, nullable=False)
    rack_no = Column(String, nullable=True, default="0")
    division = Column(String, nullable=True)
    obatch = Column(String, nullable=True)
    barcode1 = Column(String, nullable=True)
    barcode2 = Column(String, nullable=True)
    optional1 = Column(String, nullable=True)
    optional2 = Column(String, nullable=True)

    # rack_id = Column(String, ForeignKey("rack_info.id"), nullable=True)
    updated_by = Column(String, ForeignKey("users.id"), nullable=True)

    created_at = Column(String, default=lambda: datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
    updated_at = Column(String, default=lambda: datetime.now().strftime("%d-%m-%Y %H:%M:%S"), onupdate=lambda: datetime.now().strftime("%d-%m-%Y %H:%M:%S"))

    # --- Relationships (optional, if other models exist) ---
    # rack = relationship("RackInfo", back_populates="products", lazy="joined", uselist=False)
    updater = relationship("User", back_populates="updated_products", lazy="joined", uselist=False)
    
    __table_args__ = (
        UniqueConstraint('item_code', 'batch_number', 'expiry_date', 'mrp', name='unique_product_batch'),
    )
    
    
class ProductQtyConverter(Base):
    __tablename__ = "product_qty_converter"

    id = Column(String, primary_key=True, index=True)
    product_name = Column(String, unique=True, nullable=False)
    item_code = Column(String, nullable=True)
    shipper_val = Column(Integer, nullable=True)
    box_val = Column(Integer, nullable=True)
    strip_val = Column(Integer, nullable=False, default=1)

    created_at = Column(
        String,
        default=lambda: datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    )
    updated_at = Column(
        String,
        default=lambda: datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
        onupdate=lambda: datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    )

    updated_by = Column(String, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    updater = relationship("User", back_populates="updated_product_qty", lazy="joined")
    
    

class RackMaster(Base):
    __tablename__ = "rack_master"

    id = Column(Integer, primary_key=True, index=True)
    rack_no = Column(String(100), nullable=False, unique=True)
    rack_name = Column(String(255), nullable=False)
    user_assigned = Column(Integer, ForeignKey("users.id",ondelete="SET NULL"), nullable=True)
    updated_at = Column(String,
        default=lambda: datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
        onupdate=lambda: datetime.now().strftime("%d-%m-%Y %H:%M:%S"))

    user = relationship("User", backref="racks")


class TrayMaster(Base):
    __tablename__ = "tray_master"

    id = Column(Integer, primary_key=True, index=True)
    tray_no = Column(String(100), unique=True,nullable=False)
    tray_qr_value = Column(String(255), nullable=True)
    current_invoice_no = Column(String, ForeignKey("invoices.id",ondelete="SET NULL"), nullable=True)

    invoice = relationship("Invoice", back_populates="trays")