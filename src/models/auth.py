from sqlalchemy import Column, Integer, String, DateTime, Enum, ForeignKey, Boolean
import enum
from datetime import datetime
from src.models.parties import PartyMaster
from src.models.products import ProductMaster, ProductQtyConverter
from src.models.invoices import Transaction, InvoiceMetadata


from src.db.database import Base  
from sqlalchemy.orm import relationship, backref

class UserTypeEnum(str, enum.Enum):
    admin = "admin"
    operator = "operator"
    picker = "picker"
    packer = "packer"
    checker = "checker"
    
    

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    # username = Column(String(50), unique=True, index=True, nullable=False)
    # email = Column(String(100), unique=True, index=True, nullable=False)
    # hashed_password = Column(String, nullable=False)
    
    user_type = Column(Enum(UserTypeEnum), nullable=True, default=None)
    first_name = Column(String, nullable=False)
    middle_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    
    # rack_no = Column(String, ForeignKey("rack_master.id"), nullable=True)
    # rack = relationship("RackMaster")
    
    erp_id = Column(String, nullable=True)
    erp_name = Column(String, nullable=True)
    
    email = Column(String(100), nullable=False, index=True)
    phone = Column(String, nullable=True)
    
    username = Column(String, unique=True, nullable=False, index=True)
    hashed_password = Column(String, nullable=False)
    
    created_at = Column(String, default=lambda: datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
    updated_at = Column(String, default=lambda: datetime.now().strftime("%d-%m-%Y %H:%M:%S"), onupdate=lambda: datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
    
    active = Column(Boolean, default=True)

    # --- Relationships ---
    updated_parties = relationship("PartyMaster", back_populates="updater")
    updated_products = relationship("ProductMaster", back_populates="updater")
    updated_product_qty = relationship("ProductQtyConverter", back_populates="updater")

    transactions = relationship("Transaction", back_populates="user")

    picker_invoices = relationship("InvoiceMetadata", back_populates="picker", foreign_keys="InvoiceMetadata.picker_id")
    checker_invoices = relationship("InvoiceMetadata", back_populates="checker", foreign_keys="InvoiceMetadata.checker_id")
    packer_invoices = relationship("InvoiceMetadata", back_populates="packer", foreign_keys="InvoiceMetadata.packer_id")