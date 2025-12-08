from sqlalchemy import Column, String, Integer, ForeignKey, Enum, Float, ARRAY, JSON, UniqueConstraint
from sqlalchemy.orm import relationship
from datetime import datetime
from src.db.database import Base
import enum

class PriorityLevel(str, enum.Enum):
    HIGH = 1
    MEDIUM = 2
    LOW = 3
    

class InvoiceStatus(str, enum.Enum):
    not_started = "not_started"
    checking_start = "checking_start"
    checking_end = "checking_end"
    picking_start = "picking_start"
    picking_end = "picking_end"
    # started = "started"
    # picked = "picked"
    # checked = "checked"
    # packed = "packed"
    completed = "completed"
    
class InvoiceType(str, enum.Enum):
    purchase = "purchase"
    sell = "sell"


class Invoice(Base):
    __tablename__ = "invoices"

    id = Column(String, primary_key=True, index=True)

    # 1 = High, 2 = Medium, 3 = Low
    priority = Column(Enum(PriorityLevel), nullable=False)

    invoice_no = Column(String, unique=True, nullable=False)
    invoice_date = Column(String, nullable=False)  # Format: DD-MM-YYYY

    party_id = Column(String, ForeignKey("party_master.id", ondelete="SET NULL"), nullable=True)
    
    invoice_type = Column(Enum(InvoiceType), default="purchase", nullable=True)

    # Possible values: not_started / picked / checked / packed / completed
    status = Column(Enum(InvoiceStatus), default="not_started", nullable=False)

    created_at = Column(
        String,
        default=lambda: datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    )
    started_at = Column(String, nullable=True)
    updated_at = Column(
        String,
        default=lambda: datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
        onupdate=lambda: datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    )

    # Relationships
    party = relationship("PartyMaster", back_populates="invoices", passive_deletes=True)
    invoice_products = relationship("InvoiceProductList", back_populates="invoice", passive_deletes=True)
    invoice_metadata = relationship("InvoiceMetadata", back_populates="invoice", uselist=False, passive_deletes=True)
    transactions = relationship("Transaction", back_populates="invoice", passive_deletes=True)
    trays = relationship("TrayMaster", back_populates="invoice", passive_deletes=True)
    

class ScanStatusEnum(str, enum.Enum):
    success = "success"
    auto_confirm = "auto_confirm"
    auto_fallback = "auto_fallback"
    auto_multi = "auto_multi"
    manual = "manual"

class InvoiceProductList(Base):
    __tablename__ = "invoice_product_list"

    id = Column(String, primary_key=True, index=True)
    invoice_id = Column(String, ForeignKey("invoices.id", ondelete="SET NULL"), nullable=True)
    product_name = Column(String, nullable=False)
    rack_no = Column(String, nullable=True, default="0")
    batch_number = Column(String, nullable=False)
    expiry_date = Column(String, nullable=False)  # Format: MM-YYYY
    mrp = Column(Float, nullable=False)
    actual_qty = Column(Float, nullable=False, default=0.0)
    scanned_qty = Column(Float, nullable=False, default=0.0)
    scan_status = Column(Enum(ScanStatusEnum), nullable=True) 
    # Relationships
    invoice = relationship("Invoice", back_populates="invoice_products")
    # rack = relationship("RackMaster", back_populates="invoice_products", lazy="joined")
    
    


class InvoiceMetadata(Base):
    __tablename__ = "invoice_metadata"

    id = Column(String, primary_key=True, index=True)
    invoice_id = Column(String, ForeignKey("invoices.id", ondelete="SET NULL"), nullable=True, unique=True)

    # Picker details
    picker_id = Column(String, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    picker_start = Column(String, nullable=True)  # Format: DD-MM-YYYY HH:MM:SS
    picker_end = Column(String, nullable=True)

    # Checker details
    checker_id = Column(String, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    checker_start = Column(String, nullable=True)
    checker_end = Column(String, nullable=True)

    # Packer details
    packer_id = Column(String, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    packer_start = Column(String, nullable=True)
    packer_end = Column(String, nullable=True)

    # Tray list — stores multiple tray IDs from tray_master
    tray_list = Column(JSON, nullable=True)

    # Relationships
    invoice = relationship("Invoice", back_populates="invoice_metadata",uselist=False)
    picker = relationship("User", foreign_keys=[picker_id])
    checker = relationship("User", foreign_keys=[checker_id])
    packer = relationship("User", foreign_keys=[packer_id])
    
    
class OperationTypeEnum(str, enum.Enum):
    scan = "scan"
    qty_change = "qty_change"
    

class OperationStatus(str, enum.Enum):
    checker_end = "checker_end"
    picker_end = "picker_end"
    packer_end = "packer_end"
    

class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(String, primary_key=True, index=True)
    timestamp = Column(String, default=lambda: datetime.now().strftime("%d-%m-%Y %H:%M:%S"))

    invoice_id = Column(String, ForeignKey("invoices.id", ondelete="SET NULL"), nullable=True)
    user_id = Column(String, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    rack_id = Column(Integer, ForeignKey("rack_master.id"), nullable=True)
    invoice_product_id = Column(String, ForeignKey("invoice_product_list.id", ondelete="SET NULL"), nullable=True)
    
    operation_status = Column(Enum(OperationStatus), nullable=True)
    
    operation_type = Column(Enum(OperationTypeEnum), nullable=False)
    scan_status = Column(Enum(ScanStatusEnum), nullable=False)
    image = Column(String, nullable=True)  # store base64 encoded string

    # Relationships
    invoice = relationship("Invoice", back_populates="transactions", lazy="joined")
    user = relationship("User", back_populates="transactions", lazy="joined")
    rack = relationship("RackMaster", lazy="joined")
    invoice_product = relationship("InvoiceProductList", lazy="joined")
    


class PerformanceMetrics(Base):
    __tablename__ = "performance_metrics"

    __table_args__ = (
        UniqueConstraint("invoice_id", "operation_status", name="uq_invoice_operation"),
    )

    id = Column(String, primary_key=True, index=True)

    # Foreign Keys (Both nullable)
    operator_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    invoice_id = Column(String, ForeignKey("invoices.id", ondelete="SET NULL"), nullable=True)
    operation_status = Column(Enum(OperationStatus), nullable=True)

    # Time fields (nullable)
    invoice_start_time = Column(String, nullable=True)   # Format: YYYY-MM-DD HH:MM:SS
    invoice_end_time = Column(String, nullable=True)

    # Performance metrics (ALL nullable)
    line_items = Column(Integer, nullable=True)
    time_to_pick = Column(Integer, nullable=True)
    total_scans = Column(Integer, nullable=True)
    median_time_btw_2_scans = Column(Integer, nullable=True)
    accuracy = Column(Float, nullable=True)   # Percentage (0–100)

    # Timestamps
    created_at = Column(
        String,
        default=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        nullable=True
    )

    updated_at = Column(
        String,
        default=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        onupdate=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        nullable=True
    )

    # Relationships
    operator = relationship("User", backref="performance_metrics")
    invoice = relationship("Invoice", backref="performance_metrics", uselist=False)