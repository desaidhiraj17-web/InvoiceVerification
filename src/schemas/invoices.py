from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
from datetime import date, datetime
import calendar
from src.models.invoices import InvoiceStatus, ScanStatusEnum, OperationTypeEnum, OperationStatus
from src.logger.logger_setup import logger


class InvoiceMetadataUpdateSchema(BaseModel):
    picker_start: Optional[int] = None
    picker_end: Optional[int] =  None

    checker_start: Optional[int] = None
    checker_end: Optional[int] = None 

    packer_start: Optional[int] = None 
    packer_end: Optional[int] = None 
    status: InvoiceStatus = None
    
    
class InvoiceProductActionSchema(BaseModel):
    action: str  # "add" or "delete"
    product_id: Optional[str] = None  # required for delete, optional for add
    product_name: Optional[str] = None
    batch_number: Optional[str] = None
    expiry_date: Optional[str] = None
    mrp: Optional[float] = 0.0
    actual_qty: Optional[float] = 0.0
    scanned_qty: Optional[float] = 0.0
    rack_no : Optional[int] = None
    scan_status: Optional[ScanStatusEnum] = None
    
    @field_validator("expiry_date", mode="before")
    @classmethod
    def validate_and_format_expiry(cls, value:str):
        if not value:
            return value
        try:
            month, year = map(int, value.strip().split("-"))
        except Exception:
            raise ValueError("expiry_date must be in MM-YYYY format (e.g. '12-2025')")
        if not (1 <= month <= 12):
            logger.error(f"Month must be between 1 and 12. {month} not allowed")
            raise ValueError(f"Month must be between 1 and 12. {month} not allowed")

            # convert MM-YYYY → DD-MM-YYYY
        last_day = calendar.monthrange(year, month)[1]
        return f"{last_day:02d}-{month:02d}-{year}"
    
    
class TransactionItem(BaseModel):
    timestamp: Optional[int] = None
    operation_type: OperationTypeEnum
    operation_status: OperationStatus
    scan_status: ScanStatusEnum  
    image: Optional[str] = None
    invoice_product_id: Optional[str] = None
    


class TransactionAdd(BaseModel):
    invoice_id: str
    rack_id: Optional[int] = None
    products: List[TransactionItem]
    
    
class PerformanceDashboardFilter(BaseModel):
    from_date: Optional[str] = Field(None, description="DD-MM-YYYY")
    to_date: Optional[str] = Field(None, description="DD-MM-YYYY")
    operator_id: Optional[bool] = None
    invoice_id: Optional[str] = None

    @field_validator("from_date", "to_date")
    @classmethod
    def validate_dd_mm_yyyy(cls, v):
        if v is None:
            return v

        try:
            dt = datetime.strptime(v, "%d-%m-%Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            logger.error("Date must be in DD-MM-YYYY format")
            raise ValueError("Date must be in DD-MM-YYYY format")
