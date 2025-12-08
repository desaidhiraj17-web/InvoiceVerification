from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
from datetime import date
import calendar
from src.logger.logger_setup import logger
from src.models.invoices import ScanStatusEnum

class MatchScanRequest(BaseModel):
    invoice_id: str
    rack_id: Optional[str] = None
    batch_number: str
    expiry_date: str
    mfg_date: Optional[str] = None
    mrp: float
    barcode1: Optional[str] = None
    barcode2: Optional[str] = None
    
    @field_validator("expiry_date", "mfg_date", mode="before")
    @classmethod
    def validate_date_format(cls, value, info):
        if not value:
            return value
        try:
            month, year = map(int, value.strip().split("-"))
            if not (1 <= month <= 12):
                logger.error(f" month should be in 1 to 12. {month} not allowed")
                raise ValueError
            # convert MM-YYYY → DD-MM-YYYY
            if info.field_name == "expiry_date":
                last_day = calendar.monthrange(year, month)[1]
                return f"{last_day:02d}-{month:02d}-{year}"
            else:
                return f"01-{month:02d}-{year}"
        except Exception:
            raise ValueError(f"{info.field_name} must be in MM-YYYY format (e.g. '12-2025')")
        
        

class UpdateTrayInvoiceRequest(BaseModel):
    invoice_id: str | None  # can be null to remove the invoice
    
    
class ProductScanUpdate(BaseModel):
    invoice_id: str
    product_name: str
    product_id:str
    # batch_number: str
    # expiry_date: str
    # mrp: float
    scanned_qty: float
    shipper_val: Optional[int] = None
    box_val: Optional[int] = None
    strip_val: Optional[int] = None
    
    # @field_validator("expiry_date", mode="before")
    # @classmethod
    # def validate_and_format_expiry(cls, value:str):
    #     if not value:
    #         return value
    #     try:
    #         month, year = map(int, value.strip().split("-"))
    #     except Exception:
    #         raise ValueError("expiry_date must be in MM-YYYY format (e.g. '12-2025')")
    #     if not (1 <= month <= 12):
    #         logger.error(f"Month must be between 1 and 12. {month} not allowed")
    #         raise ValueError(f"Month must be between 1 and 12. {month} not allowed")

    #         # convert MM-YYYY → DD-MM-YYYY
    #     last_day = calendar.monthrange(year, month)[1]
    #     return f"{last_day:02d}-{month:02d}-{year}"
        

class ProductItem(BaseModel):
    product_name: str
    product_id: str
    scanned_qty: float
    shipper_val: Optional[int] = None
    box_val: Optional[int] = None
    strip_val: Optional[int] = None
    scan_status: Optional[ScanStatusEnum] = None


class ProductScanQtyUpdate(BaseModel):
    invoice_id: str
    completed: bool = False
    products: List[ProductItem]
    
    
class ProductQtyConverterResponse(BaseModel):
    id: str
    product_name: str
    item_code: Optional[str]
    shipper_val: Optional[int]
    box_val: Optional[int]
    strip_val: int
    created_at: Optional[str] = None   
    updated_at: Optional[str] = None  

class ProductQtyConverterListResponse(BaseModel):
    status: str
    total: int
    page: int
    page_size: int
    data: List[ProductQtyConverterResponse]
    
    
class UpdateProductQtyConverterSchema(BaseModel):
    product_name: str
    shipper_val: Optional[int] = None
    box_val: Optional[int] = None
    strip_val: Optional[int] = None