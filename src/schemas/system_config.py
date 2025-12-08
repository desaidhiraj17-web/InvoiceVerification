from pydantic import BaseModel
from typing import Optional

class SystemConfigSchema(BaseModel):
    id: str
    update_quantity_enabled: bool
    picker_enabled: bool
    checker_enabled: bool
    packed_enabled: bool
    rack_enabled: bool
    show_actual_qty: bool
    updated_at: str

    class Config:
        from_attributes = True
        
        
class SystemConfigUpdateSchema(BaseModel):
    update_quantity_enabled: Optional[bool] = None
    picker_enabled: Optional[bool] = None
    checker_enabled: Optional[bool] = None
    packed_enabled: Optional[bool] = None
    rack_enabled: Optional[bool] = None
    show_actual_qty: Optional[bool] = None