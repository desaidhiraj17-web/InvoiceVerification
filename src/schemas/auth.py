from pydantic import BaseModel, Field
from typing import Union, Optional

class TokenResponse(BaseModel):
    access_token: str
    message : str
    status_code : int

class LoginUser(BaseModel):
    username: str
    password: str
    
    
class RefreshTokenRequest(BaseModel):
    refresh_token: str
    
    
class RegisterUserSchema(BaseModel):
    userName: Union[str, int] = Field(..., description="Username of the client")
    name: Union[str, int] = Field(..., description="Full name of the client")
    password: Union[str, int] = Field(..., description="Password for authentication")
    erpCode: Union[str, int] = Field(..., description="ERP system code")
    installationId: Union[str, int] = Field(..., description="Installation ID")
    clientId: Union[str, int] = Field(..., description="Unique client ID")
    email1: Union[str, int] = Field(..., description="Primary email address")
    phone1: Union[str, int] = Field(..., description="Primary phone number")
    clientType: Union[str, int] = Field(..., description="Client type")
    address: Union[str, int] = Field(..., description="Address of the client")
    city: Union[str, int] = Field(..., description="City name")
    pinCode: Union[str, int] = Field(..., description="PIN/ZIP code")
    gst: Union[str, int] = Field(None, description="GST number (lowercase key)")
    
    email2: Optional[Union[str, int]] = Field(None, description="Secondary email address")
    state:Optional[Union[str, int]] = Field(None, description="State name")
    phone2: Optional[Union[str, int]] = Field(None, description="Secondary phone number")
    pan: Optional[Union[str, int]] = Field(None, description="PAN number (lowercase key)")
    dl1: Optional[Union[str, int]] = Field(None, description="Drug license 1 (lowercase key)")
    dl2: Optional[Union[str, int]] = Field(None, description="Drug license 2 (lowercase key)")
    dl3: Optional[Union[str, int]] = Field(None, description="Drug license 3 (lowercase key)")
    dl4: Optional[Union[str, int]] = Field(None, description="Drug license 4 (lowercase key)")
    