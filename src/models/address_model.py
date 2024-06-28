from typing import Optional
from pydantic import BaseModel


class AddressQuery(BaseModel):
    address: str
