"""
This module normalizes us address data using usaddress-scourgify
https://github.com/GreenBuildingRegistry/usaddress-scourgify
"""
from scourgify import normalize_address_record, NormalizeAddress

def normalize_address_record(address: any) -> dict:
    """
    Normalizes a single address record
    """
    if isinstance(address, str):
        address = NormalizeAddress(address).normalize()
    return normalize_address_record(address)
