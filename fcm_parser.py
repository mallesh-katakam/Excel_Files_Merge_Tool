"""
FCM (Flight Centre Management) File Parser
Handles special parsing logic for FCM files with different sector and PNR/ticket number formats.
"""

import re
import logging
from typing import List, Optional

logger = logging.getLogger(__name__)


def is_fcm_file(filename: str) -> bool:
    """
    Check if the filename contains 'fcm' (case-insensitive).
    
    Args:
        filename: Name of the file to check
        
    Returns:
        True if filename contains 'fcm', False otherwise
    """
    if not filename:
        return False
    return 'fcm' in filename.lower()


def parse_fcm_sector(sector: str) -> List[str]:
    """
    Parse FCM sector format where single alphabetic characters are used as delimiters
    instead of '/'. 
    
    FCM Format Examples:
        'PHL-DOH-T-DOH-BOM-T' -> ['PHL-DOH', 'DOH-BOM']
        'MAA-BOM-U-BOM-SFO-U' -> ['MAA-BOM', 'BOM-SFO']
        'DEL-BOM-X-BOM-LHR-X-LHR-JFK-X' -> ['DEL-BOM', 'BOM-LHR', 'LHR-JFK']
    
    Logic:
    1. Split the sector string by single alphabetic characters (A-Z) that appear as segments
    2. Each segment between delimiters represents a flight sector
    3. Ignore trailing single alphabetic characters
    
    Args:
        sector: Sector string in FCM format
        
    Returns:
        List of individual sector strings (e.g., ['PHL-DOH', 'DOH-BOM'])
    """
    if not sector or not isinstance(sector, str):
        return []
    
    sector = sector.strip()
    if not sector:
        return []
    
    # Split by single uppercase letter surrounded by hyphens (e.g., -T-, -U-, -X-)
    # Pattern: Look for patterns like "-X-" where X is a single letter
    # We'll split the string and extract airport pairs
    
    sectors = []
    parts = sector.split('-')
    
    # Process parts to identify sectors
    # Single alphabetic parts are delimiters, others are airports
    current_airports = []
    
    for i, part in enumerate(parts):
        part = part.strip()
        if not part:
            continue
            
        # Check if this is a single alphabetic character (delimiter)
        if len(part) == 1 and part.isalpha():
            # This is a delimiter
            # If we have accumulated airports, form a sector
            if len(current_airports) >= 2:
                # Take first and last airport to form sector
                sector_str = f"{current_airports[0]}-{current_airports[-1]}"
                sectors.append(sector_str)
                # Reset and start new sector with the last airport
                current_airports = [current_airports[-1]]
            else:
                # Not enough airports, reset
                current_airports = []
        else:
            # This is an airport code (should be 3 letters typically)
            if part.isalpha() and len(part) >= 2:  # Airport codes are typically 3 letters
                current_airports.append(part.upper())
    
    # Handle any remaining airports (form a sector if we have at least 2)
    if len(current_airports) >= 2:
        sector_str = f"{current_airports[0]}-{current_airports[-1]}"
        sectors.append(sector_str)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_sectors = []
    for sector in sectors:
        if sector not in seen:
            seen.add(sector)
            unique_sectors.append(sector)
    
    logger.debug(f"FCM sector parsed: '{sector}' -> {unique_sectors}")
    return unique_sectors


def parse_fcm_pnr_or_ticket(value: str) -> str:
    """
    Parse PNR or ticket number for FCM files.
    
    FCM Format: If '-' exists, consider only what's before the '-'
    Examples:
        'Q7W2QG-1/1' -> 'Q7W2QG'
        'ABC123-2/3' -> 'ABC123'
        'XYZ789' -> 'XYZ789' (no change if no '-')
        
    Args:
        value: PNR or ticket number string
        
    Returns:
        Cleaned PNR/ticket number string
    """
    if not value or not isinstance(value, str):
        return str(value) if value else ''
    
    value = str(value).strip()
    
    # If '-' exists, take only the part before it
    if '-' in value:
        cleaned = value.split('-')[0].strip()
        logger.debug(f"FCM PNR/Ticket cleaned: '{value}' -> '{cleaned}'")
        return cleaned
    
    return value


def split_fcm_multi_sector(sector: str) -> List[str]:
    """
    Split multi-sector route for FCM files.
    This is the main function to be used instead of the standard split_multi_sector
    for FCM files.
    
    Args:
        sector: Sector string (can be in FCM format or standard format)
        
    Returns:
        List of individual sectors
    """
    return parse_fcm_sector(sector)


def generate_fcm_ticket_variations(ticket_number: str) -> List[str]:
    """
    Generate ticket number variations for FCM files with different prefixes.
    
    For FCM files, ticket numbers may need to be queried with various prefixes.
    This function generates all possible variations to try when looking up 
    ticket numbers in the database.
    
    Prefixes to try: 607, 098, 176, 125, 057, 074, 157, 220
    
    Each prefix is tried in two formats:
    - Direct concatenation (e.g., '1762790431640')
    - With dash separator (e.g., '176-2790431640')
    
    Args:
        ticket_number: Original ticket number string
        
    Returns:
        List of ticket number variations including:
        - Original ticket number (first in list)
        - All prefix variations (with and without dashes)
        
    Example:
        >>> generate_fcm_ticket_variations('2790431640')
        ['2790431640', '6072790431640', '607-2790431640', '0982790431640', 
         '098-2790431640', '1762790431640', '176-2790431640', ...]
    """
    if not ticket_number or not isinstance(ticket_number, str):
        return [str(ticket_number) if ticket_number else '']
    
    ticket_number = ticket_number.strip()
    if not ticket_number:
        return ['']
    
    # Define prefixes to try
    prefixes = ['607', '098', '176', '125', '057', '074', '157', '220']
    
    # Start with original ticket number
    variations = [ticket_number]
    
    # Generate variations with each prefix
    for prefix in prefixes:
        # Without dash: e.g., '1762790431640'
        variations.append(f"{prefix}{ticket_number}")
        # With dash: e.g., '176-2790431640'
        variations.append(f"{prefix}-{ticket_number}")
    
    logger.debug(f"FCM ticket variations for '{ticket_number}': {len(variations)} variations generated")
    return variations



