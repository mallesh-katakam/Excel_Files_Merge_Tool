"""
Data Merge Tool - Production Version
Automated data enrichment tool for processing Excel/CSV files with database information.
Supports multi-file processing, automated scheduling, SFTP integration, and email notifications.
"""
import pandas as pd
import mysql.connector
from mysql.connector import Error
from typing import List, Dict, Optional
import logging
import os
import time
from pathlib import Path
import glob
import schedule
import threading
from datetime import datetime
import json
import paramiko
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, BotoCoreError
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from data_splitter import split_to_sheets
from invoice_deduplicator import process_multiple_files
from gstr_2b_3b_merger import merge_gstr_data
from fcm_parser import is_fcm_file, parse_fcm_sector, parse_fcm_pnr_or_ticket, generate_fcm_ticket_variations, expand_slash_notation_ticket
from booking_date_normalizer import normalize_excel_booking_date, normalize_db_booking_date

# Configure enhanced logging for automated execution
log_file = f"data_merge_{datetime.now().strftime('%Y%m%d')}.log"
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ====================================================================
# CONFIGURATION LOADING
# ====================================================================

def load_config(config_path: str = "config.json") -> Dict:
    """Load configuration from JSON file."""
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        logger.info(f"Configuration loaded from {config_path}")
        return config
    except FileNotFoundError:
        logger.error(f"Configuration file {config_path} not found")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing configuration file: {e}")
        raise
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        raise

# Load configuration
CONFIG = load_config()

# Extract configuration values
INPUT_DIRECTORY = CONFIG["input_directory"]
OUTPUT_DIRECTORY = CONFIG["output_directory"]
SUPPORTED_EXTENSIONS = CONFIG["supported_extensions"]
DB_CONFIG = CONFIG["database"]
TABLE_NAME = CONFIG["table_name"]
COLUMN_MAPPING = CONFIG["column_mapping"]
POSSIBLE_REFERENCE_COMBINATIONS = CONFIG["possible_reference_combinations"]
BATCH_SIZE = CONFIG["processing"]["batch_size"]
MAX_RETRIES = CONFIG["processing"]["max_retries"]
CONNECTION_TIMEOUT = CONFIG["processing"]["connection_timeout"]
QUERY_TIMEOUT = CONFIG["processing"]["query_timeout"]
DEBUG_MODE = CONFIG["debug"]["debug_mode"]
DEBUG_ID = CONFIG["debug"]["debug_id"]
SFTP_CONFIG = CONFIG.get("sftp", {})
EMAIL_CONFIG = CONFIG.get("email", {})
GSTR_DIRECTORY = CONFIG.get("paths", {}).get("gstr_directory", "")

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

# ====================================================================


class FileProcessor:
    """Handles file discovery and batch processing for multiple files."""
    
    def __init__(self, input_directory: str, output_directory: str, supported_extensions: List[str]):
        self.input_directory = input_directory
        self.output_directory = output_directory
        self.supported_extensions = supported_extensions
    
    def discover_files(self) -> List[str]:
        """Discover all supported files in the input directory."""
        files = []
        try:
            for ext in self.supported_extensions:
                pattern = os.path.join(self.input_directory, f"*{ext}")
                found_files = glob.glob(pattern)
                files.extend(found_files)
            
            # Filter out temporary Excel lock files (files starting with ~$)
            files = [f for f in files if not os.path.basename(f).startswith('~$')]
            
            logger.info(f"Found {len(files)} files to process in {self.input_directory}")
            return files
        except Exception as e:
            logger.error(f"Error discovering files: {e}")
            return []
    
    def get_output_path(self, input_file: str) -> str:
        """Generate output path for processed file with unique filename."""
        filename = os.path.basename(input_file)
        name, ext = os.path.splitext(filename)
        
        # Add timestamp with microseconds for better uniqueness
        now = datetime.now()
        timestamp = now.strftime("%Y%m%d%H%M%S%f")  # Format: YYYYMMDDHHMMSSFFFFFF (includes microseconds)
        
        # Determine output extension
        if ext.lower() == '.csv':
            output_ext = '.csv'
        else:
            output_ext = '.xlsx'
        
        # Generate base output path
        output_path = os.path.join(self.output_directory, f"{name}_{timestamp}{output_ext}")
        
        # If file already exists, append a counter to make it unique
        counter = 1
        while os.path.exists(output_path):
            output_path = os.path.join(self.output_directory, f"{name}_{timestamp}_{counter}{output_ext}")
            counter += 1
        
        return output_path
    
    def move_processed_file(self, input_file: str) -> bool:
        """Move processed file to avoid reprocessing."""
        try:
            filename = os.path.basename(input_file)
            processed_dir = os.path.join(self.input_directory, "processed")
            os.makedirs(processed_dir, exist_ok=True)
            
            destination = os.path.join(processed_dir, filename)
            os.rename(input_file, destination)
            logger.info(f"Moved processed file to: {destination}")
            return True
        except Exception as e:
            logger.error(f"Error moving file {input_file}: {e}")
            return False


class DataEnricher:
    """
    Enhanced data enricher with improved error handling, retry logic, and performance optimizations.
    """
    
    def __init__(self, host: str, database: str, user: str, password: str, 
                 port: int = 3306, debug_mode: bool = False, debug_id: Optional[int] = None):
        """Initialize database connection parameters."""
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connection = None
        self.debug_mode = debug_mode
        self.debug_id = debug_id
        self.connection_attempts = 0
        self.current_file_path = None  # Track current file being processed for FCM detection
    
    def connect(self) -> bool:
        """Establish connection to MySQL database with retry logic."""
        for attempt in range(MAX_RETRIES):
            try:
                self.connection = mysql.connector.connect(
                    host=self.host,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    port=self.port,
                    connection_timeout=CONNECTION_TIMEOUT,
                    autocommit=True
                )
                if self.connection.is_connected():
                    logger.info("Connected to MySQL database")
                    self.connection_attempts = 0
                    return True
            except Error as e:
                self.connection_attempts += 1
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error("Failed to connect to database after all retries")
                    return False
        return False
    
    def disconnect(self):
        """Close database connection safely."""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Database connection closed")
    
    def validate_file(self, file_path: str) -> bool:
        """Validate file exists and is readable."""
        try:
            path = Path(file_path)
            if not path.exists():
                logger.error(f"File does not exist: {file_path}")
                return False
            if not path.is_file():
                logger.error(f"Path is not a file: {file_path}")
                return False
            if not os.access(file_path, os.R_OK):
                logger.error(f"File is not readable: {file_path}")
                return False
            return True
        except Exception as e:
            logger.error(f"Error validating file: {e}")
            return False
    
    def read_file_safely(self, file_path: str):
        """
        Read file and return either:
        - pd.DataFrame for single sheet/CSV files
        - Dict[str, pd.DataFrame] for multi-sheet Excel files
        """
        try:
            file_extension = os.path.splitext(file_path)[1].lower()
            if file_extension in ['.xlsx', '.xls', '.xlsb']:
                # Determine engine based on file extension
                if file_extension == '.xlsb':
                    engine = 'pyxlsb'
                elif file_extension == '.xls':
                    engine = None  # pandas will use xlrd automatically
                else:
                    engine = 'openpyxl'  # openpyxl needed for READING .xlsx files
                
                # Read all sheets from Excel file
                try:
                    excel_file = pd.ExcelFile(file_path, engine=engine)
                except ImportError as e:
                    if file_extension == '.xlsb':
                        logger.error(f"pyxlsb library is required to read .xlsb files. Install it with: pip install pyxlsb")
                        raise ImportError("pyxlsb library is required to read .xlsb files. Install it with: pip install pyxlsb")
                    raise
                
                sheet_names = excel_file.sheet_names
                logger.info(f"Found {len(sheet_names)} sheet(s): {sheet_names}")
                
                sheets_dict = {}
                for sheet_name in sheet_names:
                    # Detect the correct header row automatically for each sheet
                    preview = pd.read_excel(file_path, sheet_name=sheet_name, nrows=10, header=None, engine=engine)
                    header_row = None
                    for i, row in preview.iterrows():
                        # Heuristic: a row is header if most cells are strings and not NaN
                        non_null = row.dropna()
                        if len(non_null) > 2 and all(isinstance(x, str) for x in non_null):
                            header_row = i
                            break

                    if header_row is not None:
                        df_sheet = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row, engine=engine)
                    else:
                        df_sheet = pd.read_excel(file_path, sheet_name=sheet_name, engine=engine)
                    
                    # Remove unnamed columns (columns that start with "Unnamed:")
                    unnamed_cols = [col for col in df_sheet.columns if str(col).startswith('Unnamed:')]
                    if unnamed_cols:
                        df_sheet = df_sheet.drop(columns=unnamed_cols)
                    
                    if len(df_sheet) > 0:
                        sheets_dict[sheet_name] = df_sheet
                        logger.info(f"Loaded sheet '{sheet_name}' with {len(df_sheet)} rows")
                
                # Return dict if multiple sheets, single DataFrame if one sheet
                if len(sheets_dict) == 0:
                    return pd.DataFrame()
                elif len(sheets_dict) == 1:
                    df = list(sheets_dict.values())[0]
                    logger.info(f"File loaded with columns: {list(df.columns)}")
                    return df
                else:
                    logger.info(f"Returning {len(sheets_dict)} separate sheets")
                    return sheets_dict
            else:
                df = pd.read_csv(file_path)
                # Remove unnamed columns (columns that start with "Unnamed:")
                unnamed_cols = [col for col in df.columns if str(col).startswith('Unnamed:')]
                if unnamed_cols:
                    df = df.drop(columns=unnamed_cols)
                    logger.info(f"Removed {len(unnamed_cols)} unnamed columns: {unnamed_cols}")
                logger.info(f"File loaded with columns: {list(df.columns)}")
                return df
        except Exception as e:
            logger.error(f"Error reading file: {e}")
            return None

    def get_all_columns(self, table_name: str) -> List[str]:
        """Get all column names from the database table with retry logic."""
        for attempt in range(MAX_RETRIES):
            try:
                cursor = self.connection.cursor()
                cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
                columns = [column[0] for column in cursor.fetchall()]
                cursor.close()
                return columns
            except Error as e:
                logger.warning(f"Error fetching columns (attempt {attempt + 1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(1)
                else:
                    logger.error("Failed to fetch columns after all retries")
                    return []
    
    def execute_query_with_retry(self, query: str, params: List = None) -> List[Dict]:
        """Execute query with retry logic and timeout."""
        for attempt in range(MAX_RETRIES):
            try:
                cursor = self.connection.cursor(dictionary=True)
                cursor.execute(query, params or [])
                results = cursor.fetchall()
                cursor.close()
                return results
            except Error as e:
                logger.warning(f"Query failed (attempt {attempt + 1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(1)
                    # Try to reconnect if connection is lost
                    if not self.connection.is_connected():
                        logger.info("Reconnecting to database...")
                        self.connect()
                else:
                    logger.error(f"Query failed after all retries: {query}")
                    return []
    
    def is_empty_value(self, value) -> bool:
        """Check if a value is empty/null."""
        if pd.isna(value) or value is None:
            return True
        if isinstance(value, str):
            # Check for empty string or string representations of null
            stripped = value.strip().lower()
            return stripped == '' or stripped == 'null' or stripped == 'none' or stripped == 'nan'
        return False
    
    def convert_excel_date(self, value):
        """
        Convert Excel serial date number to actual date using pandas.
        Returns formatted date string 'YYYY-MM-DD' or original value if not a date.
        """
        if value is None or pd.isna(value):
            return None
        
        # If it's already a datetime object, format it
        if isinstance(value, (pd.Timestamp, datetime)):
            return value.strftime('%Y-%m-%d')
        
        # If it's a string, try to parse it
        if isinstance(value, str):
            try:
                parsed_date = pd.to_datetime(value, errors='coerce')
                if pd.notna(parsed_date):
                    return parsed_date.strftime('%Y-%m-%d')
            except:
                pass
            return value
        
        # If it's a number, use pandas to convert from Excel serial date
        if isinstance(value, (int, float)):
            try:
                # Use pandas built-in Excel date conversion
                # origin='1899-12-30' is Excel's epoch
                if 1 <= value <= 100000:  # Reasonable range for Excel dates
                    converted_date = pd.to_datetime(value, unit='D', origin='1899-12-30')
                    return converted_date.strftime('%Y-%m-%d')
            except:
                pass
        
        return value
    
    def find_column_case_insensitive(self, column_name: str, excel_columns: List[str]) -> Optional[str]:
        """Find a column name in Excel columns using case-insensitive matching."""
        column_name_lower = str(column_name).lower().strip()
        for excel_col in excel_columns:
            if str(excel_col).lower().strip() == column_name_lower:
                return excel_col
        return None
    
    def debug_log(self, message: str):
        """Log debug messages only if debug mode is enabled."""
        if self.debug_mode:
            logger.info(f"[DEBUG] {message}")
    
    def normalize_pnr_ticket_value(self, value, column_type: str) -> str:
        """
        Normalize PNR or Ticket Number value.
        For FCM files, removes suffix after '-' (e.g., 'Q7W2QG-1/1' -> 'Q7W2QG').
        
        Args:
            value: The PNR or ticket value to normalize
            column_type: Type of column ('PNR_Number' or 'Ticket_Number')
            
        Returns:
            Normalized value as string
        """
        if self.is_empty_value(value):
            return str(value) if value else ''
        
        value_str = str(value).strip()
        
        # Check if current file is an FCM file
        if self.current_file_path and is_fcm_file(os.path.basename(self.current_file_path)):
            # Use FCM parser for PNR/Ticket number cleaning
            return parse_fcm_pnr_or_ticket(value_str)
        
        # Return as-is for non-FCM files
        return value_str
    
    def normalize_ref_column_value(self, ref_col: str, value):
        """Normalize reference column values for DB matching."""
        if ref_col == 'Invoice_Total' and not self.is_empty_value(value):
            try:
                return round(float(value), 2)
            except (TypeError, ValueError):
                pass
        return value
    
    def split_multi_sector(self, sector: str) -> List[str]:
        """
        Split multi-sector route into individual sectors by '/' separator.
        For FCM files, uses single alphabetic characters as delimiters instead.
        
        Standard Examples:
            'MAA-HYD' -> ['MAA-HYD'] (no '/', returns as single sector)
            'MAA-HYD/HYD-MAA' -> ['MAA-HYD', 'HYD-MAA'] (split by '/')
            'MAA-HYD/BLR-DEL/MAA-BOM' -> ['MAA-HYD', 'BLR-DEL', 'MAA-BOM'] (split by '/')
        
        FCM File Examples:
            'PHL-DOH-T-DOH-BOM-T' -> ['PHL-DOH', 'DOH-BOM']
            'MAA-BOM-U-BOM-SFO-U' -> ['MAA-BOM', 'BOM-SFO']
        """
        if self.is_empty_value(sector):
            return []
        
        sector_str = str(sector).strip()
        
        # Check if current file is an FCM file
        if self.current_file_path and is_fcm_file(os.path.basename(self.current_file_path)):
            # Use FCM parser for sector splitting
            return parse_fcm_sector(sector_str)
        
        # Standard parsing: Split by '/' to get individual sectors
        if '/' in sector_str:
            sectors = [s.strip() for s in sector_str.split('/') if s.strip()]
            return sectors
        else:
            # No '/' found, return as single sector
            return [sector_str]
    
    def get_first_last_sector(self, sector: str) -> Optional[str]:
        """
        Get the first-last airport pattern from a multi-sector route.
        This ignores all intermediate airports.
        Examples:
            'MAA-HYD' -> 'MAA-HYD' (2 airports, no change)
            'MAA-HYD-BBI' -> 'MAA-BBI' (ignores HYD)
            'MAA-HYD-BLR-DEL' -> 'MAA-DEL' (ignores HYD, BLR)
            'MAA-HYD-BLR-DEL-BOM' -> 'MAA-BOM' (ignores HYD, BLR, DEL)
        Returns None if sector is invalid or has less than 2 airports.
        """
        if self.is_empty_value(sector):
            return None
        
        sector_str = str(sector).strip()
        airports = sector_str.split('-')
        
        # Need at least 2 airports
        if len(airports) < 2:
            return None
        
        # If only 2 airports, return as is
        if len(airports) == 2:
            return sector_str
        
        # Return first and last airport
        return f"{airports[0]}-{airports[-1]}"
    
    def get_first_last_sector_from_multi(self, sector: str) -> Optional[str]:
        """
        Get the first-last airport pattern from a multi-sector route.
        This extracts the first airport from the first segment and the last airport from the last segment.
        Works with both standard '/' delimited sectors and FCM format.
        
        Examples:
            Standard: 'HYD-BLR/BLR-PNQ' -> 'HYD-PNQ' (first of first segment + last of last segment)
            Standard: 'MAA-HYD/HYD-BLR/BLR-DEL' -> 'MAA-DEL' (first of first + last of last)
            FCM: 'CJB-MAA-Q-MAA-CMB-Q' -> 'CJB-CMB' (first airport + last airport)
            Single: 'MAA-HYD' -> None (single sector, not applicable)
            Complex: 'HYD-BLR-CCU/CCU-PNQ-DEL' -> 'HYD-DEL' (first of 'HYD-BLR-CCU' + last of 'CCU-PNQ-DEL')
        
        Returns None if sector is invalid, is a single sector, or cannot extract airports.
        """
        if self.is_empty_value(sector):
            return None
        
        sector_str = str(sector).strip()
        
        # Split using the appropriate method (handles both standard and FCM formats)
        sectors = self.split_multi_sector(sector_str)
        
        # Need at least 2 sectors for first-last pattern
        if not sectors or len(sectors) < 2:
            return None
        
        # Get first sector and extract first airport
        first_sector = sectors[0]
        first_airports = first_sector.split('-')
        if len(first_airports) < 2:
            return None
        first_airport = first_airports[0].strip()
        
        # Get last sector and extract last airport
        last_sector = sectors[-1]
        last_airports = last_sector.split('-')
        if len(last_airports) < 2:
            return None
        last_airport = last_airports[-1].strip()
        
        # Return combined first-last pattern
        if first_airport and last_airport:
            return f"{first_airport}-{last_airport}"
        
        return None
    
    def combine_consecutive_sectors(self, sectors: List[str]) -> Optional[str]:
        """
        Combine consecutive sectors into a single first-last pattern.
        Takes the first airport of the first sector and last airport of the last sector.
        Examples:
            ['PNQ-HYD', 'HYD-CJB'] -> 'PNQ-CJB'
            ['IXJ-AMD', 'AMD-PNQ'] -> 'IXJ-PNQ'
        Returns None if invalid or cannot extract airports.
        """
        if not sectors or len(sectors) < 1:
            return None
        
        # Get first airport from first sector
        first_sector = sectors[0]
        first_airports = first_sector.split('-')
        if len(first_airports) < 2:
            return None
        first_airport = first_airports[0].strip()
        
        # Get last airport from last sector
        last_sector = sectors[-1]
        last_airports = last_sector.split('-')
        if len(last_airports) < 2:
            return None
        last_airport = last_airports[-1].strip()
        
        if first_airport and last_airport:
            return f"{first_airport}-{last_airport}"
        
        return None
    
    def get_all_sector_combinations(self, sector: str) -> List[str]:
        """
        Get all possible sector combinations for matching, including:
        1. Individual sectors (split by '/')
        2. Paired combinations based on number of segments
        3. First-last sector combination
        
        Examples:
            'PNQ-HYD/HYD-CJB/CJB-PNQ' (3 segments, 2 '/'):
                - Individual: ['PNQ-HYD', 'HYD-CJB', 'CJB-PNQ']
                - Pairs: ['PNQ-CJB' (segments 0-1), 'HYD-PNQ' (segments 1-2)]
                - First-last: ['PNQ-PNQ']
            
            'IXJ-AMD/AMD-PNQ/PNQ-DEL/DEL-IXJ' (4 segments, 3 '/'):
                - Individual: ['IXJ-AMD', 'AMD-PNQ', 'PNQ-DEL', 'DEL-IXJ']
                - Half splits: ['IXJ-PNQ' (segments 0-1), 'PNQ-IXJ' (segments 2-3)]
                - First-last: ['IXJ-IXJ']
            
            'A-B/B-C/C-D/D-E/E-F/F-G' (6 segments, 5 '/'):
                - Individual: ['A-B', 'B-C', 'C-D', 'D-E', 'E-F', 'F-G']
                - Consecutive pairs: ['A-C', 'B-D', 'C-E', 'D-F', 'E-G']
                - Half splits: ['A-D' (first half), 'D-G' (second half)]
                - First-last: ['A-G']
        
        Works dynamically for any number of segments!
        
        Returns list of unique sector patterns to query.
        """
        if self.is_empty_value(sector):
            return []
        
        sector_str = str(sector).strip()
        all_combinations = []
        
        # Split to get individual sectors (handles both standard '/' and FCM format)
        sectors = self.split_multi_sector(sector_str)
        
        # If only one sector after splitting, return as-is
        if len(sectors) <= 1:
            return sectors
        
        # Add all individual sectors
        all_combinations.extend(sectors)
        
        num_sectors = len(sectors)
        
        # Handle different numbers of segments
        if num_sectors == 2:
            # 2 segments (1 '/'): already have individuals + will add first-last below
            pass
        
        elif num_sectors == 3:
            # 3 segments (2 '/'): Add paired combinations
            # Pair 1: segments 0-1 (first two)
            pair1 = self.combine_consecutive_sectors(sectors[0:2])
            if pair1 and pair1 not in all_combinations:
                all_combinations.append(pair1)
            
            # Pair 2: segments 1-2 (last two)
            pair2 = self.combine_consecutive_sectors(sectors[1:3])
            if pair2 and pair2 not in all_combinations:
                all_combinations.append(pair2)
        
        elif num_sectors == 4:
            # 4 segments (3 '/'): Split in half
            # First half: segments 0-1
            first_half = self.combine_consecutive_sectors(sectors[0:2])
            if first_half and first_half not in all_combinations:
                all_combinations.append(first_half)
            
            # Second half: segments 2-3
            second_half = self.combine_consecutive_sectors(sectors[2:4])
            if second_half and second_half not in all_combinations:
                all_combinations.append(second_half)
        
        elif num_sectors > 4:
            # For more than 4 segments: Add consecutive pairs
            for i in range(len(sectors) - 1):
                pair = self.combine_consecutive_sectors(sectors[i:i+2])
                if pair and pair not in all_combinations:
                    all_combinations.append(pair)
            
            # For even number of segments, also split in half
            if num_sectors % 2 == 0:
                mid_point = num_sectors // 2
                # First half
                first_half = self.combine_consecutive_sectors(sectors[0:mid_point])
                if first_half and first_half not in all_combinations:
                    all_combinations.append(first_half)
                
                # Second half
                second_half = self.combine_consecutive_sectors(sectors[mid_point:num_sectors])
                if second_half and second_half not in all_combinations:
                    all_combinations.append(second_half)
        
        # Always add first-last sector combination
        first_last = self.get_first_last_sector_from_multi(sector_str)
        if first_last and first_last not in all_combinations:
            all_combinations.append(first_last)
        
        return all_combinations
    
    def detect_reference_columns(self, df_excel: pd.DataFrame, 
                                possible_combinations: List[List[str]]) -> List[str]:
        """
        Detect which reference column combination is available in the Excel file (exact match only).
        """
        available_columns = set(df_excel.columns)
        
        for combination in possible_combinations:
            if all(col in available_columns for col in combination):
                logger.info(f"Detected reference columns: {combination}")
                return combination
        
        logger.error("No suitable reference columns found")
        return []
    
    def get_fcm_ticket_variations(self, ticket_value: str) -> List[str]:
        """
        Get all ticket number variations.
        
        For all files:
        - Handles slash notation: '2791140688/89' -> ['2791140688', '2791140689']
        - If ticket number length > 10, includes both original and version with dash
          at position (length-10). Example: '1252791614965' -> ['1252791614965', '125-2791614965']
        
        For FCM files, additionally generates variations with prefixes:
        607, 098, 176, 125, 057, 074, 157, 220
        
        Each prefix is tried both with and without a dash separator.
        
        Args:
            ticket_value: Original ticket number value (may contain slash notation)
            
        Returns:
            List of ticket number variations to try. First item is always the original value.
            
        Example (for FCM files):
            get_fcm_ticket_variations('2790431640') returns:
            ['2790431640', '6072790431640', '607-2790431640', '0982790431640', ...]
        Example (for non-FCM files with long ticket):
            get_fcm_ticket_variations('1252791614965') returns:
            ['1252791614965', '125-2791614965']
        Example (with slash notation):
            get_fcm_ticket_variations('2791140688/89') returns:
            ['2791140688', '2791140689', ...] (plus prefix variations for FCM files)
        """
        # Check if current file is an FCM file
        if self.current_file_path and is_fcm_file(os.path.basename(self.current_file_path)):
            # Use FCM parser to generate variations (includes slash expansion, dash logic, and prefixes)
            return generate_fcm_ticket_variations(ticket_value)
        
        # For non-FCM files, apply slash expansion and dash logic for long ticket numbers
        if not ticket_value or not isinstance(ticket_value, str):
            return [str(ticket_value) if ticket_value else '']
        
        ticket_value = ticket_value.strip()
        if not ticket_value:
            return ['']
        
        # First, expand slash notation (e.g., '2791140688/89' -> ['2791140688', '2791140689'])
        base_tickets = expand_slash_notation_ticket(ticket_value)
        
        all_variations = []
        seen = set()
        
        for base_ticket in base_tickets:
            if not base_ticket or base_ticket in seen:
                continue
            seen.add(base_ticket)
            all_variations.append(base_ticket)
            
            # If ticket number length > 10, add version with dash at position (length-10)
            if len(base_ticket) > 10 and '-' not in base_ticket:
                dash_position = len(base_ticket) - 10
                ticket_with_dash = f"{base_ticket[:dash_position]}-{base_ticket[dash_position:]}"
                if ticket_with_dash not in seen:
                    seen.add(ticket_with_dash)
                    all_variations.append(ticket_with_dash)
        
        return all_variations
    
    def create_dynamic_query(self, reference_columns: List[str], params: List) -> str:
        """
        Create dynamic WHERE clause based on available reference columns.
        """
        conditions = []
        
        for ref_col in reference_columns:
            conditions.append(f"`{ref_col}` = %s")
        
        return " AND ".join(conditions)
    
    def try_match_with_combination(self, row: pd.Series, combination: List[str], 
                                   excel_to_db_mapping: Dict[str, str], 
                                   table_name: str, missing_columns: List[str]) -> Optional[Dict]:
        """
        Try to match a single row with database using a specific combination.
        Returns matched data dict if found, None otherwise.
        """
        # Get values for this combination
        key_values = []
        for ref_col in combination:
            # Find the Excel column name that maps to this DB column
            excel_col = None
            for excel_name, db_name in excel_to_db_mapping.items():
                if db_name == ref_col:
                    excel_col = excel_name
                    break
            # If not found in mapping, try case-insensitive search (need to get df_excel from context)
            # Note: This method might not have direct access to df_excel, so we'll use a simpler approach
            if excel_col is None:
                # Try to find column in row.index case-insensitively
                for col in row.index:
                    if str(col).lower().strip() == str(ref_col).lower().strip():
                        excel_col = col
                        break
            # Last resort: use ref_col as-is
            if excel_col is None:
                excel_col = ref_col
            
            # Check if column exists and has non-empty value
            if excel_col not in row.index:
                return None  # Column not available
            
            value = row[excel_col]
            if self.is_empty_value(value):
                return None  # Empty value, can't use this combination
            
            key_values.append(value)
        
        # Build and execute query
        ref_cols_str = ', '.join([f"`{c}`" for c in combination])
        missing_columns_str = ', '.join([f"`{col}`" for col in missing_columns])
        
        # Build WHERE clause with AND conditions
        where_conditions = ' AND '.join([f"`{c}` = %s" for c in combination])
        
        query = (
            f"SELECT {ref_cols_str}, {missing_columns_str} "
            f"FROM `{table_name}` "
            f"WHERE {where_conditions} "
            f"LIMIT 1"
        )
        
        results = self.execute_query_with_retry(query, key_values)
        
        if results and len(results) > 0:
            return {col: results[0].get(col) for col in missing_columns}
        
        return None
    
    def apply_header_formatting(self, original_excel_path: str, output_excel_path: str, 
                                header_row_index: int = 1) -> bool:
        """
        Header formatting has been disabled (openpyxl removed).
        This function now does nothing and returns True.
        """
        logger.info("Header formatting skipped (openpyxl removed)")
        return True
    
    def _apply_sheet_formatting(self, original_ws, output_ws, header_row_index: int):
        """
        Sheet formatting has been disabled (openpyxl removed).
        This function now does nothing.
        """
        pass
    
    def format_date_columns(self, output_excel_path: str, header_row_index: int = 1) -> bool:
        """
        Date column formatting has been disabled (openpyxl removed).
        This function now does nothing and returns True.
        """
        logger.info("Date column formatting skipped (openpyxl removed)")
        return True
    
    def _enrich_single_dataframe(self, df_excel: pd.DataFrame, table_name: str,
                                 possible_reference_combinations: List[List[str]],
                                 column_mapping: Dict[str, str]) -> Optional[pd.DataFrame]:
        """Helper method to enrich a single DataFrame."""
        logger.info(f"Processing {len(df_excel)} rows...")
        logger.info(f"Available columns: {list(df_excel.columns)}")
        
        # Create column mapping for database operations (without renaming Excel columns)
        excel_to_db_mapping = {}
        if column_mapping:
            excel_columns_list = list(df_excel.columns)
            for mapping_key, db_col in column_mapping.items():
                if mapping_key in df_excel.columns:
                    excel_to_db_mapping[mapping_key] = db_col
                else:
                    matched_col = self.find_column_case_insensitive(mapping_key, excel_columns_list)
                    if matched_col:
                        excel_to_db_mapping[matched_col] = db_col
                        logger.info(f"Matched '{mapping_key}' (from config) to '{matched_col}' (in Excel) - case-insensitive match")
            logger.info(f"Created mapping for {len(excel_to_db_mapping)} columns")
        
        # Create a temporary DataFrame with mapped column names for reference detection
        df_temp = df_excel.copy()
        if excel_to_db_mapping:
            df_temp = df_temp.rename(columns=excel_to_db_mapping)
        
        # Check if at least one combination is available (for validation)
        available_columns = set(df_temp.columns)
        valid_combinations = []
        for combination in possible_reference_combinations:
            if all(col in available_columns for col in combination):
                valid_combinations.append(combination)
        
        if not valid_combinations:
            logger.error("No suitable reference columns found")
            return None
        
        logger.info(f"Will try matching with {len(valid_combinations)} combinations in cascade order: {valid_combinations}")
        
        # Get database columns
        all_db_columns = self.get_all_columns(table_name)
        if not all_db_columns:
            logger.error("Failed to get database columns")
            return None
        
        # Validate that all columns in combinations exist in the database
        all_db_columns_set = set(all_db_columns)
        validated_combinations = []
        for combination in valid_combinations:
            if all(col in all_db_columns_set for col in combination):
                validated_combinations.append(combination)
            else:
                missing_cols = [col for col in combination if col not in all_db_columns_set]
                logger.warning(f"Skipping combination {combination} - database columns not found: {missing_cols}")
        
        if not validated_combinations:
            logger.error("No valid combinations found after database column validation")
            return None
        
        # Update valid_combinations to only include validated ones
        valid_combinations = validated_combinations
        logger.info(f"After database validation, {len(valid_combinations)} combinations are valid: {valid_combinations}")
        
        # Define output column structure: (db_column_name, output_column_name, excel_only)
        # excel_only=True means only take from Excel if column exists, not from DB
        output_column_mapping = [
            (None, 'Legal_Name', True),  # Excel only: "GST Name"
            (None, 'Company_GST_Number', True),  # Excel only: "GST Number"
            (None, 'Booking_Date', True),  # Excel only: "Booking Date"
            (None, 'Travel_Date', True),  # Excel only: "Departure Date"
            (None, 'Passenger_Name', True),  # Excel only: "LOUIS ARUL AROCKIASAMY" or similar passenger name columns
            (None, 'PNR', True),  # Excel only: "Airline Pnr"
            (None, 'Ticket_Number', True),  # Excel only: "Airline Ticket No."
            (None, 'Vendor Invoice No', True),  # Excel only: "GST INVOICE NO"
            (None, 'Vendor K3 Amount', True),  # Excel only: "TOTAL K3"
            (None, 'Airline_Name', True),  # Excel only: "Airline Name"
            (None, 'Airline_Code', True),  # Excel only: "Airline Code"
            (None, 'Travel_Mode', True),  # Excel only: "TRIP TYPE"
            (None, 'Travel_Sector', True),  # Excel only: "Sector"
            ('Place_Of_Supply', 'Embarking_State', False),  # From DB
            ('GST_Number', 'Airline_GST_Number', False),  # From DB
            ('Airline_Gst_Name', 'Airline_Legal_Name', False),  # From DB
            (None, 'Ticket_Amount', True),  # Excel only: "Total Fare (Including GST)"
            (None, 'Cost_Center', True),  # Excel only: "Cost_Center"
            (None, 'Remarks', True),  # Excel only: "Remarks"
            ('Invoice_Type', 'Invoice_Type', False),  # From DB
            ('Email_Date', 'Invoice_Received_Date', False),  # From DB
            ('Date_Of_Invoice', 'Invoice_Date', False),  # From DB
            ('Invoice_Number', 'Invoice_Number', False),  # From DB
            ('Original_Invoice_Number', 'Original_Invoice_Number', False),  # From DB
            ('Original_Invoice_Date', 'Original_Invoice_Date', False),  # From DB
            ('Invoice_Total', 'Total_Invoice_Amount', False),  # From DB
            ('Taxable_Amount', 'Invoice_Taxable_Value', False),  # From DB
            ('NonTaxable_Amount', 'Invoice_Non_Taxable_Value', False),  # From DB
            ('Igst_Total', 'Invoice_IGST', False),  # From DB
            ('Cgst_Total', 'Invoice_CGST', False),  # From DB
            ('Sgst_Total', 'Invoice_SGST', False),  # From DB
            ('Invoice_Total_GST', 'Invoice_Total_GST_Amount', False),  # From DB
            ('Igst_Rate', 'IGST_Rate', False),  # From DB
            ('Cgst_Rate', 'CGST_Rate', False),  # From DB
            ('Sgst_Rate', 'SGST_Rate', False),  # From DB
            ('Public_File_URL', 'Airline_Invoice_Download_Url', False),  # From DB
        ]
        
        # Excel column name mappings for Excel-only columns
        excel_column_mappings = {
            'Legal_Name': ['GST Name', 'gst name', 'GSTName', 'GST_Name', 'gst_name'],
            'Company_GST_Number': ['GST Number', 'gst number', 'GSTNumber', 'GST_Number', 'gst_number'],
            'Booking_Date': ['Booking Date', 'booking date', 'BookingDate', 'Booking_Date', 'booking_date'],
            'Travel_Date': ['Departure Date', 'departure date', 'DepartureDate', 'Departure_Date', 'departure_date', 'Onward Date', 'onward date', 'OnwardDate', 'Travel_Date', 'travel date'],
            'Passenger_Name': ['LOUIS ARUL AROCKIASAMY', 'louis arul arokiasamy', 'Traveller', 'Traveler_Name', 'traveler name', 'TravelerName', 'Passenger Name', 'passenger name', 'Passenger Name'],
            'PNR': ['Airline Pnr', 'airline pnr', 'Airlne Pnr', 'airlne pnr', 'Airline PNR', 'Airline PNR/Prov. Booking', 'airline pnr/prov. booking', 'pnrnumber', 'pnr number', 'PNR_Number', 'PNR', 'pnr'],
            'Ticket_Number': ['Airline Ticket No.', 'airline ticket no.', 'Airline Ticket No', 'Ticket Num/Final Booking', 'ticket num/final booking', 'TicketNumber', 'Ticket Number', 'ticket number', 'Ticket_Number'],
            'Vendor Invoice No': ['GST INVOICE NO', 'gst invoice no', 'GST Invoice No', 'GST_INVOICE_NO', 'GSTInvoiceNo', 'GST Invoice Number', 'Vendor Invoice Number', 'vendor invoice number', 'Vendor Invoice No', 'vendor invoice no', 'Vendor_Invoice_Number', 'vendor_invoice_number'],
            'Vendor K3 Amount': ['TOTAL K3', 'total k3', 'Total K3', 'TOTAL_K3', 'TotalK3', 'Total K3 Amount', 'K3', 'k3', 'Vendor K3 Amount', 'vendor k3 amount', 'Vendor_K3_Amount', 'vendor_k3_amount'],
            'Airline_Name': ['Airline Name', 'airline name', 'AirlineName', 'Airline_Name', 'airline_name'],
            'Airline_Code': ['Airline Code', 'airline code', 'AirlineCode', 'airlinecode', 'Airline_Code'],
            'Travel_Mode': ['TRIP TYPE', 'trip type', 'Trip Type', 'TRIP_TYPE', 'TripType', 'Product Type', 'product type', 'ProductType', 'Travel_Mode', 'travel mode'],
            'Travel_Sector': ['Sector', 'sector', 'Travel_Sector', 'travel sector', 'TravelSector', 'trvael sector', 'travelsector'],
            'Ticket_Amount': ['Total Fare (Including GST)', 'total fare (including gst)', 'Total Fare', 'total fare', 'TotalFare', 'Total_Fare', 'Gross Fare', 'gross fare', 'GrossFare', 'Gross_Fare', 'gross_fare'],
            'Cost_Center': ['Cost_Center', 'cost center', 'CostCenter', 'Cost Centre', 'cost centre', 'Cost Center'],
            'Remarks': ['Remarks', 'remarks', 'REMARKS', 'Remark', 'remark', 'REMARK', 'Comments', 'comments', 'COMMENTS']
        }
        
        # For FCM files, prioritize "Gross Fare" for Ticket_Amount
        if self.current_file_path and is_fcm_file(os.path.basename(self.current_file_path)):
            excel_column_mappings['Ticket_Amount'] = ['Gross Fare', 'gross fare', 'GrossFare', 'Gross_Fare', 'gross_fare', 'Total Fare (Including GST)', 'total fare (including gst)', 'Total Fare', 'total fare', 'TotalFare', 'Total_Fare']
            logger.info("FCM file detected: Prioritizing 'Gross Fare' for Ticket_Amount")
        
        # Extract DB columns to fetch (exclude Excel-only columns)
        db_columns_to_fetch = [col[0] for col in output_column_mapping if col[0] is not None and not col[2]]
        # Remove duplicates while preserving order
        db_columns_to_fetch = list(dict.fromkeys(db_columns_to_fetch))
        
        # Numeric columns that should be aggregated when multiple sectors are present
        numeric_columns_to_aggregate = [
            'Taxable_Amount', 'NonTaxable_Amount', 'Cgst_Total', 'Sgst_Total', 
            'Igst_Total', 'Invoice_Total_GST', 'Invoice_Total'
        ]
        
        # We need to fetch ALL DB columns from database (even if they exist in Excel)
        # User requirement: "initially fetch from db"
        missing_columns = db_columns_to_fetch.copy()
        
        logger.info(f"Will fetch {len(missing_columns)} columns from database: {missing_columns}")
        
        # Find the sector column name in Excel (could be mapped or original)
        sector_excel_col = None
        for excel_name, db_name in excel_to_db_mapping.items():
            if db_name == 'Travel_Sector':
                sector_excel_col = excel_name
                break
        if sector_excel_col is None:
            # Try to find it directly
            for col in df_excel.columns:
                if col.lower().strip() in ['sector', 'travel_sector', 'travel sector']:
                    sector_excel_col = col
                    break
        
        # Find the Vendor Invoice No column name in Excel
        vendor_invoice_col = None
        for excel_name, db_name in excel_to_db_mapping.items():
            if db_name == 'Vendor Invoice No':
                vendor_invoice_col = excel_name
                break
        if vendor_invoice_col is None:
            # Try to find it using the mapping
            possible_names = excel_column_mappings.get('Vendor Invoice No', [])
            for name in possible_names:
                matched_col = self.find_column_case_insensitive(name, list(df_excel.columns))
                if matched_col:
                    vendor_invoice_col = matched_col
                    break
        
        # Process data in batches with cascading matching logic (optimized for performance)
        enriched_data = []
        match_count = 0
        no_match_count = 0
        combination_usage_stats = {str(combo): 0 for combo in valid_combinations}
        
        logger.info(f"Processing {len(df_excel)} rows with cascading matching (batch mode)...")
        
        # Process in batches for better performance
        for batch_start in range(0, len(df_excel), BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, len(df_excel))
            batch_df = df_excel.iloc[batch_start:batch_end]
            
            logger.info(f"Processing batch {batch_start//BATCH_SIZE + 1}: rows {batch_start + 1}-{batch_end}")
            
            # Track which rows have been matched and which combination was used
            row_matches = {}  # idx -> {matched_data, combination}
            unmatched_indices = set(batch_df.index)
            
            # Try each combination in cascade order, only for unmatched rows
            for combination in valid_combinations:
                if not unmatched_indices:
                    break  # All rows matched, skip remaining combinations
                
                # Check if Travel_Sector is in this combination
                has_sector = 'Travel_Sector' in combination
                
                # Build keys for rows that can use this combination
                row_keys = {}  # idx -> list of tuples (for multi-sector, multiple keys per row)
                rows_with_valid_keys = []
                
                for idx in list(unmatched_indices):
                    row = batch_df.loc[idx]
                    key_values_list = []  # List of key tuples (one per sector if multi-sector)
                    valid = True
                    
                    # Get sector value if Travel_Sector is in combination
                    sector_value = None
                    if has_sector and sector_excel_col and sector_excel_col in row.index:
                        sector_value = row[sector_excel_col]
                    
                    # Get all sector combinations including individuals, pairs, and first-last
                    sectors_to_query = []
                    if has_sector and sector_value and not self.is_empty_value(sector_value):
                        sectors_to_query = self.get_all_sector_combinations(sector_value)
                    else:
                        sectors_to_query = [None]  # Single query without sector
                    
                    # Build keys for each sector
                    for sector in sectors_to_query:
                        # Collect base values for all columns
                        base_values = []
                        valid_for_sector = True
                        ticket_index = None  # Track position of Ticket_Number in the combination
                        
                        for col_idx, ref_col in enumerate(combination):
                            # Find the Excel column name that maps to this DB column
                            excel_col = None
                            for excel_name, db_name in excel_to_db_mapping.items():
                                if db_name == ref_col:
                                    excel_col = excel_name
                                    break
                            # If not found in mapping, try case-insensitive search in original Excel columns
                            if excel_col is None:
                                excel_col = self.find_column_case_insensitive(ref_col, list(df_excel.columns))
                                # If still not found, try to find by checking column_mapping keys
                                if excel_col is None:
                                    for mapping_key, db_name in column_mapping.items():
                                        if db_name == ref_col:
                                            excel_col = self.find_column_case_insensitive(mapping_key, list(df_excel.columns))
                                            if excel_col:
                                                break
                            # Last resort: use ref_col as-is (might work if column name matches exactly)
                            if excel_col is None:
                                excel_col = ref_col
                            
                            # Check if column exists and has non-empty value
                            if excel_col not in row.index:
                                valid_for_sector = False
                                break
                            
                            value = row[excel_col]
                            
                            # If this is Travel_Sector, use the sector value directly from Excel
                            if ref_col == 'Travel_Sector' and sector is not None:
                                value = sector
                            
                            # Normalize PNR/Ticket values for FCM files
                            if ref_col in ['PNR_Number', 'Ticket_Number']:
                                value = self.normalize_pnr_ticket_value(value, ref_col)
                                # Track if this is Ticket_Number for FCM variation handling
                                if ref_col == 'Ticket_Number':
                                    ticket_index = col_idx
                            
                            if self.is_empty_value(value):
                                valid_for_sector = False
                                break
                            
                            base_values.append(self.normalize_ref_column_value(ref_col, value))
                        
                        if valid_for_sector:
                            # For all files with Ticket_Number, generate variations (including dash logic for length > 10)
                            if ticket_index is not None:
                                ticket_variations = self.get_fcm_ticket_variations(base_values[ticket_index])
                                # Create a key for each ticket variation
                                for ticket_var in ticket_variations:
                                    varied_values = base_values.copy()
                                    varied_values[ticket_index] = ticket_var
                                    key_values_list.append(tuple(varied_values))
                            else:
                                # No Ticket_Number in this combination: use single key
                                key_values_list.append(tuple(base_values))
                    
                    if key_values_list:
                        row_keys[idx] = key_values_list
                        rows_with_valid_keys.append(idx)
                
                # If we have valid keys, execute batch query
                if row_keys:
                    # Collect all unique keys (flatten the list of lists)
                    all_keys = []
                    key_to_row_indices = {}  # Map key -> list of row indices that use this key
                    key_to_pattern = {}  # Map key -> 'CN' or 'IN' pattern based on GST INVOICE NO
                    
                    for idx, keys_list in row_keys.items():
                        # Get GST INVOICE NO pattern for this row
                        invoice_pattern = None
                        if vendor_invoice_col and vendor_invoice_col in batch_df.loc[idx].index:
                            vendor_invoice_val = batch_df.loc[idx, vendor_invoice_col]
                            if not self.is_empty_value(vendor_invoice_val):
                                vendor_invoice_str = str(vendor_invoice_val).strip().upper()
                                if vendor_invoice_str.startswith('CN'):
                                    invoice_pattern = 'CN'
                                elif vendor_invoice_str.startswith('IN'):
                                    invoice_pattern = 'IN'
                        
                        for key in keys_list:
                            if key not in key_to_row_indices:
                                all_keys.append(key)
                                key_to_row_indices[key] = []
                                key_to_pattern[key] = None
                            key_to_row_indices[key].append(idx)
                            # Set pattern for key (if multiple rows use same key, use first non-None pattern)
                            if invoice_pattern and key_to_pattern[key] is None:
                                key_to_pattern[key] = invoice_pattern
                    
                    unique_keys = list(dict.fromkeys(all_keys))  # Preserve order, remove duplicates
                    
                    # Build batch query with ORDER BY for Original_Invoice_Number prioritization
                    ref_cols_str = ', '.join([f"`{c}`" for c in combination])
                    missing_columns_str = ', '.join([f"`{col}`" for col in missing_columns])
                    placeholders = ', '.join(["(" + ", ".join(["%s"] * len(combination)) + ")" for _ in unique_keys])
                    
                    # Add NULL checks to ensure we don't match on NULL values in database
                    null_checks = ' AND '.join([f"`{c}` IS NOT NULL" for c in combination])
                    
                    # Add ORDER BY to prioritize Original_Invoice_Number based on pattern
                    # For CN: prioritize NOT NULL, for IN: prioritize NULL
                    # Since we have mixed patterns, we'll use a general ordering and handle prioritization in code
                    # But we can still add ORDER BY to help with the prioritization
                    order_by_clause = "ORDER BY `Original_Invoice_Number` IS NOT NULL DESC, `Created_Date` DESC"
                    
                    query = (
                        f"SELECT {ref_cols_str}, {missing_columns_str} "
                        f"FROM `{table_name}` "
                        f"WHERE ({ref_cols_str}) IN ({placeholders}) "
                        f"AND {null_checks} "
                        f"{order_by_clause}"
                    )
                    params = [v for key in unique_keys for v in key]
                    
                    # Log query details for Ticket_Number combination
                    if 'Ticket_Number' in combination:
                        logger.info(f"Executing query for combination {combination}: {len(unique_keys)} unique keys")
                        logger.debug(f"Query: {query[:200]}... (truncated)")
                        logger.debug(f"Sample params (first 3): {params[:min(6, len(params))]}")
                    
                    # Execute batch query
                    results = self.execute_query_with_retry(query, params)
                    
                    # Log results for Ticket_Number combination
                    if 'Ticket_Number' in combination:
                        logger.info(f"Query returned {len(results)} results for combination {combination}")
                    
                    # Group results by key first (multiple results per key possible)
                    results_by_key = {}
                    for r in results:
                        key = tuple(self.normalize_ref_column_value(c, r[c]) for c in combination)
                        if key not in results_by_key:
                            results_by_key[key] = []
                        results_by_key[key].append({col: r.get(col) for col in missing_columns})
                    
                    # Build lookup dictionary from grouped results
                    # Prioritize based on GST INVOICE NO pattern and Original_Invoice_Number
                    lookup = {}
                    for key in results_by_key:
                        key_results = results_by_key[key]
                        pattern = key_to_pattern.get(key)
                        
                        if pattern == 'CN':
                            # For CN: ONLY match records with Original_Invoice_Number NOT NULL
                            # No fallback - if no matching record, don't add to lookup
                            matched_result = None
                            
                            for result in key_results:
                                original_inv_num = result.get('Original_Invoice_Number')
                                if not self.is_empty_value(original_inv_num):
                                    matched_result = result
                                    break
                            
                            if matched_result is not None:
                                lookup[key] = matched_result
                        elif pattern == 'IN':
                            # For IN: ONLY match records with Original_Invoice_Number NULL
                            # No fallback - if no matching record, don't add to lookup
                            matched_result = None
                            
                            for result in key_results:
                                original_inv_num = result.get('Original_Invoice_Number')
                                if self.is_empty_value(original_inv_num):
                                    matched_result = result
                                    break
                            
                            if matched_result is not None:
                                lookup[key] = matched_result
                        else:
                            # No pattern or unknown pattern: use first result
                            lookup[key] = key_results[0]
                    
                    # Match rows with results and aggregate for multi-sector
                    for idx in rows_with_valid_keys:
                        if idx in unmatched_indices:  # Only process if not already matched
                            keys_list = row_keys[idx]
                            aggregated_data = {}
                            
                            # Collect all matching results for this row (one per sector/key)
                            all_results = []
                            for key in keys_list:
                                if key in lookup:
                                    # Each key contributes one result (already limited to 1 per key)
                                    all_results.append(lookup[key])
                            
                            if all_results:
                                # Store all matching results as separate entries
                                # Each result will create a separate output row
                                row_matches[idx] = {
                                    'matched_data_list': all_results,  # List of all matching records
                                    'combination': combination
                                }
                                unmatched_indices.remove(idx)
                                combination_usage_stats[str(combination)] += 1
                
                # After PNR_Number+Travel_Sector+Invoice_Total combo, try Ticket_Number as PNR for same combo
                if combination == ['PNR_Number', 'Travel_Sector', 'Invoice_Total'] and unmatched_indices:
                    logger.info(f"Trying Ticket_Number as PNR_Number + Sector + InvoiceTotal for {len(unmatched_indices)} unmatched rows")

                    ticket_excel_col_strict = None
                    for excel_name, db_name in excel_to_db_mapping.items():
                        if db_name == 'Ticket_Number':
                            ticket_excel_col_strict = excel_name
                            break
                    if ticket_excel_col_strict is None:
                        for col in df_excel.columns:
                            if col.lower().strip() in ['ticket number', 'ticketnumber', 'ticket_num', 'airline ticket no.', 'ticket num/final booking']:
                                ticket_excel_col_strict = col
                                break

                    invoice_total_excel_col = None
                    for excel_name, db_name in excel_to_db_mapping.items():
                        if db_name == 'Invoice_Total':
                            invoice_total_excel_col = excel_name
                            break
                    if invoice_total_excel_col is None:
                        possible_invoice_names = ['Total Fare (Including GST)', 'total fare (including gst)', 'Total Fare', 'Gross Fare', 'gross fare']
                        for col in df_excel.columns:
                            if col.strip() in possible_invoice_names or col.lower().strip() in [n.lower() for n in possible_invoice_names]:
                                invoice_total_excel_col = col
                                break

                    if ticket_excel_col_strict and sector_excel_col and invoice_total_excel_col:
                        row_keys_strict = {}
                        rows_valid_strict = []

                        for idx in list(unmatched_indices):
                            row = batch_df.loc[idx]

                            if ticket_excel_col_strict not in row.index:
                                continue
                            ticket_value_s = row[ticket_excel_col_strict]
                            ticket_value_s = self.normalize_pnr_ticket_value(ticket_value_s, 'Ticket_Number')
                            if self.is_empty_value(ticket_value_s):
                                continue

                            if sector_excel_col not in row.index:
                                continue
                            sector_value_s = row[sector_excel_col]
                            if self.is_empty_value(sector_value_s):
                                continue

                            if invoice_total_excel_col not in row.index:
                                continue
                            invoice_total_val = row[invoice_total_excel_col]
                            if self.is_empty_value(invoice_total_val):
                                continue
                            invoice_total_normalized = self.normalize_ref_column_value('Invoice_Total', invoice_total_val)
                            if invoice_total_normalized is None:
                                continue

                            sectors_strict = self.get_all_sector_combinations(sector_value_s)
                            ticket_vars_strict = self.get_fcm_ticket_variations(ticket_value_s)

                            key_list_strict = []
                            for sector_s in sectors_strict:
                                for tv in ticket_vars_strict:
                                    key_list_strict.append((tv, sector_s, invoice_total_normalized))

                            if key_list_strict:
                                row_keys_strict[idx] = key_list_strict
                                rows_valid_strict.append(idx)

                        if row_keys_strict:
                            pnr_sector_inv_combination = ['PNR_Number', 'Travel_Sector', 'Invoice_Total']
                            all_keys_strict = []
                            key_to_indices_strict = {}
                            key_to_pattern_strict = {}

                            for idx, keys_list in row_keys_strict.items():
                                inv_pattern = None
                                if vendor_invoice_col and vendor_invoice_col in batch_df.loc[idx].index:
                                    vi_val = batch_df.loc[idx, vendor_invoice_col]
                                    if not self.is_empty_value(vi_val):
                                        vi_str = str(vi_val).strip().upper()
                                        if vi_str.startswith('CN'):
                                            inv_pattern = 'CN'
                                        elif vi_str.startswith('IN'):
                                            inv_pattern = 'IN'

                                for key in keys_list:
                                    if key not in key_to_indices_strict:
                                        all_keys_strict.append(key)
                                        key_to_indices_strict[key] = []
                                        key_to_pattern_strict[key] = None
                                    key_to_indices_strict[key].append(idx)
                                    if inv_pattern and key_to_pattern_strict[key] is None:
                                        key_to_pattern_strict[key] = inv_pattern

                            unique_keys_strict = list(dict.fromkeys(all_keys_strict))

                            ref_cols_str_s = ', '.join([f"`{c}`" for c in pnr_sector_inv_combination])
                            missing_cols_str_s = ', '.join([f"`{col}`" for col in missing_columns])
                            placeholders_s = ', '.join(["(" + ", ".join(["%s"] * len(pnr_sector_inv_combination)) + ")" for _ in unique_keys_strict])
                            null_checks_s = ' AND '.join([f"`{c}` IS NOT NULL" for c in pnr_sector_inv_combination])
                            order_by_s = "ORDER BY `Original_Invoice_Number` IS NOT NULL DESC, `Created_Date` DESC"

                            query_s = (
                                f"SELECT {ref_cols_str_s}, {missing_cols_str_s} "
                                f"FROM `{table_name}` "
                                f"WHERE ({ref_cols_str_s}) IN ({placeholders_s}) "
                                f"AND {null_checks_s} "
                                f"{order_by_s}"
                            )
                            params_s = [v for key in unique_keys_strict for v in key]

                            results_strict = self.execute_query_with_retry(query_s, params_s)

                            results_by_key_strict = {}
                            for r in results_strict:
                                key = tuple(self.normalize_ref_column_value(c, r[c]) for c in pnr_sector_inv_combination)
                                if key not in results_by_key_strict:
                                    results_by_key_strict[key] = []
                                results_by_key_strict[key].append({col: r.get(col) for col in missing_columns})

                            lookup_strict = {}
                            for key in results_by_key_strict:
                                key_results = results_by_key_strict[key]
                                pattern = key_to_pattern_strict.get(key)

                                if pattern == 'CN':
                                    matched_result = None
                                    for result in key_results:
                                        original_inv_num = result.get('Original_Invoice_Number')
                                        if not self.is_empty_value(original_inv_num):
                                            matched_result = result
                                            break
                                    if matched_result is not None:
                                        lookup_strict[key] = matched_result
                                elif pattern == 'IN':
                                    matched_result = None
                                    for result in key_results:
                                        original_inv_num = result.get('Original_Invoice_Number')
                                        if self.is_empty_value(original_inv_num):
                                            matched_result = result
                                            break
                                    if matched_result is not None:
                                        lookup_strict[key] = matched_result
                                else:
                                    lookup_strict[key] = key_results[0]

                            for idx in rows_valid_strict:
                                if idx in unmatched_indices:
                                    keys_list = row_keys_strict[idx]
                                    all_results_strict = []
                                    for key in keys_list:
                                        if key in lookup_strict:
                                            all_results_strict.append(lookup_strict[key])
                                    if all_results_strict:
                                        row_matches[idx] = {
                                            'matched_data_list': all_results_strict,
                                            'combination': ['PNR_Number', 'Travel_Sector', 'Invoice_Total']
                                        }
                                        unmatched_indices.remove(idx)
                                        combination_usage_stats["Ticket_Number_as_PNR_Number+Sector+InvoiceTotal (fallback)"] = combination_usage_stats.get("Ticket_Number_as_PNR_Number+Sector+InvoiceTotal (fallback)", 0) + 1
                                        logger.debug(f"Matched row {idx} using Ticket_Number as PNR_Number + Sector + InvoiceTotal")

                            logger.info(f"Ticket_as_PNR+Sector+InvoiceTotal fallback: {combination_usage_stats.get('Ticket_Number_as_PNR_Number+Sector+InvoiceTotal (fallback)', 0)} matches found")

#pnr bookingdate logic start            
            # PRIORITY Fallback: Try PNR_Number + Booking_Date matching FIRST (before other fallbacks)
            # This runs right after the main cascade to catch rows with valid PNR but non-matching Travel_Sector
            if unmatched_indices:
                logger.info(f"Trying PNR_Number + Booking_Date matching for {len(unmatched_indices)} unmatched rows (priority fallback)")
                
                # Find PNR_Number column in Excel
                pnr_excel_col_priority = None
                for excel_name, db_name in excel_to_db_mapping.items():
                    if db_name == 'PNR_Number':
                        pnr_excel_col_priority = excel_name
                        break
                if pnr_excel_col_priority is None:
                    for col in df_excel.columns:
                        if col.lower().strip() in ['pnr', 'pnr number', 'pnrnumber', 'airline pnr', 'airlne pnr', 'airline pnr/prov. booking']:
                            pnr_excel_col_priority = col
                            break
                
                # Find Booking_Date column in Excel
                booking_date_col_priority = None
                booking_date_names_priority = ['Booking Date', 'booking date', 'BookingDate', 'Booking_Date', 'booking_date']
                for col in df_excel.columns:
                    if col in booking_date_names_priority or col.lower().strip() in [n.lower() for n in booking_date_names_priority]:
                        booking_date_col_priority = col
                        break
                
                if pnr_excel_col_priority and booking_date_col_priority:
                    logger.info(f"PNR+Booking_Date priority: PNR column='{pnr_excel_col_priority}', Booking_Date column='{booking_date_col_priority}'")
                    
                    # Build keys
                    row_keys_priority = {}
                    rows_valid_priority = []
                    excel_dates_priority = {}
                    skip_empty_pnr = 0
                    skip_empty_date = 0
                    skip_parse_fail = 0
                    
                    for idx in list(unmatched_indices):
                        row = batch_df.loc[idx]
                        
                        if pnr_excel_col_priority not in row.index:
                            continue
                        
                        pnr_val = row[pnr_excel_col_priority]
                        pnr_val = self.normalize_pnr_ticket_value(pnr_val, 'PNR_Number')
                        if self.is_empty_value(pnr_val):
                            skip_empty_pnr += 1
                            continue
                        
                        if booking_date_col_priority not in row.index:
                            continue
                        
                        date_val = row[booking_date_col_priority]
                        if self.is_empty_value(date_val):
                            skip_empty_date += 1
                            continue
                        
                        normalized_date = normalize_excel_booking_date(date_val)
                        if normalized_date is None:
                            skip_parse_fail += 1
                            logger.info(f"Priority fallback row {idx}: Date parse failed: {date_val!r} (type: {type(date_val).__name__}), PNR: {pnr_val}")
                            continue
                        
                        excel_dates_priority[idx] = normalized_date
                        pnr_variations = self.get_fcm_ticket_variations(pnr_val)
                        
                        if pnr_variations:
                            row_keys_priority[idx] = pnr_variations
                            rows_valid_priority.append(idx)
                    
                    logger.info(f"PNR+Booking_Date priority: {len(rows_valid_priority)} rows with valid keys (skipped: {skip_empty_pnr} empty PNR, {skip_empty_date} empty date, {skip_parse_fail} parse failures)")
                    
                    if row_keys_priority:
                        # Collect all PNR values
                        all_pnrs = []
                        pnr_to_indices = {}
                        pnr_to_pattern_p = {}
                        
                        for idx, pnr_list in row_keys_priority.items():
                            inv_pattern = None
                            if vendor_invoice_col and vendor_invoice_col in batch_df.loc[idx].index:
                                inv_val = batch_df.loc[idx, vendor_invoice_col]
                                if not self.is_empty_value(inv_val):
                                    inv_str = str(inv_val).strip().upper()
                                    if inv_str.startswith('CN'):
                                        inv_pattern = 'CN'
                                    elif inv_str.startswith('IN'):
                                        inv_pattern = 'IN'
                            
                            for pnr in pnr_list:
                                if pnr not in pnr_to_indices:
                                    all_pnrs.append(pnr)
                                    pnr_to_indices[pnr] = []
                                    pnr_to_pattern_p[pnr] = None
                                pnr_to_indices[pnr].append(idx)
                                if inv_pattern and pnr_to_pattern_p[pnr] is None:
                                    pnr_to_pattern_p[pnr] = inv_pattern
                        
                        unique_pnrs = list(dict.fromkeys(all_pnrs))
                        logger.info(f"PNR+Booking_Date priority: Querying DB for {len(unique_pnrs)} unique PNR values")
                        
                        # Query DB
                        cols_with_booking = list(missing_columns) + ['Booking_Date'] if 'Booking_Date' not in missing_columns else list(missing_columns)
                        cols_str = ', '.join([f"`{c}`" for c in cols_with_booking])
                        placeholders = ', '.join(['%s' for _ in unique_pnrs])
                        
                        query = (
                            f"SELECT `PNR_Number`, {cols_str} "
                            f"FROM `{table_name}` "
                            f"WHERE `PNR_Number` IN ({placeholders}) "
                            f"AND `PNR_Number` IS NOT NULL "
                            f"ORDER BY `Original_Invoice_Number` IS NOT NULL DESC, `Created_Date` DESC"
                        )
                        
                        results = self.execute_query_with_retry(query, unique_pnrs)
                        logger.info(f"PNR+Booking_Date priority: Got {len(results)} results from DB")
                        
                        # Group by PNR
                        results_by_pnr = {}
                        for r in results:
                            pnr_key = r.get('PNR_Number')
                            if pnr_key not in results_by_pnr:
                                results_by_pnr[pnr_key] = []
                            
                            db_date = r.get('Booking_Date')
                            norm_db_date = normalize_db_booking_date(db_date)
                            
                            result_data = {c: r.get(c) for c in missing_columns}
                            result_data['_norm_date'] = norm_db_date
                            result_data['_raw_date'] = db_date
                            results_by_pnr[pnr_key].append(result_data)
                        
                        # Match
                        matches_found = 0
                        for idx in rows_valid_priority:
                            if idx in unmatched_indices:
                                pnr_list = row_keys_priority[idx]
                                excel_date = excel_dates_priority.get(idx)
                                
                                if excel_date is None:
                                    continue
                                
                                matched_results = []
                                matched_pnr = None
                                
                                for pnr in pnr_list:
                                    if pnr in results_by_pnr:
                                        pattern = pnr_to_pattern_p.get(pnr)
                                        
                                        for res in results_by_pnr[pnr]:
                                            db_date = res.get('_norm_date')
                                            
                                            if excel_date == db_date:
                                                orig_inv = res.get('Original_Invoice_Number')
                                                
                                                if pattern == 'CN' and self.is_empty_value(orig_inv):
                                                    continue
                                                elif pattern == 'IN' and not self.is_empty_value(orig_inv):
                                                    continue
                                                
                                                result_copy = {k: v for k, v in res.items() if not k.startswith('_')}
                                                matched_results.append(result_copy)
                                                matched_pnr = pnr
                                                break
                                    
                                    if matched_results:
                                        break
                                
                                if matched_results:
                                    row_matches[idx] = {
                                        'matched_data_list': matched_results,
                                        'combination': ['PNR_Number', 'Booking_Date']
                                    }
                                    unmatched_indices.remove(idx)
                                    matches_found += 1
                                    combination_usage_stats["PNR_Number_Booking_Date (priority)"] = combination_usage_stats.get("PNR_Number_Booking_Date (priority)", 0) + 1
                                    logger.debug(f"Priority matched row {idx}: PNR={matched_pnr}, Date={excel_date}")
                        
                        logger.info(f"PNR+Booking_Date priority: {matches_found} matches found")
                else:
                    if not pnr_excel_col_priority:
                        logger.info("PNR+Booking_Date priority skipped: PNR column not found")
                    if not booking_date_col_priority:
                        logger.info("PNR+Booking_Date priority skipped: Booking_Date column not found")
#pnrbookingdate logic end 1            
            # Fallback: Try first-last sector pattern for unmatched rows
            # This matches patterns like "MAA-HYD-BBI" with "MAA-BBI" in the database
            if unmatched_indices and sector_excel_col:
                logger.info(f"Trying first-last sector pattern fallback for {len(unmatched_indices)} unmatched rows")
                
                # Find combinations that include Travel_Sector
                sector_combinations = [combo for combo in valid_combinations if 'Travel_Sector' in combo]
                
                for combination in sector_combinations:
                    if not unmatched_indices:
                        break
                    
                    # Build keys using first-last sector pattern
                    row_keys_fallback = {}
                    rows_with_valid_keys_fallback = []
                    
                    for idx in list(unmatched_indices):
                        row = batch_df.loc[idx]
                        
                        # Get sector value
                        if sector_excel_col not in row.index:
                            continue
                        
                        sector_value = row[sector_excel_col]
                        if self.is_empty_value(sector_value):
                            continue
                        
                        # Get first-last sector pattern
                        first_last_sector = self.get_first_last_sector(sector_value)
                        if not first_last_sector or first_last_sector == sector_value:
                            # No intermediate airports to skip, skip this fallback
                            continue
                        
                        # Build key with first-last sector
                        key_values = []
                        valid_for_sector = True
                        
                        for ref_col in combination:
                            # Find the Excel column name that maps to this DB column
                            excel_col = None
                            for excel_name, db_name in excel_to_db_mapping.items():
                                if db_name == ref_col:
                                    excel_col = excel_name
                                    break
                            # If not found in mapping, try case-insensitive search in original Excel columns
                            if excel_col is None:
                                excel_col = self.find_column_case_insensitive(ref_col, list(df_excel.columns))
                                # If still not found, try to find by checking column_mapping keys
                                if excel_col is None:
                                    for mapping_key, db_name in column_mapping.items():
                                        if db_name == ref_col:
                                            excel_col = self.find_column_case_insensitive(mapping_key, list(df_excel.columns))
                                            if excel_col:
                                                break
                            # Last resort: use ref_col as-is (might work if column name matches exactly)
                            if excel_col is None:
                                excel_col = ref_col
                            
                            # Check if column exists and has non-empty value
                            if excel_col not in row.index:
                                valid_for_sector = False
                                break
                            
                            value = row[excel_col]
                            
                            # Use first-last sector pattern for Travel_Sector
                            if ref_col == 'Travel_Sector':
                                value = first_last_sector
                            
                            if self.is_empty_value(value):
                                valid_for_sector = False
                                break
                            
                            key_values.append(self.normalize_ref_column_value(ref_col, value))
                        
                        if valid_for_sector:
                            row_keys_fallback[idx] = [tuple(key_values)]
                            rows_with_valid_keys_fallback.append(idx)
                    
                    # If we have valid keys, execute batch query
                    if row_keys_fallback:
                        # Collect all unique keys
                        all_keys_fallback = []
                        key_to_row_indices_fallback = {}
                        key_to_pattern_fallback = {}  # Map key -> 'CN' or 'IN' pattern based on GST INVOICE NO
                        
                        for idx, keys_list in row_keys_fallback.items():
                            # Get GST INVOICE NO pattern for this row
                            invoice_pattern = None
                            if vendor_invoice_col and vendor_invoice_col in batch_df.loc[idx].index:
                                vendor_invoice_val = batch_df.loc[idx, vendor_invoice_col]
                                if not self.is_empty_value(vendor_invoice_val):
                                    vendor_invoice_str = str(vendor_invoice_val).strip().upper()
                                    if vendor_invoice_str.startswith('CN'):
                                        invoice_pattern = 'CN'
                                    elif vendor_invoice_str.startswith('IN'):
                                        invoice_pattern = 'IN'
                            
                            for key in keys_list:
                                if key not in key_to_row_indices_fallback:
                                    all_keys_fallback.append(key)
                                    key_to_row_indices_fallback[key] = []
                                    key_to_pattern_fallback[key] = None
                                key_to_row_indices_fallback[key].append(idx)
                                # Set pattern for key (if multiple rows use same key, use first non-None pattern)
                                if invoice_pattern and key_to_pattern_fallback[key] is None:
                                    key_to_pattern_fallback[key] = invoice_pattern
                        
                        unique_keys_fallback = list(dict.fromkeys(all_keys_fallback))
                        
                        # Build batch query with ORDER BY for Original_Invoice_Number prioritization
                        ref_cols_str = ', '.join([f"`{c}`" for c in combination])
                        missing_columns_str = ', '.join([f"`{col}`" for col in missing_columns])
                        placeholders = ', '.join(["(" + ", ".join(["%s"] * len(combination)) + ")" for _ in unique_keys_fallback])
                        
                        # Add NULL checks
                        null_checks = ' AND '.join([f"`{c}` IS NOT NULL" for c in combination])
                        
                        # Add ORDER BY to prioritize Original_Invoice_Number based on pattern
                        order_by_clause = "ORDER BY `Original_Invoice_Number` IS NOT NULL DESC, `Created_Date` DESC"
                        
                        query = (
                            f"SELECT {ref_cols_str}, {missing_columns_str} "
                            f"FROM `{table_name}` "
                            f"WHERE ({ref_cols_str}) IN ({placeholders}) "
                            f"AND {null_checks} "
                            f"{order_by_clause}"
                        )
                        params = [v for key in unique_keys_fallback for v in key]
                        
                        # Execute batch query
                        results_fallback = self.execute_query_with_retry(query, params)
                        
                        # Group results by key first (multiple results per key possible)
                        results_by_key_fallback = {}
                        for r in results_fallback:
                            key = tuple(self.normalize_ref_column_value(c, r[c]) for c in combination)
                            if key not in results_by_key_fallback:
                                results_by_key_fallback[key] = []
                            results_by_key_fallback[key].append({col: r.get(col) for col in missing_columns})
                        
                        # Build lookup dictionary from grouped results
                        # Prioritize based on GST INVOICE NO pattern and Original_Invoice_Number
                        lookup_fallback = {}
                        for key in results_by_key_fallback:
                            key_results = results_by_key_fallback[key]
                            pattern = key_to_pattern_fallback.get(key)
                            
                            if pattern == 'CN':
                                # For CN: ONLY match records with Original_Invoice_Number NOT NULL
                                # No fallback - if no matching record, don't add to lookup
                                matched_result = None
                                
                                for result in key_results:
                                    original_inv_num = result.get('Original_Invoice_Number')
                                    if not self.is_empty_value(original_inv_num):
                                        matched_result = result
                                        break
                                
                                if matched_result is not None:
                                    lookup_fallback[key] = matched_result
                            elif pattern == 'IN':
                                # For IN: ONLY match records with Original_Invoice_Number NULL
                                # No fallback - if no matching record, don't add to lookup
                                matched_result = None
                                
                                for result in key_results:
                                    original_inv_num = result.get('Original_Invoice_Number')
                                    if self.is_empty_value(original_inv_num):
                                        matched_result = result
                                        break
                                
                                if matched_result is not None:
                                    lookup_fallback[key] = matched_result
                            else:
                                # No pattern or unknown pattern: use first result
                                lookup_fallback[key] = key_results[0]
                        
                        # Match rows with results
                        for idx in rows_with_valid_keys_fallback:
                            if idx in unmatched_indices:
                                keys_list = row_keys_fallback[idx]
                                
                                # Collect matching results
                                all_results_fallback = []
                                for key in keys_list:
                                    if key in lookup_fallback:
                                        all_results_fallback.append(lookup_fallback[key])
                                
                                if all_results_fallback:
                                    # Store all matching results as separate entries
                                    # Each result will create a separate output row
                                    row_matches[idx] = {
                                        'matched_data_list': all_results_fallback,  # List of all matching records
                                        'combination': combination
                                    }
                                    unmatched_indices.remove(idx)
                                    combination_usage_stats[f"{str(combination)} (first-last fallback)"] = combination_usage_stats.get(f"{str(combination)} (first-last fallback)", 0) + 1
                                    logger.debug(f"Matched row {idx} using first-last sector pattern: {first_last_sector}")
            
            # Fallback: Try Ticket_Number value as PNR_Number (since Ticket_Number column sometimes contains PNR numbers)
            # This handles cases where Ticket_Number column has PNR values instead of ticket numbers
            if unmatched_indices and sector_excel_col:
                logger.info(f"Trying Ticket_Number as PNR_Number fallback for {len(unmatched_indices)} unmatched rows")
                
                # Find Ticket_Number column in Excel
                ticket_excel_col = None
                for excel_name, db_name in excel_to_db_mapping.items():
                    if db_name == 'Ticket_Number':
                        ticket_excel_col = excel_name
                        break
                if ticket_excel_col is None:
                    # Try to find it directly
                    for col in df_excel.columns:
                        if col.lower().strip() in ['ticket number', 'ticketnumber', 'ticket_num', 'airline ticket no.', 'ticket num/final booking']:
                            ticket_excel_col = col
                            break
                
                # Find PNR_Number column in Excel (for reference, but we'll use Ticket_Number value)
                pnr_excel_col = None
                for excel_name, db_name in excel_to_db_mapping.items():
                    if db_name == 'PNR_Number':
                        pnr_excel_col = excel_name
                        break
                if pnr_excel_col is None:
                    for col in df_excel.columns:
                        if col.lower().strip() in ['pnr', 'pnr number', 'pnrnumber', 'airline pnr', 'airline pnr/prov. booking']:
                            pnr_excel_col = col
                            break
                
                # Only proceed if we have both Ticket_Number and Travel_Sector columns
                if ticket_excel_col and sector_excel_col:
                    # Build keys using Ticket_Number value as PNR_Number
                    row_keys_ticket_as_pnr = {}
                    rows_with_valid_keys_ticket_as_pnr = []
                    
                    for idx in list(unmatched_indices):
                        row = batch_df.loc[idx]
                        
                        # Get Ticket_Number value
                        if ticket_excel_col not in row.index:
                            continue
                        
                        ticket_value = row[ticket_excel_col]
                        # Normalize ticket value for FCM files
                        ticket_value = self.normalize_pnr_ticket_value(ticket_value, 'Ticket_Number')
                        if self.is_empty_value(ticket_value):
                            continue
                        
                        # Get sector value
                        if sector_excel_col not in row.index:
                            continue
                        
                        sector_value = row[sector_excel_col]
                        if self.is_empty_value(sector_value):
                            continue
                        
                        # Get all sector combinations including individuals, pairs, and first-last
                        sectors_to_query_ticket = self.get_all_sector_combinations(sector_value)
                        
                        # Build keys for each sector using Ticket_Number as PNR_Number
                        # For FCM files, also try with prefix variations
                        key_values_list_ticket = []

                        # Get ticket variations (includes dash variation for length > 10, plus FCM prefixes if applicable)
                        ticket_variations = self.get_fcm_ticket_variations(ticket_value)
                        
                        for sector in sectors_to_query_ticket:
                            for ticket_var in ticket_variations:
                                key_values_ticket = [ticket_var, sector]  # [PNR_Number, Travel_Sector]
                                key_values_list_ticket.append(tuple(key_values_ticket))
                        
                        if key_values_list_ticket:
                            row_keys_ticket_as_pnr[idx] = key_values_list_ticket
                            rows_with_valid_keys_ticket_as_pnr.append(idx)
                    
                    # If we have valid keys, execute batch query using PNR_Number + Travel_Sector
                    if row_keys_ticket_as_pnr:
                        # Collect all unique keys
                        all_keys_ticket_as_pnr = []
                        key_to_row_indices_ticket_as_pnr = {}
                        key_to_pattern_ticket_as_pnr = {}  # Map key -> 'CN' or 'IN' pattern based on GST INVOICE NO
                        
                        for idx, keys_list in row_keys_ticket_as_pnr.items():
                            # Get GST INVOICE NO pattern for this row
                            invoice_pattern = None
                            if vendor_invoice_col and vendor_invoice_col in batch_df.loc[idx].index:
                                vendor_invoice_val = batch_df.loc[idx, vendor_invoice_col]
                                if not self.is_empty_value(vendor_invoice_val):
                                    vendor_invoice_str = str(vendor_invoice_val).strip().upper()
                                    if vendor_invoice_str.startswith('CN'):
                                        invoice_pattern = 'CN'
                                    elif vendor_invoice_str.startswith('IN'):
                                        invoice_pattern = 'IN'
                            
                            for key in keys_list:
                                if key not in key_to_row_indices_ticket_as_pnr:
                                    all_keys_ticket_as_pnr.append(key)
                                    key_to_row_indices_ticket_as_pnr[key] = []
                                    key_to_pattern_ticket_as_pnr[key] = None
                                key_to_row_indices_ticket_as_pnr[key].append(idx)
                                # Set pattern for key (if multiple rows use same key, use first non-None pattern)
                                if invoice_pattern and key_to_pattern_ticket_as_pnr[key] is None:
                                    key_to_pattern_ticket_as_pnr[key] = invoice_pattern
                        
                        unique_keys_ticket_as_pnr = list(dict.fromkeys(all_keys_ticket_as_pnr))
                        
                        # Build batch query using PNR_Number and Travel_Sector
                        pnr_sector_combination = ['PNR_Number', 'Travel_Sector']
                        ref_cols_str_ticket = ', '.join([f"`{c}`" for c in pnr_sector_combination])
                        missing_columns_str_ticket = ', '.join([f"`{col}`" for col in missing_columns])
                        placeholders_ticket = ', '.join(["(" + ", ".join(["%s"] * len(pnr_sector_combination)) + ")" for _ in unique_keys_ticket_as_pnr])
                        
                        # Add NULL checks
                        null_checks_ticket = ' AND '.join([f"`{c}` IS NOT NULL" for c in pnr_sector_combination])
                        
                        # Add ORDER BY to prioritize Original_Invoice_Number based on pattern
                        order_by_clause_ticket = "ORDER BY `Original_Invoice_Number` IS NOT NULL DESC, `Created_Date` DESC"
                        
                        query_ticket = (
                            f"SELECT {ref_cols_str_ticket}, {missing_columns_str_ticket} "
                            f"FROM `{table_name}` "
                            f"WHERE ({ref_cols_str_ticket}) IN ({placeholders_ticket}) "
                            f"AND {null_checks_ticket} "
                            f"{order_by_clause_ticket}"
                        )
                        params_ticket = [v for key in unique_keys_ticket_as_pnr for v in key]
                        
                        # Execute batch query
                        results_ticket_as_pnr = self.execute_query_with_retry(query_ticket, params_ticket)
                        
                        # Group results by key first (multiple results per key possible)
                        results_by_key_ticket_as_pnr = {}
                        for r in results_ticket_as_pnr:
                            key = tuple(r[c] for c in pnr_sector_combination)
                            if key not in results_by_key_ticket_as_pnr:
                                results_by_key_ticket_as_pnr[key] = []
                            results_by_key_ticket_as_pnr[key].append({col: r.get(col) for col in missing_columns})
                        
                        # Build lookup dictionary from results
                        # Prioritize based on GST INVOICE NO pattern and Original_Invoice_Number
                        lookup_ticket_as_pnr = {}
                        for key in results_by_key_ticket_as_pnr:
                            key_results = results_by_key_ticket_as_pnr[key]
                            pattern = key_to_pattern_ticket_as_pnr.get(key)
                            
                            if pattern == 'CN':
                                # For CN: ONLY match records with Original_Invoice_Number NOT NULL
                                # No fallback - if no matching record, don't add to lookup
                                matched_result = None
                                
                                for result in key_results:
                                    original_inv_num = result.get('Original_Invoice_Number')
                                    if not self.is_empty_value(original_inv_num):
                                        matched_result = result
                                        break
                                
                                if matched_result is not None:
                                    lookup_ticket_as_pnr[key] = matched_result
                            elif pattern == 'IN':
                                # For IN: ONLY match records with Original_Invoice_Number NULL
                                # No fallback - if no matching record, don't add to lookup
                                matched_result = None
                                
                                for result in key_results:
                                    original_inv_num = result.get('Original_Invoice_Number')
                                    if self.is_empty_value(original_inv_num):
                                        matched_result = result
                                        break
                                
                                if matched_result is not None:
                                    lookup_ticket_as_pnr[key] = matched_result
                            else:
                                # No pattern or unknown pattern: use first result
                                lookup_ticket_as_pnr[key] = key_results[0]
                        
                        # Match rows with results
                        for idx in rows_with_valid_keys_ticket_as_pnr:
                            if idx in unmatched_indices:
                                keys_list = row_keys_ticket_as_pnr[idx]
                                
                                # Collect matching results
                                all_results_ticket = []
                                for key in keys_list:
                                    if key in lookup_ticket_as_pnr:
                                        all_results_ticket.append(lookup_ticket_as_pnr[key])
                                
                                if all_results_ticket:
                                    # Store all matching results as separate entries
                                    # Each result will create a separate output row
                                    row_matches[idx] = {
                                        'matched_data_list': all_results_ticket,  # List of all matching records
                                        'combination': ['PNR_Number', 'Travel_Sector']  # Mark as PNR_Number match
                                    }
                                    unmatched_indices.remove(idx)
                                    combination_usage_stats["Ticket_Number_as_PNR_Number (fallback)"] = combination_usage_stats.get("Ticket_Number_as_PNR_Number (fallback)", 0) + 1
                                    logger.debug(f"Matched row {idx} using Ticket_Number value as PNR_Number: {ticket_value}")
#pnrbookingdate logic start2            
            # Fallback: Try PNR_Number + Booking_Date matching with date normalization
            # This handles cases where Travel_Sector doesn't match but PNR and Booking Date do
            if unmatched_indices:
                logger.info(f"Trying PNR_Number + Booking_Date fallback for {len(unmatched_indices)} unmatched rows")
                
                # Find PNR_Number column in Excel
                pnr_excel_col_for_date = None
                for excel_name, db_name in excel_to_db_mapping.items():
                    if db_name == 'PNR_Number':
                        pnr_excel_col_for_date = excel_name
                        break
                if pnr_excel_col_for_date is None:
                    for col in df_excel.columns:
                        # Include 'airlne pnr' (typo in some files) and other variants
                        if col.lower().strip() in ['pnr', 'pnr number', 'pnrnumber', 'airline pnr', 'airlne pnr', 'airline pnr/prov. booking']:
                            pnr_excel_col_for_date = col
                            break
                
                # Find Booking_Date column in Excel
                booking_date_excel_col = None
                booking_date_col_names = ['Booking Date', 'booking date', 'BookingDate', 'Booking_Date', 'booking_date']
                for col in df_excel.columns:
                    if col in booking_date_col_names or col.lower().strip() in [n.lower() for n in booking_date_col_names]:
                        booking_date_excel_col = col
                        break
                
                logger.info(f"PNR+Booking_Date fallback: PNR column='{pnr_excel_col_for_date}', Booking_Date column='{booking_date_excel_col}'")
                
                # Only proceed if we have both PNR_Number and Booking_Date columns
                if pnr_excel_col_for_date and booking_date_excel_col:
                    # Build keys using PNR_Number and normalized Booking_Date
                    row_keys_pnr_date = {}
                    rows_with_valid_keys_pnr_date = []
                    excel_normalized_dates = {}  # Store normalized dates for matching
                    skipped_empty_pnr = 0
                    skipped_empty_date = 0
                    skipped_parse_fail = 0
                    
                    for idx in list(unmatched_indices):
                        row = batch_df.loc[idx]
                        
                        # Get PNR_Number value
                        if pnr_excel_col_for_date not in row.index:
                            continue
                        
                        pnr_value = row[pnr_excel_col_for_date]
                        # Normalize PNR value
                        pnr_value = self.normalize_pnr_ticket_value(pnr_value, 'PNR_Number')
                        if self.is_empty_value(pnr_value):
                            skipped_empty_pnr += 1
                            continue
                        
                        # Get Booking_Date value
                        if booking_date_excel_col not in row.index:
                            continue
                        
                        booking_date_value = row[booking_date_excel_col]
                        if self.is_empty_value(booking_date_value):
                            skipped_empty_date += 1
                            continue
                        
                        # Normalize Excel booking date to canonical YYYY-MM-DD
                        normalized_excel_date = normalize_excel_booking_date(booking_date_value)
                        if normalized_excel_date is None:
                            skipped_parse_fail += 1
                            # Log at INFO level for visibility
                            logger.info(f"Row {idx}: Failed to parse Excel booking date: {booking_date_value!r} (type: {type(booking_date_value).__name__}), PNR: {pnr_value}")
                            continue
                        
                        # Store the normalized date for later matching
                        excel_normalized_dates[idx] = normalized_excel_date
                        
                        # For FCM files, also try with prefix variations
                        pnr_variations = self.get_fcm_ticket_variations(pnr_value)
                        
                        if pnr_variations:
                            row_keys_pnr_date[idx] = pnr_variations
                            rows_with_valid_keys_pnr_date.append(idx)
                    
                    logger.info(f"PNR+Booking_Date fallback: {len(rows_with_valid_keys_pnr_date)} rows with valid keys (skipped: {skipped_empty_pnr} empty PNR, {skipped_empty_date} empty date, {skipped_parse_fail} parse failures)")
                    
                    # If we have valid rows, execute batch query using PNR_Number only, then filter by Booking_Date
                    if row_keys_pnr_date:
                        # Collect all unique PNR values
                        all_pnr_values = []
                        pnr_to_row_indices = {}
                        pnr_to_pattern = {}  # Map PNR -> 'CN' or 'IN' pattern based on GST INVOICE NO
                        
                        for idx, pnr_list in row_keys_pnr_date.items():
                            # Get GST INVOICE NO pattern for this row
                            invoice_pattern = None
                            if vendor_invoice_col and vendor_invoice_col in batch_df.loc[idx].index:
                                vendor_invoice_val = batch_df.loc[idx, vendor_invoice_col]
                                if not self.is_empty_value(vendor_invoice_val):
                                    vendor_invoice_str = str(vendor_invoice_val).strip().upper()
                                    if vendor_invoice_str.startswith('CN'):
                                        invoice_pattern = 'CN'
                                    elif vendor_invoice_str.startswith('IN'):
                                        invoice_pattern = 'IN'
                            
                            for pnr in pnr_list:
                                if pnr not in pnr_to_row_indices:
                                    all_pnr_values.append(pnr)
                                    pnr_to_row_indices[pnr] = []
                                    pnr_to_pattern[pnr] = None
                                pnr_to_row_indices[pnr].append(idx)
                                if invoice_pattern and pnr_to_pattern[pnr] is None:
                                    pnr_to_pattern[pnr] = invoice_pattern
                        
                        unique_pnr_values = list(dict.fromkeys(all_pnr_values))
                        logger.info(f"PNR+Booking_Date fallback: Querying DB for {len(unique_pnr_values)} unique PNR values")
                        
                        # Build batch query using PNR_Number only, also fetch Booking_Date from DB
                        missing_cols_with_booking = list(missing_columns) + ['Booking_Date'] if 'Booking_Date' not in missing_columns else list(missing_columns)
                        missing_columns_str_pnr_date = ', '.join([f"`{col}`" for col in missing_cols_with_booking])
                        placeholders_pnr = ', '.join(['%s' for _ in unique_pnr_values])
                        
                        # Add ORDER BY to prioritize Original_Invoice_Number based on pattern
                        order_by_clause_pnr_date = "ORDER BY `Original_Invoice_Number` IS NOT NULL DESC, `Created_Date` DESC"
                        
                        query_pnr_date = (
                            f"SELECT `PNR_Number`, {missing_columns_str_pnr_date} "
                            f"FROM `{table_name}` "
                            f"WHERE `PNR_Number` IN ({placeholders_pnr}) "
                            f"AND `PNR_Number` IS NOT NULL "
                            f"{order_by_clause_pnr_date}"
                        )
                        params_pnr_date = unique_pnr_values
                        
                        # Execute batch query
                        results_pnr_date = self.execute_query_with_retry(query_pnr_date, params_pnr_date)
                        logger.info(f"PNR+Booking_Date fallback: Got {len(results_pnr_date)} results from DB")
                        
                        # Group results by PNR and normalized Booking_Date
                        results_by_pnr = {}
                        db_date_parse_failures = 0
                        for r in results_pnr_date:
                            pnr_key = r.get('PNR_Number')
                            if pnr_key not in results_by_pnr:
                                results_by_pnr[pnr_key] = []
                            
                            # Normalize DB booking date for comparison
                            db_booking_date = r.get('Booking_Date')
                            normalized_db_date = normalize_db_booking_date(db_booking_date)
                            if normalized_db_date is None and db_booking_date is not None:
                                db_date_parse_failures += 1
                                logger.debug(f"DB date parse failure: PNR={pnr_key}, Booking_Date={db_booking_date!r} (type: {type(db_booking_date).__name__})")
                            
                            result_data = {col: r.get(col) for col in missing_columns}
                            result_data['_normalized_booking_date'] = normalized_db_date
                            result_data['_raw_db_booking_date'] = db_booking_date
                            results_by_pnr[pnr_key].append(result_data)
                        
                        if db_date_parse_failures > 0:
                            logger.info(f"PNR+Booking_Date fallback: {db_date_parse_failures} DB date parse failures")
                        
                        # Match rows by comparing normalized dates
                        matches_found = 0
                        date_mismatches = 0
                        for idx in rows_with_valid_keys_pnr_date:
                            if idx in unmatched_indices:
                                pnr_list = row_keys_pnr_date[idx]
                                excel_normalized = excel_normalized_dates.get(idx)
                                
                                if excel_normalized is None:
                                    continue
                                
                                all_results_pnr_date = []
                                matched_pnr = None
                                
                                # Look for matches across all PNR variations
                                for pnr in pnr_list:
                                    if pnr in results_by_pnr:
                                        pattern = pnr_to_pattern.get(pnr)
                                        
                                        for result in results_by_pnr[pnr]:
                                            db_normalized = result.get('_normalized_booking_date')
                                            
                                            # Check if dates match
                                            if excel_normalized == db_normalized:
                                                # Apply CN/IN filtering
                                                original_inv_num = result.get('Original_Invoice_Number')
                                                
                                                if pattern == 'CN':
                                                    if self.is_empty_value(original_inv_num):
                                                        continue  # Skip if CN but no Original_Invoice_Number
                                                elif pattern == 'IN':
                                                    if not self.is_empty_value(original_inv_num):
                                                        continue  # Skip if IN but has Original_Invoice_Number
                                                
                                                # Remove internal key before storing
                                                result_copy = {k: v for k, v in result.items() if not k.startswith('_')}
                                                all_results_pnr_date.append(result_copy)
                                                matched_pnr = pnr
                                                break  # Take first matching result per PNR
                                            else:
                                                # Log date mismatch for debugging
                                                if date_mismatches < 5:  # Only log first 5 mismatches
                                                    raw_db_date = result.get('_raw_db_booking_date')
                                                    logger.debug(f"Date mismatch for PNR {pnr}: Excel={excel_normalized}, DB={db_normalized} (raw: {raw_db_date!r})")
                                                date_mismatches += 1
                                    
                                    if all_results_pnr_date:
                                        break  # Found a match, stop looking
                                
                                if all_results_pnr_date:
                                    row_matches[idx] = {
                                        'matched_data_list': all_results_pnr_date,
                                        'combination': ['PNR_Number', 'Booking_Date']
                                    }
                                    unmatched_indices.remove(idx)
                                    matches_found += 1
                                    combination_usage_stats["PNR_Number_Booking_Date (fallback)"] = combination_usage_stats.get("PNR_Number_Booking_Date (fallback)", 0) + 1
                                    logger.debug(f"Matched row {idx} using PNR_Number + Booking_Date: {matched_pnr}, {excel_normalized}")
                        
                        logger.info(f"PNR+Booking_Date fallback: {matches_found} matches found, {date_mismatches} date mismatches")
                else:
                    if not pnr_excel_col_for_date:
                        logger.info("PNR_Number + Booking_Date fallback skipped: PNR column not found in Excel")
                    if not booking_date_excel_col:
                        logger.info("PNR_Number + Booking_Date fallback skipped: Booking_Date column not found in Excel")
#pnrbookingdate logic end2            
            # Build enriched rows in original order (preserve exact row sequence)
            # Iterate by position to ensure order is maintained
            for pos in range(len(batch_df)):
                idx = batch_df.index[pos]
                row_data = batch_df.iloc[pos].to_dict()
                
                # Get matched data from database (if row was matched)
                if idx in row_matches:
                    match_info = row_matches[idx]
                    matched_data_list = match_info.get('matched_data_list', [])
                    
                    # Create a separate output row for each matching database record
                    for row_index, matched_db_data in enumerate(matched_data_list):
                        # Create a new row dict with all required columns
                        complete_row = {}
                        
                        # Process each column according to output_column_mapping
                        for db_col, output_col, excel_only in output_column_mapping:
                            if excel_only:
                                # Skip Passenger_Name for duplicate rows (only include in first row)
                                if output_col == 'Passenger_Name' and row_index > 0:
                                    complete_row[output_col] = None
                                    continue
                                
                                # Excel-only columns: only take from Excel if column exists
                                excel_col_found = None
                                excel_names = excel_column_mappings.get(output_col, [])
                                for excel_name in excel_names:
                                    matched_col = self.find_column_case_insensitive(excel_name, list(df_excel.columns))
                                    if matched_col:
                                        excel_col_found = matched_col
                                        break
                                
                                if excel_col_found and excel_col_found in row_data:
                                    # For Excel-only columns, use output_col as the key directly
                                    value = row_data[excel_col_found]
                                    
                                    # Apply date conversion for date columns
                                    if output_col in ['Booking_Date', 'Travel_Date']:
                                        value = self.convert_excel_date(value)
                                    
                                    complete_row[output_col] = value
                                else:
                                    complete_row[output_col] = None
                            else:
                                # DB columns: always fetch from DB (even if exists in Excel)
                                # Use matched data from database
                                complete_row[db_col] = matched_db_data.get(db_col)
                        
                        # Check if Total Fare (Including GST) from Excel differs from Invoice_Total from DB
                        # If different, recalculate Invoice_Total as sum of Taxable_Amount + NonTaxable_Amount + Invoice_Total_GST
                        ticket_amount_value = complete_row.get('Ticket_Amount')
                        invoice_total_value = complete_row.get('Invoice_Total')
                        
                        if ticket_amount_value is not None and invoice_total_value is not None:
                            try:
                                excel_amount = float(ticket_amount_value)
                                db_amount = float(invoice_total_value)
                                
                                # If amounts don't match, recalculate
                                if abs(excel_amount - db_amount) > 0.01:  # Allow small tolerance for float comparison
                                    taxable = float(complete_row.get('Taxable_Amount') or 0)
                                    non_taxable = float(complete_row.get('NonTaxable_Amount') or 0)
                                    total_gst = float(complete_row.get('Invoice_Total_GST') or 0)
                                    complete_row['Invoice_Total'] = taxable + non_taxable + total_gst
                            except (ValueError, TypeError):
                                pass  # Keep original value if conversion fails
                        
                        enriched_data.append(complete_row)
                    
                    match_count += len(matched_data_list)  # Count each database record as a match
                else:
                    # No match found - create row with only Excel data
                    complete_row = {}
                    
                    # Process each column according to output_column_mapping
                    for db_col, output_col, excel_only in output_column_mapping:
                        if excel_only:
                            # Excel-only columns: only take from Excel if column exists
                            excel_col_found = None
                            excel_names = excel_column_mappings.get(output_col, [])
                            for excel_name in excel_names:
                                matched_col = self.find_column_case_insensitive(excel_name, list(df_excel.columns))
                                if matched_col:
                                    excel_col_found = matched_col
                                    break
                            
                            if excel_col_found and excel_col_found in row_data:
                                # For Excel-only columns, use output_col as the key directly
                                value = row_data[excel_col_found]
                                
                                # Apply date conversion for date columns
                                if output_col in ['Booking_Date', 'Travel_Date']:
                                    value = self.convert_excel_date(value)
                                
                                complete_row[output_col] = value
                            else:
                                complete_row[output_col] = None
                        else:
                            # DB columns: no match, so set to None
                            complete_row[db_col] = None
                    
                    enriched_data.append(complete_row)
                    no_match_count += 1
        
        # Log combination usage statistics
        logger.info("Combination usage statistics:")
        for combo_str, count in combination_usage_stats.items():
            logger.info(f"  {combo_str}: {count} matches")
        
        # Create final DataFrame
        df_enriched = pd.DataFrame(enriched_data)
        
        # Build column rename mapping: internal column name -> output column name
        rename_dict = {}
        for db_col, output_col, excel_only in output_column_mapping:
            if excel_only:
                # Excel-only columns: output_col is already the key
                if output_col in df_enriched.columns:
                    rename_dict[output_col] = output_col  # Already correct name
            else:
                # DB columns: db_col -> output_col
                if db_col in df_enriched.columns:
                    rename_dict[db_col] = output_col
        
        # Apply column rename mapping
        if rename_dict:
            df_enriched = df_enriched.rename(columns=rename_dict)
        
        # Build final column order based on output_column_mapping
        final_column_order = []
        for db_col, output_col, excel_only in output_column_mapping:
            if output_col in df_enriched.columns:
                final_column_order.append(output_col)
        
        # Only include columns that exist in the DataFrame
        final_column_order = [col for col in final_column_order if col in df_enriched.columns]
        
        # Add any remaining columns that weren't in the mapping (shouldn't happen, but safety check)
        remaining_cols = [col for col in df_enriched.columns if col not in final_column_order]
        if remaining_cols:
            logger.warning(f"Found unexpected columns: {remaining_cols}")
            final_column_order.extend(remaining_cols)
        
        # Reorder DataFrame to match exact order
        df_enriched = df_enriched[final_column_order]
        
        logger.info(f"Sheet processing complete: {len(df_enriched)} rows, {match_count} matches, {no_match_count} no matches")
        logger.info(f"Output columns ({len(final_column_order)}): {final_column_order}")
        
        return df_enriched

    def enrich_data(self, excel_path: str, table_name: str, 
                   possible_reference_combinations: List[List[str]] = None,
                   column_mapping: Dict[str, str] = None,
                   output_path: Optional[str] = None):
        """
        Enhanced data enrichment with dynamic column detection and batch processing.
        Returns either pd.DataFrame (single sheet) or Dict[str, pd.DataFrame] (multiple sheets).
        """
        # Store current file path for FCM detection
        self.current_file_path = excel_path
        
        if column_mapping is None:
            column_mapping = {}
        if possible_reference_combinations is None:
            possible_reference_combinations = POSSIBLE_REFERENCE_COMBINATIONS
        
        # Validate file
        if not self.validate_file(excel_path):
            return None
        
        # Read file
        data = self.read_file_safely(excel_path)
        if data is None:
            return None
        
        # Handle multiple sheets
        if isinstance(data, dict):
            logger.info(f"Processing {len(data)} separate sheets")
            enriched_sheets = {}
            for sheet_name, df_sheet in data.items():
                logger.info(f"Processing sheet: {sheet_name}")
                df_enriched = self._enrich_single_dataframe(
                    df_sheet, table_name, possible_reference_combinations, column_mapping
                )
                if df_enriched is not None:
                    enriched_sheets[sheet_name] = df_enriched
            
            # Save output with separate sheets
            if output_path and enriched_sheets:
                try:
                    output_extension = os.path.splitext(output_path)[1].lower()
                    if output_extension == '.csv':
                        # For CSV, combine all sheets
                        df_combined = pd.concat(list(enriched_sheets.values()), ignore_index=True)
                        df_combined.to_csv(output_path, index=False)
                        logger.info(f"Data saved to: {output_path}")
                        
                        # Merge GSTR 2B/3B filing status data
                        try:
                            if GSTR_DIRECTORY and os.path.exists(GSTR_DIRECTORY):
                                logger.info("Merging GSTR 2B/3B filing status data...")
                                merge_gstr_data(output_path, GSTR_DIRECTORY)
                            else:
                                logger.warning(f"GSTR directory not found or not configured: {GSTR_DIRECTORY}. Skipping GSTR merge.")
                        except Exception as gstr_error:
                            logger.warning(f"Could not merge GSTR data: {gstr_error}")
                        
                        # Split data into Invoice, Credit_Note, and Zero sheets in a single file
                        try:
                            logger.info("Splitting data into Invoice, Credit_Note, and Zero sheets...")
                            split_output_path = split_to_sheets(output_path)
                            if split_output_path:
                                logger.info(f"Data split into sheets: {split_output_path}")
                                # Process duplicate invoice numbers in each sheet
                                try:
                                    logger.info("Processing duplicate invoice numbers in sheets...")
                                    processed_files = process_multiple_files(
                                        [split_output_path],
                                        invoice_number_column='Invoice_Number'
                                    )
                                    logger.info("Completed processing duplicate invoice numbers")
                                except Exception as dedup_error:
                                    logger.warning(f"Could not process duplicate invoice numbers: {dedup_error}")
                        except Exception as split_error:
                            logger.warning(f"Could not split data into sheets: {split_error}")
                    else:
                        # Save each sheet separately in Excel
                        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                            for sheet_name, df_enriched in enriched_sheets.items():
                                df_enriched.to_excel(writer, sheet_name=sheet_name, index=False)
                        logger.info(f"Data saved to {output_path} with {len(enriched_sheets)} sheets")
                        
                        # Apply header formatting
                        try:
                            self.apply_header_formatting(excel_path, output_path)
                        except Exception as format_error:
                            logger.warning(f"Could not apply header formatting: {format_error}")
                        
                        # Apply date formatting
                        try:
                            self.format_date_columns(output_path)
                        except Exception as date_format_error:
                            logger.warning(f"Could not apply date formatting: {date_format_error}")
                        
                        # Merge GSTR 2B/3B filing status data
                        try:
                            if GSTR_DIRECTORY and os.path.exists(GSTR_DIRECTORY):
                                logger.info("Merging GSTR 2B/3B filing status data...")
                                merge_gstr_data(output_path, GSTR_DIRECTORY)
                                # Re-apply formatting after merge
                                try:
                                    self.apply_header_formatting(excel_path, output_path)
                                    self.format_date_columns(output_path)
                                except Exception as format_error:
                                    logger.warning(f"Could not re-apply formatting after GSTR merge: {format_error}")
                            else:
                                logger.warning(f"GSTR directory not found or not configured: {GSTR_DIRECTORY}. Skipping GSTR merge.")
                        except Exception as gstr_error:
                            logger.warning(f"Could not merge GSTR data: {gstr_error}")
                        
                        # Split data into Invoice, Credit_Note, and Zero sheets in a single file
                        try:
                            logger.info("Splitting data into Invoice, Credit_Note, and Zero sheets...")
                            split_output_path = split_to_sheets(output_path)
                            if split_output_path:
                                logger.info(f"Data split into sheets: {split_output_path}")
                                # Process duplicate invoice numbers in each sheet
                                try:
                                    logger.info("Processing duplicate invoice numbers in sheets...")
                                    processed_files = process_multiple_files(
                                        [split_output_path],
                                        invoice_number_column='Invoice_Number'
                                    )
                                    logger.info("Completed processing duplicate invoice numbers")
                                except Exception as dedup_error:
                                    logger.warning(f"Could not process duplicate invoice numbers: {dedup_error}")
                        except Exception as split_error:
                            logger.warning(f"Could not split data into sheets: {split_error}")
                except Exception as e:
                    logger.error(f"Error saving file: {e}")
            
            return enriched_sheets if enriched_sheets else None
        
        # Handle single sheet/DataFrame
        else:
            df_enriched = self._enrich_single_dataframe(
                data, table_name, possible_reference_combinations, column_mapping
            )
            
            # Save output
            if output_path and df_enriched is not None:
                try:
                    output_extension = os.path.splitext(output_path)[1].lower()
                    if output_extension == '.csv':
                        df_enriched.to_csv(output_path, index=False)
                        logger.info(f"Data saved to: {output_path}")
                        
                        # Merge GSTR 2B/3B filing status data
                        try:
                            if GSTR_DIRECTORY and os.path.exists(GSTR_DIRECTORY):
                                logger.info("Merging GSTR 2B/3B filing status data...")
                                merge_gstr_data(output_path, GSTR_DIRECTORY)
                            else:
                                logger.warning(f"GSTR directory not found or not configured: {GSTR_DIRECTORY}. Skipping GSTR merge.")
                        except Exception as gstr_error:
                            logger.warning(f"Could not merge GSTR data: {gstr_error}")
                        
                        # Split data into Invoice, Credit_Note, and Zero sheets in a single file
                        try:
                            logger.info("Splitting data into Invoice, Credit_Note, and Zero sheets...")
                            split_output_path = split_to_sheets(output_path)
                            if split_output_path:
                                logger.info(f"Data split into sheets: {split_output_path}")
                                # Process duplicate invoice numbers in each sheet
                                try:
                                    logger.info("Processing duplicate invoice numbers in sheets...")
                                    processed_files = process_multiple_files(
                                        [split_output_path],
                                        invoice_number_column='Invoice_Number'
                                    )
                                    logger.info("Completed processing duplicate invoice numbers")
                                except Exception as dedup_error:
                                    logger.warning(f"Could not process duplicate invoice numbers: {dedup_error}")
                        except Exception as split_error:
                            logger.warning(f"Could not split data into sheets: {split_error}")
                    else:
                        df_enriched.to_excel(output_path, index=False, engine='xlsxwriter')
                        logger.info(f"Data saved to: {output_path}")
                        
                        try:
                            self.apply_header_formatting(excel_path, output_path)
                        except Exception as format_error:
                            logger.warning(f"Could not apply header formatting: {format_error}")
                        
                        # Merge GSTR 2B/3B filing status data
                        try:
                            if GSTR_DIRECTORY and os.path.exists(GSTR_DIRECTORY):
                                logger.info("Merging GSTR 2B/3B filing status data...")
                                merge_gstr_data(output_path, GSTR_DIRECTORY)
                            else:
                                logger.warning(f"GSTR directory not found or not configured: {GSTR_DIRECTORY}. Skipping GSTR merge.")
                        except Exception as gstr_error:
                            logger.warning(f"Could not merge GSTR data: {gstr_error}")
                        
                        # Apply date formatting
                        try:
                            self.format_date_columns(output_path)
                        except Exception as date_format_error:
                            logger.warning(f"Could not apply date formatting: {date_format_error}")
                        
                        # Split data into Invoice, Credit_Note, and Zero sheets in a single file
                        try:
                            logger.info("Splitting data into Invoice, Credit_Note, and Zero sheets...")
                            split_output_path = split_to_sheets(output_path)
                            if split_output_path:
                                logger.info(f"Data split into sheets: {split_output_path}")
                                # Process duplicate invoice numbers in each sheet
                                try:
                                    logger.info("Processing duplicate invoice numbers in sheets...")
                                    processed_files = process_multiple_files(
                                        [split_output_path],
                                        invoice_number_column='Invoice_Number'
                                    )
                                    logger.info("Completed processing duplicate invoice numbers")
                                except Exception as dedup_error:
                                    logger.warning(f"Could not process duplicate invoice numbers: {dedup_error}")
                        except Exception as split_error:
                            logger.warning(f"Could not split data into sheets: {split_error}")
                except Exception as e:
                    logger.error(f"Error saving file: {e}")
            
            return df_enriched


class SFTPDownloader:
    """Simple SFTP client for downloading files from remote server."""
    
    def __init__(self, host: str, port: int, username: str, password: str):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.ssh_client = None
        self.sftp_client = None
    
    def connect(self) -> bool:
        try:
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_client.connect(
                hostname=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                timeout=30
            )
            self.sftp_client = self.ssh_client.open_sftp()
            logger.info(f"Connected to SFTP server: {self.host}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to SFTP server: {e}")
            return False
    
    def disconnect(self):
        try:
            if self.sftp_client:
                self.sftp_client.close()
            if self.ssh_client:
                self.ssh_client.close()
            logger.info("SFTP connection closed")
        except Exception as e:
            logger.error(f"Error closing SFTP connection: {e}")
    
    def download_file(self, remote_path: str, local_dir: str) -> Optional[str]:
        try:
            os.makedirs(local_dir, exist_ok=True)
            filename = os.path.basename(remote_path)
            local_path = os.path.join(local_dir, filename)
            logger.info(f"Downloading {remote_path} to {local_path}")
            self.sftp_client.get(remote_path, local_path)
            logger.info(f"File downloaded successfully: {local_path}")
            return local_path
        except Exception as e:
            logger.error(f"Failed to download file: {e}")
            return None



class EmailSender:
    """Handles sending email notifications via AWS SES."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.enabled = config.get("enabled", False)
        self.recipient = config.get("recipient_email", "")
        self.sender_email = config.get("sender_email", "")
        self.subject = config.get("subject", "Data Merge Processing Report")
        self.aws_region = config.get("aws_region", "us-south-1")
        self.aws_access_key_id = config.get("aws_access_key_id", "")
        self.aws_secret_access_key = config.get("aws_secret_access_key", "")
        self.ses_client = None
    
    def _get_ses_client(self):
        """Create and return a boto3 SES client."""
        if self.ses_client is None:
            try:
                kwargs = {"region_name": self.aws_region}
                if self.aws_access_key_id and self.aws_secret_access_key:
                    kwargs["aws_access_key_id"] = self.aws_access_key_id
                    kwargs["aws_secret_access_key"] = self.aws_secret_access_key
                self.ses_client = boto3.client("ses", **kwargs)
                logger.info(f"SES client initialized for region {self.aws_region}")
            except (NoCredentialsError, BotoCoreError) as e:
                logger.error(f"Failed to initialize SES client: {e}")
                raise
        return self.ses_client
    
    def send_email(self, processing_result: Dict, log_file_path: Optional[str] = None, output_files: Optional[List[str]] = None) -> bool:
        """Send email notification with processing results via AWS SES."""
        if not self.enabled:
            logger.info("Email notifications are disabled")
            return False
        
        if not self.recipient or not self.sender_email:
            logger.warning("Email configuration incomplete - skipping email send")
            return False
        
        try:
            msg = MIMEMultipart()
            msg['From'] = self.sender_email
            msg['To'] = self.recipient
            msg['Subject'] = self.subject
            
            body = self._create_email_body(processing_result)
            msg.attach(MIMEText(body, 'html'))
            
            if output_files:
                for file_path in output_files:
                    if file_path and os.path.exists(file_path):
                        try:
                            with open(file_path, 'rb') as attachment:
                                part = MIMEBase('application', 'octet-stream')
                                part.set_payload(attachment.read())
                            encoders.encode_base64(part)
                            
                            file_ext = os.path.splitext(file_path)[1].lower()
                            if file_ext in ['.xlsx', '.xls', '.xlsb']:
                                mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                            elif file_ext == '.csv':
                                mime_type = 'text/csv'
                            else:
                                mime_type = 'application/octet-stream'
                            
                            part.add_header('Content-Type', mime_type)
                            part.add_header(
                                'Content-Disposition',
                                f'attachment; filename="{os.path.basename(file_path)}"'
                            )
                            msg.attach(part)
                            logger.info(f"Attached processed file: {os.path.basename(file_path)}")
                        except Exception as e:
                            logger.warning(f"Could not attach file {file_path}: {e}")
            
            if log_file_path and os.path.exists(log_file_path):
                try:
                    with open(log_file_path, 'rb') as attachment:
                        part = MIMEBase('application', 'octet-stream')
                        part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Type', 'text/plain')
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename="{os.path.basename(log_file_path)}"'
                    )
                    msg.attach(part)
                    logger.info(f"Attached log file: {os.path.basename(log_file_path)}")
                except Exception as e:
                    logger.warning(f"Could not attach log file: {e}")
            
            ses = self._get_ses_client()
            response = ses.send_raw_email(
                Source=self.sender_email,
                Destinations=[self.recipient],
                RawMessage={"Data": msg.as_string()},
            )
            
            message_id = response.get("MessageId", "N/A")
            logger.info(f"Email sent successfully to {self.recipient} (MessageId: {message_id})")
            return True
        
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_msg = e.response["Error"]["Message"]
            logger.error(f"SES ClientError [{error_code}]: {error_msg}")
            return False
        except NoCredentialsError:
            logger.error("AWS credentials not found. Run 'aws configure' to set up credentials.")
            return False
        except Exception as e:
            logger.error(f"Failed to send email via SES: {e}")
            return False
    
    def _create_email_body(self, result: Dict) -> str:
        """Create HTML email body with processing results."""
        status = result.get("status", "unknown")
        processed = result.get("processed", 0)
        errors = result.get("errors", 0)
        results = result.get("results", [])
        
        # Determine status color
        if status == "completed" and errors == 0:
            status_color = "green"
        elif status == "completed" and errors > 0:
            status_color = "orange"
        else:
            status_color = "red"
        
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                .header {{ background-color: #4CAF50; color: white; padding: 10px; }}
                .content {{ padding: 20px; }}
                .status {{ color: {status_color}; font-weight: bold; }}
                .summary {{ background-color: #f5f5f5; padding: 15px; margin: 10px 0; border-radius: 5px; }}
                .file-list {{ margin-top: 15px; }}
                .file-item {{ padding: 5px; margin: 5px 0; }}
                .success {{ color: green; }}
                .error {{ color: red; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>Data Merge Processing Report</h2>
            </div>
            <div class="content">
                <p><strong>Processing Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p><strong>Status:</strong> <span class="status">{status.upper()}</span></p>
                
                <div class="summary">
                    <h3>Summary</h3>
                    <p><strong>Files Processed Successfully:</strong> {processed}</p>
                    <p><strong>Errors:</strong> {errors}</p>
                    <p><strong>Total Files:</strong> {len(results)}</p>
                </div>
        """
        
        if results:
            html += """
                <div class="file-list">
                    <h3>File Details</h3>
            """
            for res in results:
                file_name = os.path.basename(res.get("file", "Unknown"))
                file_status = res.get("status", "unknown")
                status_class = "success" if file_status == "success" else "error"
                
                html += f"""
                    <div class="file-item">
                        <strong>{file_name}</strong> - <span class="{status_class}">{file_status.upper()}</span>
                """
                
                if file_status == "success":
                    rows = res.get("rows", 0)
                    output = res.get("output", "")
                    html += f"<br>Rows processed: {rows}"
                    if output:
                        html += f"<br>Output: {os.path.basename(output)}"
                
                if file_status in ["failed", "error"]:
                    error_msg = res.get("error", "Unknown error")
                    html += f"<br>Error: {error_msg}"
                
                html += "</div>"
            
            html += "</div>"
        
        html += """
            </div>
        </body>
        </html>
        """
        
        return html


class AutomatedProcessor:
    """Handles automated processing with scheduling."""
    
    def __init__(self, db_config: Dict, table_name: str, column_mapping: Dict, 
                 possible_reference_combinations: List[List[str]]):
        self.db_config = db_config
        self.table_name = table_name
        self.column_mapping = column_mapping
        self.possible_reference_combinations = possible_reference_combinations
        self.file_processor = FileProcessor(INPUT_DIRECTORY, OUTPUT_DIRECTORY, SUPPORTED_EXTENSIONS)
        self.is_running = False
        self.email_sender = EmailSender(EMAIL_CONFIG)
    
    def process_all_files(self) -> Dict[str, any]:
        """Process all files in the input directory."""
        logger.info("Starting automated file processing...")
        
        # Optional SFTP prefetch before discovering files
        try:
            if SFTP_CONFIG.get("enabled"):
                sftp_local_dir = SFTP_CONFIG.get("local_download_dir", INPUT_DIRECTORY)
                remote_path = SFTP_CONFIG.get("remote_file_path")
                if remote_path:
                    logger.info("SFTP prefetch enabled - attempting download from remote_file_path")
                    sftp = SFTPDownloader(
                        host=SFTP_CONFIG.get("host"),
                        port=SFTP_CONFIG.get("port", 22),
                        username=SFTP_CONFIG.get("username"),
                        password=SFTP_CONFIG.get("password")
                    )
                    if sftp.connect():
                        try:
                            downloaded = sftp.download_file(remote_path=remote_path, local_dir=sftp_local_dir)
                            if downloaded:
                                logger.info(f"SFTP file available at: {downloaded}")
                            else:
                                logger.warning("SFTP download did not produce a file")
                        finally:
                            sftp.disconnect()
                    else:
                        logger.error("Skipping SFTP download due to connection failure")
                else:
                    logger.info("SFTP enabled but no 'remote_file_path' provided; skipping download")
        except Exception as e:
            logger.error(f"SFTP prefetch error: {e}")
        
        # Discover files
        files_to_process = self.file_processor.discover_files()
        
        if not files_to_process:
            logger.info("No files found to process")
            return {"status": "no_files", "processed": 0, "errors": 0}
        
        # Initialize enricher
        enricher = DataEnricher(**self.db_config, debug_mode=DEBUG_MODE, debug_id=DEBUG_ID)
        
        processed_count = 0
        error_count = 0
        results = []
        
        try:
            # Connect to database
            if not enricher.connect():
                logger.error("Failed to connect to database")
                return {"status": "db_error", "processed": 0, "errors": len(files_to_process)}
            
            # Process each file
            for file_path in files_to_process:
                try:
                    logger.info(f"Processing file: {file_path}")
                    
                    # Generate output path
                    output_path = self.file_processor.get_output_path(file_path)
                    
                    # Enrich data
                    df_result = enricher.enrich_data(
                        excel_path=file_path,
                        table_name=self.table_name,
                        possible_reference_combinations=self.possible_reference_combinations,
                        column_mapping=self.column_mapping,
                        output_path=output_path
                    )
                    
                    if df_result is not None:
                        processed_count += 1
                        logger.info(f"Successfully processed: {file_path}")
                        
                        # Note: Original file is kept in place, not moved
                        
                        # Handle both DataFrame and dict (multiple sheets)
                        if isinstance(df_result, dict):
                            total_rows = sum(len(df) for df in df_result.values())
                            sheets_info = {name: len(df) for name, df in df_result.items()}
                            results.append({
                                "file": file_path,
                                "status": "success",
                                "rows": total_rows,
                                "sheets": len(df_result),
                                "sheets_info": sheets_info,
                                "output": output_path
                            })
                        else:
                            results.append({
                                "file": file_path,
                                "status": "success",
                                "rows": len(df_result),
                                "output": output_path
                            })
                    else:
                        error_count += 1
                        logger.error(f"Failed to process: {file_path}")
                        results.append({
                            "file": file_path,
                            "status": "failed",
                            "error": "Processing failed"
                        })
                        
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error processing {file_path}: {e}")
                    results.append({
                        "file": file_path,
                        "status": "error",
                        "error": str(e)
                    })
            
        except Exception as e:
            logger.error(f"Critical error during processing: {e}")
            return {"status": "critical_error", "processed": processed_count, "errors": error_count}
        
        finally:
            enricher.disconnect()
        
        # Log summary
        logger.info(f"Processing complete: {processed_count} successful, {error_count} errors")
        
        result = {
            "status": "completed",
            "processed": processed_count,
            "errors": error_count,
            "results": results
        }
        
        # Send email notification
        if EMAIL_CONFIG.get("enabled", False):
            log_file_path = f"data_merge_{datetime.now().strftime('%Y%m%d')}.log"
            # Collect all output file paths from successful processing
            output_files = [res.get("output") for res in results if res.get("status") == "success" and res.get("output")]
            self.email_sender.send_email(result, log_file_path, output_files)
        
        return result
    
    def run_scheduled_job(self):
        """Run the scheduled processing job."""
        if self.is_running:
            logger.warning("Previous job still running, skipping this execution")
            return
        
        self.is_running = True
        try:
            logger.info("Starting scheduled processing job...")
            result = self.process_all_files()
            logger.info(f"Scheduled job completed: {result}")
            
            # Email is already sent in process_all_files if enabled
        except Exception as e:
            logger.error(f"Scheduled job failed: {e}")
            # Send error notification email
            if EMAIL_CONFIG.get("enabled", False):
                error_result = {
                    "status": "error",
                    "processed": 0,
                    "errors": 1,
                    "results": [{"file": "Scheduled Job", "status": "error", "error": str(e)}]
                }
                log_file_path = f"data_merge_{datetime.now().strftime('%Y%m%d')}.log"
                self.email_sender.send_email(error_result, log_file_path, None)
        finally:
            self.is_running = False
    
    def get_schedule_config(self):
        """Get scheduling configuration from loaded config."""
        schedule_config = CONFIG.get("scheduling", {})
        schedule_time = schedule_config.get("time", "13:00")
        enabled = schedule_config.get("enabled", True)
        return schedule_time, enabled
    
    def start_scheduler(self):
        """Start the scheduler using time from config.json."""
        schedule_time, enabled = self.get_schedule_config()
        
        if not enabled:
            logger.info("Scheduling is disabled in config.json")
            return
        
        logger.info(f"Setting up daily scheduler for {schedule_time} execution (from config.json)")
        schedule.every().day.at(schedule_time).do(self.run_scheduled_job)
        
        logger.info("Scheduler started. Waiting for scheduled execution...")
        while True:
            schedule.run_pending()
            time.sleep(1)  # Check every second for precise scheduling


# ====================================================================
# MAIN EXECUTION
# ====================================================================

if __name__ == "__main__":
    import sys
    
    # Check command line arguments
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
    else:
        mode = "manual"  # Default mode
    
    print("="*60)
    print("ENHANCED DATA ENRICHMENT TOOL")
    print("="*60)
    print(f"Mode: {mode.upper()}")
    print(f"Input Directory: {INPUT_DIRECTORY}")
    print(f"Output Directory: {OUTPUT_DIRECTORY}")
    print(f"Database: {DB_CONFIG['database']}")
    print(f"Table: {TABLE_NAME}")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Debug mode: {DEBUG_MODE}")
    if SFTP_CONFIG.get("enabled"):
        print(f"SFTP: ON -> {SFTP_CONFIG.get('host')} | Remote: {SFTP_CONFIG.get('remote_file_path')} | Local: {SFTP_CONFIG.get('local_download_dir', INPUT_DIRECTORY)}")
    else:
        print("SFTP: OFF")
    print("="*60)
    
    # Initialize processor
    processor = AutomatedProcessor(
        db_config=DB_CONFIG,
        table_name=TABLE_NAME,
        column_mapping=COLUMN_MAPPING,
        possible_reference_combinations=POSSIBLE_REFERENCE_COMBINATIONS
    )
    
    if mode == "auto" or mode == "scheduler":
        # Run in automated/scheduled mode
        logger.info("Starting in automated mode with daily scheduling")
        try:
            processor.start_scheduler()
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
        except Exception as e:
            logger.error(f"Scheduler error: {e}")
    
    elif mode == "process":
        # Process all files once
        logger.info("Starting one-time processing of all files")
        try:
            result = processor.process_all_files()
            print("\n" + "="*60)
            print("PROCESSING COMPLETE!")
            print("="*60)
            print(f"Status: {result['status']}")
            print(f"Files processed: {result['processed']}")
            print(f"Errors: {result['errors']}")
            if 'results' in result:
                print("\nDetailed Results:")
                for res in result['results']:
                    print(f"  {res['file']}: {res['status']}")
            print("="*60)
        except Exception as e:
            logger.error(f"Processing error: {e}")
            print(f"\nERROR: {e}")
    
    else:
        # Manual mode - process single file (legacy behavior)
        logger.info("Starting in manual mode")
        
        # Check if input directory has files
        files = processor.file_processor.discover_files()
        if files:
            print(f"\nFound {len(files)} files in input directory:")
            for i, file in enumerate(files, 1):
                print(f"  {i}. {os.path.basename(file)}")
            
            if len(files) == 1:
                # Process the single file
                file_to_process = files[0]
                output_path = processor.file_processor.get_output_path(file_to_process)
                
                print(f"\nProcessing: {os.path.basename(file_to_process)}")
                print(f"Output: {output_path}")
                
                try:
                    result = processor.process_all_files()
                    print("\n" + "="*60)
                    print("SUCCESS!")
                    print("="*60)
                    print(f"Files processed: {result['processed']}")
                    print(f"Errors: {result['errors']}")
                    print("="*60)
                except Exception as e:
                    print(f"\nERROR: {e}")
                    logger.error(f"Manual processing error: {e}")
            else:
                print(f"\nMultiple files found. Use 'python data_merge.py process' to process all files")
                print("Or use 'python data_merge.py auto' to start automated processing")
        else:
            print(f"\nNo files found in {INPUT_DIRECTORY}")
            print("Please add Excel/CSV files to the input directory")