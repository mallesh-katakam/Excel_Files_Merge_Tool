"""
Data Splitter Module
Splits processed data into three files based on Vendor K3 Amount and Ticket_Amount:
- Invoice: Vendor K3 Amount > 0
- credit_note: Vendor K3 Amount < 0
- zero: Vendor K3 Amount == 0
Preserves the original data order.
"""

import pandas as pd
import os
import logging
from typing import Optional, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)


def split_by_vendor_k3_amount(
    input_file_path: str,
    output_directory: Optional[str] = None,
    vendor_k3_column: str = 'Vendor K3 Amount',
    ticket_amount_column: str = 'Ticket_Amount'
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Split data into three files based on Vendor K3 Amount.
    Invoice: Vendor K3 Amount > 0
    Credit Note: Vendor K3 Amount < 0
    Zero: Vendor K3 Amount == 0
    
    Args:
        input_file_path: Path to the input file (Excel or CSV)
        output_directory: Directory to save split files. If None, uses same directory as input file.
        vendor_k3_column: Name of the column containing Vendor K3 Amount
        ticket_amount_column: Name of the column containing Ticket_Amount (not used for splitting, kept for compatibility)
    
    Returns:
        Tuple of (credit_note_file_path, invoice_file_path, zero_file_path) or (None, None, None) if error
    """
    try:
        # Determine output directory
        if output_directory is None:
            output_directory = os.path.dirname(input_file_path)
        
        # Get base filename without extension
        base_name = Path(input_file_path).stem
        file_ext = Path(input_file_path).suffix.lower()
        
        # Read the input file
        logger.info(f"Reading file for splitting: {input_file_path}")
        
        if file_ext == '.csv':
            df = pd.read_csv(input_file_path)
        elif file_ext in ['.xlsx', '.xls']:
            # Read first sheet if multiple sheets exist
            df = pd.read_excel(input_file_path, sheet_name=0)
        else:
            logger.error(f"Unsupported file format: {file_ext}")
            return None, None
        
        if df.empty:
            logger.warning(f"Input file is empty: {input_file_path}")
            return None, None, None
        
        # Check if required columns exist
        if vendor_k3_column not in df.columns:
            logger.error(f"Column '{vendor_k3_column}' not found in file. Available columns: {list(df.columns)}")
            return None, None, None
        
        # Convert column to numeric, handling any non-numeric values
        df[vendor_k3_column] = pd.to_numeric(df[vendor_k3_column], errors='coerce')
        
        # Split data based on Vendor K3 Amount into three categories
        # Invoice: Vendor K3 Amount > 0
        # Credit Note: Vendor K3 Amount < 0
        # Zero: Vendor K3 Amount == 0
        # Preserve original order by using the original index
        invoice_mask = df[vendor_k3_column] > 0
        credit_note_mask = df[vendor_k3_column] < 0
        zero_mask = df[vendor_k3_column] == 0
        
        invoice_df = df[invoice_mask].copy()
        credit_note_df = df[credit_note_mask].copy()
        zero_df = df[zero_mask].copy()
        
        logger.info(f"Split results: Invoices (>0): {len(invoice_df)} rows, Credit Notes (<0): {len(credit_note_df)} rows, Zero (==0): {len(zero_df)} rows")
        
        # Generate output file paths
        invoice_path = os.path.join(output_directory, f"{base_name}_Invoice{file_ext}")
        credit_note_path = os.path.join(output_directory, f"{base_name}_credit_note{file_ext}")
        zero_path = os.path.join(output_directory, f"{base_name}_zero{file_ext}")
        
        # Save invoice file
        if len(invoice_df) > 0:
            if file_ext == '.csv':
                invoice_df.to_csv(invoice_path, index=False)
            else:
                invoice_df.to_excel(invoice_path, index=False, engine='xlsxwriter')
            logger.info(f"Invoice file saved: {invoice_path} ({len(invoice_df)} rows)")
        else:
            logger.info(f"No invoice records found. Skipping invoice file creation.")
            invoice_path = None
        
        # Save credit note file
        if len(credit_note_df) > 0:
            if file_ext == '.csv':
                credit_note_df.to_csv(credit_note_path, index=False)
            else:
                credit_note_df.to_excel(credit_note_path, index=False, engine='xlsxwriter')
            logger.info(f"Credit note file saved: {credit_note_path} ({len(credit_note_df)} rows)")
        else:
            logger.info(f"No credit note records found. Skipping credit note file creation.")
            credit_note_path = None
        
        # Save zero file
        if len(zero_df) > 0:
            if file_ext == '.csv':
                zero_df.to_csv(zero_path, index=False)
            else:
                zero_df.to_excel(zero_path, index=False, engine='xlsxwriter')
            logger.info(f"Zero file saved: {zero_path} ({len(zero_df)} rows)")
        else:
            logger.info(f"No zero records found. Skipping zero file creation.")
            zero_path = None
        
        return credit_note_path, invoice_path, zero_path
        
    except Exception as e:
        logger.error(f"Error splitting data from {input_file_path}: {e}")
        return None, None, None


def split_dataframe_by_vendor_k3_amount(
    df: pd.DataFrame,
    output_directory: str,
    base_filename: str,
    vendor_k3_column: str = 'Vendor K3 Amount',
    ticket_amount_column: str = 'Ticket_Amount',
    file_extension: str = '.xlsx'
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Split a DataFrame into three files based on Vendor K3 Amount.
    Invoice: Vendor K3 Amount > 0
    Credit Note: Vendor K3 Amount < 0
    Zero: Vendor K3 Amount == 0
    
    Args:
        df: DataFrame to split
        output_directory: Directory to save split files
        base_filename: Base name for output files (without extension)
        vendor_k3_column: Name of the column containing Vendor K3 Amount
        ticket_amount_column: Name of the column containing Ticket_Amount (not used for splitting, kept for compatibility)
        file_extension: File extension for output files
    
    Returns:
        Tuple of (credit_note_file_path, invoice_file_path, zero_file_path) or (None, None, None) if error
    """
    try:
        if df.empty:
            logger.warning("DataFrame is empty, nothing to split")
            return None, None, None
        
        # Check if required columns exist
        if vendor_k3_column not in df.columns:
            logger.error(f"Column '{vendor_k3_column}' not found in DataFrame. Available columns: {list(df.columns)}")
            return None, None, None
        
        # Convert column to numeric, handling any non-numeric values
        df[vendor_k3_column] = pd.to_numeric(df[vendor_k3_column], errors='coerce')
        
        # Split data based on Vendor K3 Amount into three categories
        # Invoice: Vendor K3 Amount > 0
        # Credit Note: Vendor K3 Amount < 0
        # Zero: Vendor K3 Amount == 0
        # Preserve original order by using the original index
        invoice_mask = df[vendor_k3_column] > 0
        credit_note_mask = df[vendor_k3_column] < 0
        zero_mask = df[vendor_k3_column] == 0
        
        invoice_df = df[invoice_mask].copy()
        credit_note_df = df[credit_note_mask].copy()
        zero_df = df[zero_mask].copy()
        
        logger.info(f"Split results: Invoices (>0): {len(invoice_df)} rows, Credit Notes (<0): {len(credit_note_df)} rows, Zero (==0): {len(zero_df)} rows")
        
        # Generate output file paths
        invoice_path = os.path.join(output_directory, f"{base_filename}_Invoice{file_extension}")
        credit_note_path = os.path.join(output_directory, f"{base_filename}_credit_note{file_extension}")
        zero_path = os.path.join(output_directory, f"{base_filename}_zero{file_extension}")
        
        # Save invoice file
        if len(invoice_df) > 0:
            if file_extension == '.csv':
                invoice_df.to_csv(invoice_path, index=False)
            else:
                invoice_df.to_excel(invoice_path, index=False, engine='xlsxwriter')
            logger.info(f"Invoice file saved: {invoice_path} ({len(invoice_df)} rows)")
        else:
            logger.info(f"No invoice records found. Skipping invoice file creation.")
            invoice_path = None
        
        # Save credit note file
        if len(credit_note_df) > 0:
            if file_extension == '.csv':
                credit_note_df.to_csv(credit_note_path, index=False)
            else:
                credit_note_df.to_excel(credit_note_path, index=False, engine='xlsxwriter')
            logger.info(f"Credit note file saved: {credit_note_path} ({len(credit_note_df)} rows)")
        else:
            logger.info(f"No credit note records found. Skipping credit note file creation.")
            credit_note_path = None
        
        # Save zero file
        if len(zero_df) > 0:
            if file_extension == '.csv':
                zero_df.to_csv(zero_path, index=False)
            else:
                zero_df.to_excel(zero_path, index=False, engine='xlsxwriter')
            logger.info(f"Zero file saved: {zero_path} ({len(zero_df)} rows)")
        else:
            logger.info(f"No zero records found. Skipping zero file creation.")
            zero_path = None
        
        return credit_note_path, invoice_path, zero_path
        
    except Exception as e:
        logger.error(f"Error splitting DataFrame: {e}")
        return None, None, None

