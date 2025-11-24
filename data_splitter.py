"""
Data Splitter Module
Splits processed data into two files based on Vendor K3 Amount and Ticket_Amount:
- credit_note: Vendor K3 Amount < 0 OR Ticket_Amount < 0
- Invoice: Vendor K3 Amount >= 0 AND Ticket_Amount >= 0
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
) -> Tuple[Optional[str], Optional[str]]:
    """
    Split data into two files based on Vendor K3 Amount and Ticket_Amount.
    Credit Note: Vendor K3 Amount < 0 OR Ticket_Amount < 0
    Invoice: Vendor K3 Amount >= 0 AND Ticket_Amount >= 0
    
    Args:
        input_file_path: Path to the input file (Excel or CSV)
        output_directory: Directory to save split files. If None, uses same directory as input file.
        vendor_k3_column: Name of the column containing Vendor K3 Amount
        ticket_amount_column: Name of the column containing Ticket_Amount
    
    Returns:
        Tuple of (credit_note_file_path, invoice_file_path) or (None, None) if error
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
            return None, None
        
        # Check if required columns exist
        if vendor_k3_column not in df.columns:
            logger.error(f"Column '{vendor_k3_column}' not found in file. Available columns: {list(df.columns)}")
            return None, None
        
        if ticket_amount_column not in df.columns:
            logger.warning(f"Column '{ticket_amount_column}' not found in file. Will only use {vendor_k3_column} for splitting.")
            use_ticket_amount = False
        else:
            use_ticket_amount = True
        
        # Convert columns to numeric, handling any non-numeric values
        df[vendor_k3_column] = pd.to_numeric(df[vendor_k3_column], errors='coerce')
        if use_ticket_amount:
            df[ticket_amount_column] = pd.to_numeric(df[ticket_amount_column], errors='coerce')
        
        # Split data based on Vendor K3 Amount and Ticket_Amount
        # Credit Note: Vendor K3 Amount < 0 OR Ticket_Amount < 0
        # Invoice: Vendor K3 Amount >= 0 AND Ticket_Amount >= 0
        # Preserve original order by using the original index
        if use_ticket_amount:
            credit_note_mask = (df[vendor_k3_column] < 0) | (df[ticket_amount_column] < 0)
            invoice_mask = (df[vendor_k3_column] >= 0) & (df[ticket_amount_column] >= 0)
        else:
            credit_note_mask = df[vendor_k3_column] < 0
            invoice_mask = df[vendor_k3_column] >= 0
        
        credit_note_df = df[credit_note_mask].copy()
        invoice_df = df[invoice_mask].copy()
        
        logger.info(f"Split results: Credit Notes: {len(credit_note_df)} rows, Invoices: {len(invoice_df)} rows")
        
        # Generate output file paths
        credit_note_path = os.path.join(output_directory, f"{base_name}_credit_note{file_ext}")
        invoice_path = os.path.join(output_directory, f"{base_name}_Invoice{file_ext}")
        
        # Save credit note file
        if len(credit_note_df) > 0:
            if file_ext == '.csv':
                credit_note_df.to_csv(credit_note_path, index=False)
            else:
                credit_note_df.to_excel(credit_note_path, index=False, engine='openpyxl')
            logger.info(f"Credit note file saved: {credit_note_path} ({len(credit_note_df)} rows)")
        else:
            logger.info(f"No credit note records found. Skipping credit note file creation.")
            credit_note_path = None
        
        # Save invoice file
        if len(invoice_df) > 0:
            if file_ext == '.csv':
                invoice_df.to_csv(invoice_path, index=False)
            else:
                invoice_df.to_excel(invoice_path, index=False, engine='openpyxl')
            logger.info(f"Invoice file saved: {invoice_path} ({len(invoice_df)} rows)")
        else:
            logger.info(f"No invoice records found. Skipping invoice file creation.")
            invoice_path = None
        
        return credit_note_path, invoice_path
        
    except Exception as e:
        logger.error(f"Error splitting data from {input_file_path}: {e}")
        return None, None


def split_dataframe_by_vendor_k3_amount(
    df: pd.DataFrame,
    output_directory: str,
    base_filename: str,
    vendor_k3_column: str = 'Vendor K3 Amount',
    ticket_amount_column: str = 'Ticket_Amount',
    file_extension: str = '.xlsx'
) -> Tuple[Optional[str], Optional[str]]:
    """
    Split a DataFrame into two files based on Vendor K3 Amount and Ticket_Amount.
    Credit Note: Vendor K3 Amount < 0 OR Ticket_Amount < 0
    Invoice: Vendor K3 Amount >= 0 AND Ticket_Amount >= 0
    
    Args:
        df: DataFrame to split
        output_directory: Directory to save split files
        base_filename: Base name for output files (without extension)
        vendor_k3_column: Name of the column containing Vendor K3 Amount
        ticket_amount_column: Name of the column containing Ticket_Amount
        file_extension: File extension for output files
    
    Returns:
        Tuple of (credit_note_file_path, invoice_file_path) or (None, None) if error
    """
    try:
        if df.empty:
            logger.warning("DataFrame is empty, nothing to split")
            return None, None
        
        # Check if required columns exist
        if vendor_k3_column not in df.columns:
            logger.error(f"Column '{vendor_k3_column}' not found in DataFrame. Available columns: {list(df.columns)}")
            return None, None
        
        if ticket_amount_column not in df.columns:
            logger.warning(f"Column '{ticket_amount_column}' not found in DataFrame. Will only use {vendor_k3_column} for splitting.")
            use_ticket_amount = False
        else:
            use_ticket_amount = True
        
        # Convert columns to numeric, handling any non-numeric values
        df[vendor_k3_column] = pd.to_numeric(df[vendor_k3_column], errors='coerce')
        if use_ticket_amount:
            df[ticket_amount_column] = pd.to_numeric(df[ticket_amount_column], errors='coerce')
        
        # Split data based on Vendor K3 Amount and Ticket_Amount
        # Credit Note: Vendor K3 Amount < 0 OR Ticket_Amount < 0
        # Invoice: Vendor K3 Amount >= 0 AND Ticket_Amount >= 0
        # Preserve original order by using the original index
        if use_ticket_amount:
            credit_note_mask = (df[vendor_k3_column] < 0) | (df[ticket_amount_column] < 0)
            invoice_mask = (df[vendor_k3_column] >= 0) & (df[ticket_amount_column] >= 0)
        else:
            credit_note_mask = df[vendor_k3_column] < 0
            invoice_mask = df[vendor_k3_column] >= 0
        
        credit_note_df = df[credit_note_mask].copy()
        invoice_df = df[invoice_mask].copy()
        
        logger.info(f"Split results: Credit Notes: {len(credit_note_df)} rows, Invoices: {len(invoice_df)} rows")
        
        # Generate output file paths
        credit_note_path = os.path.join(output_directory, f"{base_filename}_credit_note{file_extension}")
        invoice_path = os.path.join(output_directory, f"{base_filename}_Invoice{file_extension}")
        
        # Save credit note file
        if len(credit_note_df) > 0:
            if file_extension == '.csv':
                credit_note_df.to_csv(credit_note_path, index=False)
            else:
                credit_note_df.to_excel(credit_note_path, index=False, engine='openpyxl')
            logger.info(f"Credit note file saved: {credit_note_path} ({len(credit_note_df)} rows)")
        else:
            logger.info(f"No credit note records found. Skipping credit note file creation.")
            credit_note_path = None
        
        # Save invoice file
        if len(invoice_df) > 0:
            if file_extension == '.csv':
                invoice_df.to_csv(invoice_path, index=False)
            else:
                invoice_df.to_excel(invoice_path, index=False, engine='openpyxl')
            logger.info(f"Invoice file saved: {invoice_path} ({len(invoice_df)} rows)")
        else:
            logger.info(f"No invoice records found. Skipping invoice file creation.")
            invoice_path = None
        
        return credit_note_path, invoice_path
        
    except Exception as e:
        logger.error(f"Error splitting DataFrame: {e}")
        return None, None

