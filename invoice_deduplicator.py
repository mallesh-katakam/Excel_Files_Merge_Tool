"""
Invoice Deduplicator Module
Processes files to ensure unique Invoice_Number values:
- Identifies duplicate Invoice_Number values
- Marks duplicate rows with 'Need_Manual_Verification' = 'Yes'
- Removes Invoice_Number from duplicate rows (keeps first occurrence)
Preserves the original data order.
"""

import pandas as pd
import os
import logging
from typing import Optional, List
from pathlib import Path

logger = logging.getLogger(__name__)


def process_duplicate_invoice_numbers(
    input_file_path: str,
    output_file_path: Optional[str] = None,
    invoice_number_column: str = 'Invoice_Number'
) -> Optional[str]:
    """
    Process a file to ensure unique Invoice_Number values.
    Duplicate invoice numbers are marked for manual verification and removed.
    
    Args:
        input_file_path: Path to the input file (Excel or CSV)
        output_file_path: Path to save processed file. If None, overwrites input file.
        invoice_number_column: Name of the column containing Invoice_Number
    
    Returns:
        Path to the processed file or None if error
    """
    try:
        # Determine output path
        if output_file_path is None:
            output_file_path = input_file_path
        
        # Get file extension
        file_ext = Path(input_file_path).suffix.lower()
        
        # Read the input file
        logger.info(f"Reading file for invoice deduplication: {input_file_path}")
        
        if file_ext == '.csv':
            df = pd.read_csv(input_file_path)
        elif file_ext in ['.xlsx', '.xls']:
            # Read first sheet if multiple sheets exist
            df = pd.read_excel(input_file_path, sheet_name=0)
        else:
            logger.error(f"Unsupported file format: {file_ext}")
            return None
        
        if df.empty:
            logger.warning(f"Input file is empty: {input_file_path}")
            return None
        
        # Check if Invoice_Number column exists
        if invoice_number_column not in df.columns:
            logger.warning(f"Column '{invoice_number_column}' not found in file. Available columns: {list(df.columns)}")
            # If column doesn't exist, just return the file path without processing
            return output_file_path
        
        # Initialize Need_Manual_Verification column if it doesn't exist
        if 'Need_Manual_Verification' not in df.columns:
            df['Need_Manual_Verification'] = 'No'
        
        # Convert Invoice_Number to string, handling NaN/None values properly
        # First, replace NaN/None with empty string, then convert to string
        df[invoice_number_column] = df[invoice_number_column].fillna('').astype(str)
        # Replace string representations of empty/None values
        df[invoice_number_column] = df[invoice_number_column].replace(['nan', 'None', 'NaT', '<NA>'], '')
        
        # Find duplicate invoice numbers (excluding empty values)
        # Keep first occurrence, mark others as duplicates
        seen_invoice_numbers = set()
        duplicate_count = 0
        
        for idx, row in df.iterrows():
            invoice_num = row[invoice_number_column]
            
            # Skip empty/None values
            if pd.isna(invoice_num) or invoice_num is None or str(invoice_num).strip() == '':
                continue
            
            invoice_num_str = str(invoice_num).strip()
            
            # Check if this invoice number was seen before
            if invoice_num_str in seen_invoice_numbers:
                # This is a duplicate - mark for manual verification and remove invoice number
                df.at[idx, 'Need_Manual_Verification'] = 'Yes'
                df.at[idx, invoice_number_column] = ''  # Set to empty string
                duplicate_count += 1
                logger.debug(f"Found duplicate Invoice_Number '{invoice_num_str}' at row {idx}, marked for manual verification")
            else:
                # First occurrence - keep it
                seen_invoice_numbers.add(invoice_num_str)
        
        logger.info(f"Found {duplicate_count} duplicate Invoice_Number(s). Marked for manual verification and removed.")
        
        # Save the processed file
        if file_ext == '.csv':
            df.to_csv(output_file_path, index=False)
        else:
            df.to_excel(output_file_path, index=False, engine='xlsxwriter')
        
        logger.info(f"Processed file saved: {output_file_path}")
        return output_file_path
        
    except Exception as e:
        logger.error(f"Error processing duplicate invoice numbers from {input_file_path}: {e}")
        return None


def process_dataframe_duplicate_invoice_numbers(
    df: pd.DataFrame,
    invoice_number_column: str = 'Invoice_Number'
) -> pd.DataFrame:
    """
    Process a DataFrame to ensure unique Invoice_Number values.
    Duplicate invoice numbers are marked for manual verification and removed.
    
    Args:
        df: DataFrame to process
        invoice_number_column: Name of the column containing Invoice_Number
    
    Returns:
        Processed DataFrame with unique Invoice_Number values
    """
    try:
        if df.empty:
            logger.warning("DataFrame is empty, nothing to process")
            return df
        
        # Check if Invoice_Number column exists
        if invoice_number_column not in df.columns:
            logger.warning(f"Column '{invoice_number_column}' not found in DataFrame. Available columns: {list(df.columns)}")
            # If column doesn't exist, return DataFrame as-is
            return df
        
        # Create a copy to avoid modifying original
        df_processed = df.copy()
        
        # Initialize Need_Manual_Verification column if it doesn't exist
        if 'Need_Manual_Verification' not in df_processed.columns:
            df_processed['Need_Manual_Verification'] = 'No'
        
        # Convert Invoice_Number to string, handling NaN/None values properly
        # First, replace NaN/None with empty string, then convert to string
        df_processed[invoice_number_column] = df_processed[invoice_number_column].fillna('').astype(str)
        # Replace string representations of empty/None values
        df_processed[invoice_number_column] = df_processed[invoice_number_column].replace(['nan', 'None', 'NaT', '<NA>'], '')
        
        # Find duplicate invoice numbers (excluding empty values)
        # Keep first occurrence, mark others as duplicates
        seen_invoice_numbers = set()
        duplicate_count = 0
        
        for idx, row in df_processed.iterrows():
            invoice_num = row[invoice_number_column]
            
            # Skip empty/None values
            if pd.isna(invoice_num) or invoice_num is None or str(invoice_num).strip() == '':
                continue
            
            invoice_num_str = str(invoice_num).strip()
            
            # Check if this invoice number was seen before
            if invoice_num_str in seen_invoice_numbers:
                # This is a duplicate - mark for manual verification and remove invoice number
                df_processed.at[idx, 'Need_Manual_Verification'] = 'Yes'
                df_processed.at[idx, invoice_number_column] = ''  # Set to empty string
                duplicate_count += 1
                logger.debug(f"Found duplicate Invoice_Number '{invoice_num_str}' at row {idx}, marked for manual verification")
            else:
                # First occurrence - keep it
                seen_invoice_numbers.add(invoice_num_str)
        
        logger.info(f"Found {duplicate_count} duplicate Invoice_Number(s). Marked for manual verification and removed.")
        
        return df_processed
        
    except Exception as e:
        logger.error(f"Error processing duplicate invoice numbers in DataFrame: {e}")
        return df


def process_multiple_files(
    file_paths: List[Optional[str]],
    invoice_number_column: str = 'Invoice_Number'
) -> List[Optional[str]]:
    """
    Process multiple files to ensure unique Invoice_Number values in each.
    
    Args:
        file_paths: List of file paths to process (None values are skipped)
        invoice_number_column: Name of the column containing Invoice_Number
    
    Returns:
        List of processed file paths (or None if error)
    """
    processed_paths = []
    
    for file_path in file_paths:
        if file_path is None:
            processed_paths.append(None)
            continue
        
        processed_path = process_duplicate_invoice_numbers(
            file_path,
            output_file_path=None,  # Overwrite original file
            invoice_number_column=invoice_number_column
        )
        processed_paths.append(processed_path)
    
    return processed_paths

