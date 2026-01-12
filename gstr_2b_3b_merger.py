"""
GSTR 2B/3B Filing Status Merger Module

This module merges GSTR 2B and 3B filing status data from external files
into the output file based on invoice number matching.

Logic:
- If Vendor K3 Amount < 0: Merge "Note Number" (invoice_number), "GSTR 2B Filing Status", "GSTR 3B Filing Status"
- If Vendor K3 Amount > 0: Merge "Invoice Number", "GSTR 2B Filing Status", "GSTR 3B Filing Status"
"""

import pandas as pd
import os
import glob
import logging
from typing import Optional, Dict, List, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)


def find_column_case_insensitive(target_name: str, column_list: List[str]) -> Optional[str]:
    """Find a column name in a list using case-insensitive matching."""
    target_lower = target_name.lower().strip()
    for col in column_list:
        if col.lower().strip() == target_lower:
            return col
    return None


def load_gstr_files(gstr_directory: str) -> Optional[pd.DataFrame]:
    """
    Load all GSTR 2B/3B files from the specified directory.
    Handles multiple sheets and multiple files.
    
    Args:
        gstr_directory: Path to directory containing GSTR files
        
    Returns:
        Combined DataFrame with all data from all files and sheets, or None if error
    """
    if not os.path.exists(gstr_directory):
        logger.warning(f"GSTR directory does not exist: {gstr_directory}")
        return None
    
    all_dataframes = []
    supported_extensions = ['.xlsx', '.xls', '.xlsb', '.csv']
    
    try:
        # Find all supported files in the directory
        files_found = []
        for ext in supported_extensions:
            pattern = os.path.join(gstr_directory, f"*{ext}")
            found_files = glob.glob(pattern)
            # Filter out temporary Excel lock files
            found_files = [f for f in found_files if not os.path.basename(f).startswith('~$')]
            files_found.extend(found_files)
        
        if not files_found:
            logger.warning(f"No GSTR files found in {gstr_directory}")
            return None
        
        logger.info(f"Found {len(files_found)} GSTR file(s) to process")
        
        # Process each file
        for file_path in files_found:
            try:
                file_ext = os.path.splitext(file_path)[1].lower()
                
                if file_ext == '.csv':
                    # Read CSV file
                    df = pd.read_csv(file_path)
                    all_dataframes.append(df)
                    logger.info(f"Loaded CSV file: {os.path.basename(file_path)} ({len(df)} rows)")
                else:
                    # Read Excel file (may have multiple sheets)
                    excel_file = pd.ExcelFile(file_path)
                    for sheet_name in excel_file.sheet_names:
                        df = pd.read_excel(file_path, sheet_name=sheet_name)
                        if not df.empty:
                            all_dataframes.append(df)
                            logger.info(f"Loaded sheet '{sheet_name}' from {os.path.basename(file_path)} ({len(df)} rows)")
            
            except Exception as e:
                logger.error(f"Error loading file {file_path}: {e}")
                continue
        
        if not all_dataframes:
            logger.warning("No data loaded from GSTR files")
            return None
        
        # Combine all dataframes
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"Combined GSTR data: {len(combined_df)} total rows from {len(all_dataframes)} source(s)")
        
        return combined_df
    
    except Exception as e:
        logger.error(f"Error loading GSTR files: {e}")
        return None


def merge_gstr_data(output_file_path: str, gstr_directory: str) -> bool:
    """
    Merge GSTR 2B/3B filing status data into the output file.
    
    Args:
        output_file_path: Path to the output file to merge data into
        gstr_directory: Path to directory containing GSTR 2B/3B files
        
    Returns:
        True if merge was successful, False otherwise
    """
    try:
        # Load the output file
        logger.info(f"Loading output file: {output_file_path}")
        output_ext = os.path.splitext(output_file_path)[1].lower()
        
        # Load GSTR files first (needed for all processing)
        df_gstr = load_gstr_files(gstr_directory)
        if df_gstr is None or df_gstr.empty:
            logger.warning("No GSTR data to merge")
            return False
        
        logger.info(f"Loaded GSTR data with {len(df_gstr)} rows")
        logger.info(f"GSTR columns: {list(df_gstr.columns)}")
        
        # Find required columns in GSTR data
        note_number_col = find_column_case_insensitive('Note Number', list(df_gstr.columns))
        invoice_number_col = find_column_case_insensitive('Invoice Number', list(df_gstr.columns))
        gstr_2b_col = find_column_case_insensitive('GSTR 2B Filing Status', list(df_gstr.columns))
        gstr_3b_col = find_column_case_insensitive('GSTR 3B Filing Status', list(df_gstr.columns))
        
        # Validate required columns exist
        if not gstr_2b_col or not gstr_3b_col:
            logger.error("Required GSTR columns not found. Need: 'GSTR 2B Filing Status' and 'GSTR 3B Filing Status'")
            return False
        
        if not note_number_col and not invoice_number_col:
            logger.error("Neither 'Note Number' nor 'Invoice Number' found in GSTR data")
            return False
        
        # Helper function to normalize invoice numbers for matching
        def normalize_invoice_number(value) -> str:
            """Normalize invoice number by converting to string, stripping whitespace, quotes, and handling NaN/None."""
            if pd.isna(value) or value is None:
                return ''
            # Convert to string and normalize
            normalized = str(value).strip()
            # Remove leading/trailing quotes (single or double quotes)
            normalized = normalized.strip("'\"")
            # Remove all whitespace characters (spaces, tabs, newlines)
            normalized = ''.join(normalized.split())
            # Convert to uppercase for case-insensitive matching
            normalized = normalized.upper()
            return normalized
        
        # Prepare GSTR data for merging
        # Create separate mappings for Note Number (credit notes) and Invoice Number (invoices)
        gstr_note_mapping = {}  # For Vendor K3 Amount < 0
        gstr_invoice_mapping = {}  # For Vendor K3 Amount > 0
        
        # Process Note Number column (for credit notes - Vendor K3 Amount < 0)
        if note_number_col:
            for idx, row in df_gstr.iterrows():
                note_num = row[note_number_col]
                note_num_normalized = normalize_invoice_number(note_num)
                if note_num_normalized:
                    if note_num_normalized not in gstr_note_mapping:
                        gstr_note_mapping[note_num_normalized] = {
                            'GSTR_2B_Filing_Status': str(row[gstr_2b_col]).strip() if pd.notna(row[gstr_2b_col]) else '',
                            'GSTR_3B_Filing_Status': str(row[gstr_3b_col]).strip() if pd.notna(row[gstr_3b_col]) else ''
                        }
        
        # Process Invoice Number column (for invoices - Vendor K3 Amount > 0)
        if invoice_number_col:
            for idx, row in df_gstr.iterrows():
                inv_num = row[invoice_number_col]
                inv_num_normalized = normalize_invoice_number(inv_num)
                if inv_num_normalized:
                    if inv_num_normalized not in gstr_invoice_mapping:
                        gstr_invoice_mapping[inv_num_normalized] = {
                            'GSTR_2B_Filing_Status': str(row[gstr_2b_col]).strip() if pd.notna(row[gstr_2b_col]) else '',
                            'GSTR_3B_Filing_Status': str(row[gstr_3b_col]).strip() if pd.notna(row[gstr_3b_col]) else ''
                        }
        
        logger.info(f"Created Note Number mapping for {len(gstr_note_mapping)} entries (for Vendor K3 Amount < 0)")
        logger.info(f"Created Invoice Number mapping for {len(gstr_invoice_mapping)} entries (for Vendor K3 Amount > 0)")
        
        if gstr_invoice_mapping:
            sample_invoices = list(gstr_invoice_mapping.keys())[:5]
            logger.debug(f"Sample Invoice Numbers in GSTR mapping: {sample_invoices}")
        
        if gstr_note_mapping:
            sample_notes = list(gstr_note_mapping.keys())[:5]
            logger.debug(f"Sample Note Numbers in GSTR mapping: {sample_notes}")
        
        # Helper function to merge GSTR data into a DataFrame
        def merge_gstr_into_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
            """Merge GSTR data into a single DataFrame. Returns (merged_df, merged_count)."""
            # Check if required columns exist
            vendor_k3_col = find_column_case_insensitive('Vendor K3 Amount', list(df.columns))
            if vendor_k3_col is None:
                logger.error("'Vendor K3 Amount' column not found in output file")
                return df, 0
            
            # Initialize new columns
            df = df.copy()
            df['GSTR_2B_Filing_Status'] = ''
            df['GSTR_3B_Filing_Status'] = ''
            
            # Find invoice number columns
            invoice_num_col = find_column_case_insensitive('Invoice_Number', list(df.columns))
            if invoice_num_col is None:
                invoice_num_col = find_column_case_insensitive('Invoice Number', list(df.columns))
            
            if invoice_num_col is None:
                logger.error("Invoice number column not found in output file")
                logger.error(f"Available columns in output file: {list(df.columns)}")
                return df, 0
            
            logger.info(f"Using invoice number column: '{invoice_num_col}'")
            
            # Merge data based on Vendor K3 Amount
            merged_count = 0
            not_found_count = 0
            sample_not_found = []  # For debugging
            
            for idx, row in df.iterrows():
                vendor_k3_value = row[vendor_k3_col]
                invoice_num = row[invoice_num_col]
                
                # Normalize invoice number
                invoice_num_normalized = normalize_invoice_number(invoice_num)
                
                # Skip if invoice number is empty
                if not invoice_num_normalized:
                    continue
                
                # Determine Vendor K3 Amount value
                try:
                    vendor_k3_float = float(vendor_k3_value) if pd.notna(vendor_k3_value) else 0
                except (ValueError, TypeError):
                    vendor_k3_float = 0
                
                matched = False
                
                if vendor_k3_float < 0:
                    # For credit notes (< 0): Match using Note Number from GSTR file
                    if invoice_num_normalized in gstr_note_mapping:
                        gstr_data = gstr_note_mapping[invoice_num_normalized]
                        df.at[idx, 'GSTR_2B_Filing_Status'] = gstr_data['GSTR_2B_Filing_Status']
                        df.at[idx, 'GSTR_3B_Filing_Status'] = gstr_data['GSTR_3B_Filing_Status']
                        merged_count += 1
                        matched = True
                
                elif vendor_k3_float > 0:
                    # For invoices (> 0): Match using Invoice Number from GSTR file
                    if invoice_num_normalized in gstr_invoice_mapping:
                        gstr_data = gstr_invoice_mapping[invoice_num_normalized]
                        df.at[idx, 'GSTR_2B_Filing_Status'] = gstr_data['GSTR_2B_Filing_Status']
                        df.at[idx, 'GSTR_3B_Filing_Status'] = gstr_data['GSTR_3B_Filing_Status']
                        merged_count += 1
                        matched = True
                
                # Track unmatched invoice numbers for debugging
                if not matched and vendor_k3_float != 0:
                    not_found_count += 1
                    if len(sample_not_found) < 10:
                        sample_not_found.append({
                            'invoice': invoice_num_normalized,
                            'vendor_k3': vendor_k3_float,
                            'type': 'credit_note' if vendor_k3_float < 0 else 'invoice'
                        })
            
            if sample_not_found:
                logger.debug(f"Sample unmatched invoice numbers (first 10): {sample_not_found}")
            if not_found_count > 0:
                logger.warning(f"Total unmatched invoice numbers: {not_found_count}")
            logger.info(f"Successfully merged GSTR data for {merged_count} rows")
            
            return df, merged_count
        
        # Process the output file
        if output_ext == '.csv':
            df_output = pd.read_csv(output_file_path)
            logger.info(f"Loaded output file with {len(df_output)} rows")
            df_output, merged_count = merge_gstr_into_dataframe(df_output)
            logger.info(f"Merged GSTR data for {merged_count} rows")
            df_output.to_csv(output_file_path, index=False)
        else:
            # For Excel, process each sheet separately
            excel_file = pd.ExcelFile(output_file_path)
            output_sheets = {}
            total_merged = 0
            
            for sheet_name in excel_file.sheet_names:
                df_sheet = pd.read_excel(output_file_path, sheet_name=sheet_name)
                if not df_sheet.empty:
                    logger.info(f"Processing sheet '{sheet_name}' with {len(df_sheet)} rows")
                    df_merged, merged_count = merge_gstr_into_dataframe(df_sheet)
                    output_sheets[sheet_name] = df_merged
                    total_merged += merged_count
                    logger.info(f"Merged GSTR data for {merged_count} rows in sheet '{sheet_name}'")
            
            if not output_sheets:
                logger.error("No data found in output file")
                return False
            
            # Save all sheets back
            with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
                for sheet_name, df_sheet in output_sheets.items():
                    df_sheet.to_excel(writer, sheet_name=sheet_name, index=False)
            
            logger.info(f"Total merged GSTR data for {total_merged} rows across all sheets")
        
        logger.info(f"Successfully merged GSTR data and saved to: {output_file_path}")
        return True
    
    except Exception as e:
        logger.error(f"Error merging GSTR data: {e}", exc_info=True)
        return False

