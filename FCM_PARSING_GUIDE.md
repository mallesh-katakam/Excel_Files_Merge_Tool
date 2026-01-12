# FCM File Parsing Guide

## Overview

The system now includes special parsing logic for **FCM (Flight Centre Management)** files, which use a different format for sector and PNR/ticket number representation.

## What is FCM File Detection?

The system automatically detects FCM files by checking if the filename contains "**fcm**" (case-insensitive).

**Examples of FCM files:**
- `fcm_report.xlsx`
- `FCM_Data.csv`
- `my_fcm_file.xlsx`
- `data_FCM_2024.csv`

## FCM-Specific Parsing Rules

### 1. Sector Parsing for FCM Files

**Standard Format (Non-FCM):**
- Sectors are separated by `/`
- Example: `MAA-HYD/HYD-MAA` → `['MAA-HYD', 'HYD-MAA']`

**FCM Format:**
- Sectors are separated by **single alphabetic characters** (like `T`, `U`, `X`, etc.)
- The single character acts as a delimiter between flight segments
- Trailing single characters at the end are ignored

#### FCM Sector Examples:

1. **`PHL-DOH-T-DOH-BOM-T`**
   - Output: `['PHL-DOH', 'DOH-BOM']`
   - Explanation: Split at `-T-`, ignoring the trailing `-T`

2. **`MAA-BOM-U-BOM-SFO-U`**
   - Output: `['MAA-BOM', 'BOM-SFO']`
   - Explanation: Split at `-U-`, ignoring the trailing `-U`

3. **`DEL-BOM-X-BOM-LHR-X-LHR-JFK-X`**
   - Output: `['DEL-BOM', 'BOM-LHR', 'LHR-JFK']`
   - Explanation: Multiple segments split by `-X-`

### 2. PNR/Ticket Number Parsing for FCM Files

**Standard Format (Non-FCM):**
- PNR and Ticket numbers are used as-is
- Example: `Q7W2QG-1/1` → `Q7W2QG-1/1` (no change)

**FCM Format:**
- If a hyphen (`-`) exists in the value, only the part **before the first hyphen** is used
- Everything after the first hyphen is discarded

#### FCM PNR/Ticket Examples:

1. **`Q7W2QG-1/1`**
   - Output: `Q7W2QG`
   - Explanation: Remove `-1/1` suffix

2. **`ABC123-2/3`**
   - Output: `ABC123`
   - Explanation: Remove `-2/3` suffix

3. **`XYZ789`**
   - Output: `XYZ789`
   - Explanation: No hyphen, so no change

4. **`TEST-CASE-MULTIPLE`**
   - Output: `TEST`
   - Explanation: Remove everything after the first hyphen

## File Structure

### New Files Added:

1. **`fcm_parser.py`**
   - Contains all FCM-specific parsing logic
   - Includes helper functions:
     - `is_fcm_file(filename)` - Detects if a file is FCM format
     - `parse_fcm_sector(sector)` - Parses FCM sector format
     - `parse_fcm_pnr_or_ticket(value)` - Cleans PNR/ticket numbers for FCM files
   - Can be run standalone to test functionality: `python fcm_parser.py`

2. **`test_fcm_integration.py`**
   - Integration test script
   - Verifies that FCM parsing is correctly integrated with `data_merge.py`
   - Run with: `python test_fcm_integration.py`

### Modified Files:

1. **`data_merge.py`**
   - Imported `fcm_parser` module
   - Modified `DataEnricher` class:
     - Added `current_file_path` attribute to track the file being processed
     - Added `normalize_pnr_ticket_value()` method for PNR/ticket normalization
     - Modified `split_multi_sector()` to use FCM parsing when appropriate
     - Modified key building logic to normalize PNR/ticket values for FCM files

## How It Works

### Automatic Detection Flow:

```
1. File is loaded for processing
   ↓
2. Filename is checked for "fcm" (case-insensitive)
   ↓
3a. IF FCM file detected:
    - Use FCM sector parsing (single letter delimiters)
    - Clean PNR/ticket numbers (remove suffix after "-")
   ↓
3b. IF NOT FCM file:
    - Use standard sector parsing ("/" delimiters)
    - Use PNR/ticket numbers as-is
   ↓
4. Continue with normal processing and database matching
```

## Testing

### Run Standalone FCM Parser Tests:
```bash
python fcm_parser.py
```

This will test:
- Sector parsing with various FCM formats
- PNR/ticket number cleaning
- File detection logic

### Run Integration Tests:
```bash
python test_fcm_integration.py
```

This will verify:
- FCM file detection in DataEnricher
- FCM sector splitting integration
- Standard sector splitting still works
- FCM PNR/ticket normalization
- Standard PNR/ticket handling still works

## Usage Examples

### Example 1: Processing an FCM File

**File:** `fcm_report_2024.xlsx`

**Data:**
```
PNR Number      | Ticket Number | Sector
Q7W2QG-1/1      | TKT001-2/1   | PHL-DOH-T-DOH-BOM-T
ABC123-1/2      | TKT002-3/1   | MAA-BOM-U-BOM-SFO-U
```

**System Processing:**
- File detected as FCM (contains "fcm")
- PNR `Q7W2QG-1/1` → cleaned to `Q7W2QG`
- Ticket `TKT001-2/1` → cleaned to `TKT001`
- Sector `PHL-DOH-T-DOH-BOM-T` → split to `['PHL-DOH', 'DOH-BOM']`
- Database queries executed with cleaned values

### Example 2: Processing a Regular File

**File:** `regular_report_2024.xlsx`

**Data:**
```
PNR Number      | Ticket Number | Sector
Q7W2QG-1/1      | TKT001-2/1   | MAA-HYD/HYD-MAA
ABC123-1/2      | TKT002-3/1   | BOM-DEL/DEL-BOM
```

**System Processing:**
- File NOT detected as FCM (doesn't contain "fcm")
- PNR `Q7W2QG-1/1` → used as-is: `Q7W2QG-1/1`
- Ticket `TKT001-2/1` → used as-is: `TKT001-2/1`
- Sector `MAA-HYD/HYD-MAA` → split by `/` to `['MAA-HYD', 'HYD-MAA']`
- Database queries executed with original values

## Benefits

✅ **Automatic Detection**: No manual configuration needed - system detects FCM files automatically

✅ **Backward Compatible**: Regular files continue to work exactly as before

✅ **No Code Duplication**: Parsing logic centralized in separate module

✅ **Easy Testing**: Standalone test scripts for verification

✅ **Flexible**: Easy to add more parsing rules or file type detections in the future

## Technical Notes

- FCM detection is **case-insensitive** (FCM, fcm, Fcm all work)
- FCM parsing applies to **both PNR and Ticket Number** columns
- Sector parsing uses the same comprehensive combination logic (individual sectors, pairs, first-last patterns)
- Empty or null values are handled gracefully in both FCM and standard modes

## Troubleshooting

### Issue: FCM file not being detected
**Solution:** Ensure the filename contains "fcm" somewhere in it (case-insensitive)

### Issue: Sectors not parsing correctly
**Solution:** 
- Verify the sector format matches FCM format (e.g., `PHL-DOH-T-DOH-BOM-T`)
- Check that single letter delimiters are present
- Run `python fcm_parser.py` to test parsing logic

### Issue: PNR/Ticket numbers not being cleaned
**Solution:**
- Verify the file is detected as FCM
- Check that values contain hyphens (if no hyphen, no cleaning occurs)
- Test with `python test_fcm_integration.py`

## Future Enhancements

Possible future improvements:
- Support for other travel management company formats
- Configuration file to define custom parsing rules
- Support for multiple delimiter types in a single file
- Logging of FCM detection and parsing actions for audit trails

