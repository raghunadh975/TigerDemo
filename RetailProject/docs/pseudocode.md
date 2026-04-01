# Bronze Layer Load - Pseudocode Documentation

## Overview

This notebook implements a medallion architecture Bronze layer ETL process that loads data from the **raw** layer to the **bronze** layer with two load strategies:

- **SCD2 (Slowly Changing Dimension Type 2)**: Tracks historical changes for dimension tables
- **APPEND**: Simple append for fact tables

Both strategies support **full** or **incremental** load types.

---

## Architecture Components

```
┌─────────────────────────────────────────────────────────────┐
│                    Configuration Layer                       │
│  bronze_config.json: table_name, primary_keys,              │
│                      load_strategy, load_type               │
└─────────────────────────────────────────────────────────────┘
                            |
                            v
┌─────────────────────────────────────────────────────────────┐
│                      Control Layer                           │
│  load_control table: tracks watermarks for incremental      │
│                      (table_name, wm_col, last_loaded_wm)   │
└─────────────────────────────────────────────────────────────┘
                            |
                            v
┌─────────────────────────────────────────────────────────────┐
│                      Data Sources                            │
│  FULL:        retail.raw.{table_name}                       │
│  INCREMENTAL: retail.raw.{table_name}_vw                    │
│               (filtered by watermark)                        │
└─────────────────────────────────────────────────────────────┘
                            |
                            v
┌─────────────────────────────────────────────────────────────┐
│                    Load Functions                            │
│  load_scd2():   Dimension tables with history tracking      │
│  load_append(): Fact tables with simple append              │
└─────────────────────────────────────────────────────────────┘
                            |
                            v
┌─────────────────────────────────────────────────────────────┐
│                      Target Layer                            │
│  retail.bronze.{table_name}                                 │
│  With audit columns: created_at, updated_at                 │
│  With SCD2 columns: start_date, end_date, is_current        │
└─────────────────────────────────────────────────────────────┘
```

---

## Configuration Structure

```python
# bronze_config.json
[
  {
    "table_name": "customers",
    "primary_keys": ["customer_id"],
    "load_strategy": "SCD2",
    "load_type": "incremental",  # or "full"
    "catalog_name": "retail",
    "source_schema": "raw",
    "target_schema": "bronze"
  },
  {
    "table_name": "orders",
    "primary_keys": ["order_id"],
    "load_strategy": "APPEND",
    "load_type": "full",
    "catalog_name": "retail",
    "source_schema": "raw",
    "target_schema": "bronze"
  }
]
```

---

## Function Pseudocode

### 1. get_incremental_records(full_nm)

**Purpose**: Creates a filtered view that returns only records newer than the last loaded watermark.

```
FUNCTION get_incremental_records(full_nm):
    // full_nm example: "retail.raw.customers"
    
    view_name = full_nm + "_vw"
    
    // Drop existing view
    DROP VIEW IF EXISTS view_name
    
    // Retrieve watermark metadata from control table
    QUERY control_table WHERE table_name = full_nm
    GET wm_col, last_loaded_wm
    
    // Create filtered view
    CREATE VIEW view_name AS
        SELECT * FROM full_nm
        WHERE wm_col > last_loaded_wm
    
    PRINT "Incremental view created: " + view_name
END FUNCTION
```

**Example**:
- Input: `"retail.raw.customers"`
- Creates: `retail.raw.customers_vw`
- Filter: `WHERE created_timestamp > '2026-03-30 10:00:00'`

---

### 2. update_control_table(source_tbl_nm)

**Purpose**: Updates the watermark in the control table after successful data load.

```
FUNCTION update_control_table(source_tbl_nm):
    // Get watermark column name
    QUERY control_table WHERE table_name = source_tbl_nm
    GET wm_col
    
    // Get maximum timestamp from source table
    max_timestamp = SELECT MAX(wm_col) FROM source_tbl_nm
    
    // Update control table
    UPDATE control_table
    SET last_loaded_wm = max_timestamp,
        updated_at = current_timestamp()
    WHERE table_name = source_tbl_nm
    
    PRINT "Updated watermark to: " + max_timestamp
END FUNCTION
```

**Example**:
- Before: `last_loaded_wm = '2026-03-30 10:00:00'`
- After: `last_loaded_wm = '2026-03-31 14:23:45'`

---

### 3. load_scd2(source_table, target_table, primary_keys, catalog_name, source_schema, target_schema, load_type)

**Purpose**: Implements SCD2 logic to track historical changes in dimension tables.

```
FUNCTION load_scd2(parameters):
    // Step 1: Determine source based on load_type
    full_nm = catalog_name.source_schema.source_table
    
    IF load_type == "incremental":
        CALL get_incremental_records(full_nm)
        source_tbl_nm = full_nm + "_vw"
    ELSE:
        source_tbl_nm = full_nm
    
    target_tbl_nm = catalog_name.target_schema.target_table
    
    // Step 2: Read and clean source data
    source_df = READ TABLE source_tbl_nm
    source_df = source_df
        .FILTER primary_keys ARE NOT NULL
        .ORDER BY all columns ASC NULLS LAST
        .DROP DUPLICATES on primary_keys
        .FILL NULL values with "N/A"
    
    source_count = COUNT source_df
    
    IF source_count == 0:
        PRINT "No new records to process"
        RETURN {status: "success", operation: "no_new_data", records: 0}
    
    // Step 3: Check if target table exists
    IF target_table DOES NOT EXIST:
        // Initial Load
        bronze_df = source_df
            .ADD COLUMN created_at = current_timestamp()
            .ADD COLUMN updated_at = current_timestamp()
            .ADD COLUMN start_date = current_timestamp()
            .ADD COLUMN end_date = NULL
            .ADD COLUMN is_current = TRUE
        
        WRITE bronze_df TO target_tbl_nm MODE overwrite
        
        IF load_type == "incremental":
            CALL update_control_table(full_nm)
        
        RETURN {status: "success", operation: "initial_load", records: count}
    
    ELSE:
        // Incremental Load - SCD2 Logic
        
        // Step 4: Read current records from target
        target_df = READ TABLE target_tbl_nm WHERE is_current = TRUE
        
        // Step 5: Calculate hash for change detection
        audit_columns = ['create_timestamp', 'created_at', 'updated_at', 
                        'start_date', 'end_date', 'is_current']
        source_cols = source_df.columns EXCLUDING audit_columns
        
        source_with_hash = source_df
            .ADD COLUMN row_hash = MD5(CONCAT all source_cols)
        
        target_with_hash = target_df
            .ADD COLUMN row_hash = MD5(CONCAT all source_cols)
        
        // Step 6: Identify changed records
        pk_join_condition = JOIN ON all primary_keys
        
        changed_records = source_with_hash INNER JOIN target_with_hash
            ON pk_join_condition
            WHERE source.row_hash != target.row_hash
            SELECT source columns
        
        // Step 7: Identify new records
        new_records = source_with_hash LEFT ANTI JOIN target_with_hash
            ON pk_join_condition
            SELECT source columns
        
        changed_count = COUNT changed_records
        new_count = COUNT new_records
        
        // Step 8: Process changed records (SCD2)
        IF changed_count > 0:
            // Expire old versions
            updates_df = changed_records.SELECT primary_keys
            
            DELTA MERGE INTO target_tbl_nm AS target
            USING updates_df AS updates
            ON primary_keys match
            WHEN MATCHED AND target.is_current = TRUE THEN
                UPDATE SET
                    is_current = FALSE,
                    end_date = current_timestamp(),
                    updated_at = current_timestamp()
            
            // Insert new versions
            new_versions = changed_records
                .ADD COLUMN created_at = current_timestamp()
                .ADD COLUMN updated_at = current_timestamp()
                .ADD COLUMN start_date = current_timestamp()
                .ADD COLUMN end_date = NULL
                .ADD COLUMN is_current = TRUE
            
            WRITE new_versions TO target_tbl_nm MODE append
        
        // Step 9: Process new records
        IF new_count > 0:
            new_bronze = new_records
                .ADD COLUMN created_at = current_timestamp()
                .ADD COLUMN updated_at = current_timestamp()
                .ADD COLUMN start_date = current_timestamp()
                .ADD COLUMN end_date = NULL
                .ADD COLUMN is_current = TRUE
            
            WRITE new_bronze TO target_tbl_nm MODE append
        
        // Step 10: Update watermark
        IF load_type == "incremental":
            CALL update_control_table(full_nm)
        
        RETURN {
            status: "success",
            operation: "incremental_load",
            new_records: new_count,
            changed_records: changed_count
        }
    
END FUNCTION
```

---

### 4. load_append(source_table, target_table, primary_keys, catalog_name, source_schema, target_schema)

**Purpose**: Simple append strategy for fact tables that don't require history tracking.

```
FUNCTION load_append(parameters):
    // Build fully qualified table names
    source_table_name = catalog_name.source_schema.source_table
    target_table_name = catalog_name.target_schema.target_table
    
    // Read source data
    source_df = READ TABLE source_table_name
    
    // Add audit columns
    bronze_df = source_df
        .ADD COLUMN created_at = current_timestamp()
        .ADD COLUMN updated_at = current_timestamp()
    
    // Append to target
    WRITE bronze_df TO target_table_name MODE append
    
    RETURN {
        status: "success",
        operation: "append",
        new_records: COUNT source_df
    }
END FUNCTION
```

---

### 5. run() - Main Orchestration Function

**Purpose**: Orchestrates the entire Bronze layer load process for all configured tables.

```
FUNCTION run():
    PRINT "Bronze Layer Load Started"
    
    results = []
    control_table_name = "retail.bronze.load_control"
    
    // Process each table from configuration
    FOR EACH config IN table_config:
        table_name = config.table_name
        primary_keys = config.primary_keys
        load_strategy = config.load_strategy
        load_type = config.load_type (default: "full")
        catalog_name = config.catalog_name
        source_schema = config.source_schema
        target_schema = config.target_schema
        
        PRINT "Processing: " + table_name
        PRINT "Strategy: " + load_strategy
        PRINT "Load Type: " + load_type
        
        TRY:
            // Route to appropriate load function
            IF load_strategy == "SCD2":
                result = CALL load_scd2(
                    source_table = table_name,
                    target_table = table_name,
                    primary_keys = primary_keys,
                    catalog_name = catalog_name,
                    source_schema = source_schema,
                    target_schema = target_schema,
                    load_type = load_type
                )
            
            ELSE IF load_strategy == "APPEND":
                result = CALL load_append(
                    source_table = table_name,
                    target_table = table_name,
                    primary_keys = primary_keys,
                    catalog_name = catalog_name,
                    source_schema = source_schema,
                    target_schema = target_schema
                )
            
            ELSE:
                result = {status: "failed", error: "Unknown strategy"}
            
            // Print result summary
            IF result.status == "success":
                IF result.operation == "initial_load":
                    PRINT "SUCCESS - Initial load: " + result.records
                ELSE IF result.operation == "incremental_load":
                    PRINT "SUCCESS - New: " + result.new_records + 
                          ", Changed: " + result.changed_records
                ELSE IF result.operation == "no_new_data":
                    PRINT "SUCCESS - No new data to process"
                ELSE:
                    PRINT "SUCCESS - Appended: " + result.new_records
            ELSE:
                PRINT "FAILED - " + result.error
            
            ADD result TO results
        
        CATCH Exception e:
            PRINT "FAILED - " + e.message
            ADD {status: "failed", error: e.message} TO results
    
    PRINT "Bronze Layer Load Completed"
END FUNCTION
```

---

## Execution Flow

### Cell Execution Order

```
1. Cell 1: Documentation (Markdown)
   ↓
2. Cell 2: Control Table Initialization (Commented out)
   ↓
3. Cell 3: Load Configuration
   - Reads bronze_config.json
   - Stores configuration in table_config variable
   ↓
4. Cell 4: Define get_incremental_records() function
   ↓
5. Cell 5: Define update_control_table() function
   ↓
6. Cell 6: Define load_scd2() function
   ↓
7. Cell 7: Define load_append() function
   ↓
8. Cell 8: Documentation (Markdown)
   ↓
9. Cell 9: Define run() orchestration function
   ↓
10. Cell 10: Documentation (Markdown)
   ↓
11. Cell 11: Execute run()
    - Processes all tables from configuration
    - Calls load_scd2() or load_append() for each table
    - Prints results
```

---

## SCD2 Logic Deep Dive

### Scenario: Customer Record Change

**Initial State (Bronze Table)**:
```
customer_id | name      | email           | start_date | end_date | is_current
------------|-----------|-----------------|------------|----------|------------
1001        | John Doe  | john@email.com  | 2026-01-01 | NULL     | TRUE
```

**Raw Layer Update**:
```
customer_id | name      | email              | created_timestamp
------------|-----------|--------------------|-----------------
1001        | John Doe  | john.new@email.com | 2026-03-31
```

**SCD2 Processing**:

1. **Hash Calculation**:
   - Old hash: `MD5("1001|John Doe|john@email.com")`
   - New hash: `MD5("1001|John Doe|john.new@email.com")`
   - Hashes differ → Change detected

2. **Expire Old Record**:
   ```sql
   UPDATE bronze.customers
   SET is_current = FALSE,
       end_date = '2026-03-31 14:30:00',
       updated_at = '2026-03-31 14:30:00'
   WHERE customer_id = 1001 AND is_current = TRUE
   ```

3. **Insert New Version**:
   ```sql
   INSERT INTO bronze.customers
   VALUES (1001, 'John Doe', 'john.new@email.com', 
           '2026-03-31 14:30:00', NULL, TRUE)
   ```

**Final State (Bronze Table)**:
```
customer_id | name      | email              | start_date          | end_date            | is_current
------------|-----------|--------------------|--------------------|---------------------|------------
1001        | John Doe  | john@email.com     | 2026-01-01         | 2026-03-31 14:30:00 | FALSE
1001        | John Doe  | john.new@email.com | 2026-03-31 14:30:00| NULL                | TRUE
```

---

## Incremental Load Mechanism

### Control Table Schema
```
table_name             | wm_col              | last_loaded_wm      | updated_at
-----------------------|---------------------|---------------------|--------------------
retail.raw.customers   | created_timestamp   | 2026-03-30 10:00:00 | 2026-03-30 10:05:00
```

### Incremental View Creation
```sql
CREATE VIEW retail.raw.customers_vw AS
SELECT *
FROM retail.raw.customers
WHERE created_timestamp > '2026-03-30 10:00:00'
```

### Load Process Flow
```
1. run() starts
   ↓
2. For customers (load_type: "incremental"):
   ↓
3. get_incremental_records("retail.raw.customers")
   - Creates customers_vw with watermark filter
   ↓
4. load_scd2() reads from customers_vw
   - Only processes new/changed records
   ↓
5. After successful load:
   update_control_table("retail.raw.customers")
   - Updates last_loaded_wm to MAX(created_timestamp)
   ↓
6. Next run will only see records newer than updated watermark
```

### Benefit
- **Full Load**: Processes all 1,000,000 rows every time
- **Incremental Load**: Processes only 100 new rows since last run

---

## Key Design Patterns

### 1. Hash-Based Change Detection
```python
// Exclude audit columns from hash to avoid false positives
audit_columns = ['created_at', 'updated_at', 'start_date', 'end_date', 'is_current']
business_columns = all_columns - audit_columns

// Only business data changes trigger SCD2 versioning
row_hash = MD5(CONCAT(business_columns))
```

### 2. Null Handling
```python
// Fill nulls to ensure consistent processing
source_df.fillna("N/A")

// Filter out records with null primary keys
source_df.where(all primary_keys are NOT NULL)
```

### 3. Duplicate Prevention
```python
// Remove duplicates based on primary keys
source_df.dropDuplicates(primary_keys)

// Order by all columns for deterministic deduplication
source_df.orderBy(all columns ASC NULLS LAST)
```

### 4. Delta MERGE for Atomic Updates
```python
// Atomic operation ensures consistency
DELTA MERGE ensures:
- Old record expires
- New record inserts
- All or nothing (transactional)
```

---

## Function Call Graph

```
run()
├── load_scd2()
│   ├── get_incremental_records() [if load_type == "incremental"]
│   ├── spark.table() [read source]
│   ├── DeltaTable.forName().merge().execute() [expire old records]
│   ├── write.mode("append").saveAsTable() [insert new versions]
│   └── update_control_table() [if load_type == "incremental"]
│
└── load_append()
    ├── spark.table() [read source]
    └── write.mode("append").saveAsTable() [append to target]
```

---

## Configuration-Driven Design

### Adding a New Table

**Step 1**: Add to `bronze_config.json`
```json
{
  "table_name": "products",
  "primary_keys": ["product_id"],
  "load_strategy": "SCD2",
  "load_type": "incremental",
  "catalog_name": "retail",
  "source_schema": "raw",
  "target_schema": "bronze"
}
```

**Step 2**: Initialize control table (if incremental)
```sql
INSERT INTO retail.bronze.load_control
VALUES ('retail.raw.products', 'updated_at', '1900-01-01', current_timestamp())
```

**Step 3**: Run the notebook
- No code changes required
- Configuration drives the process

---

## Error Handling

Each function returns a status dictionary:
```python
// Success
{
  "status": "success",
  "operation": "incremental_load",
  "new_records": 10,
  "changed_records": 5
}

// Failure
{
  "status": "failed",
  "error": "Table not found: retail.raw.products"
}
```

---

## Summary

This Bronze layer implementation provides:

✓ **Flexible Load Strategies**: SCD2 for dimensions, APPEND for facts  
✓ **Efficient Processing**: Incremental loads reduce processing time  
✓ **History Tracking**: Full audit trail with SCD2  
✓ **Configuration-Driven**: Easy to add/modify tables  
✓ **Idempotent**: Can re-run safely  
✓ **Transactional**: Delta MERGE ensures consistency  
✓ **Scalable**: Handles large datasets with watermark filtering  

---

## Variables in Scope (After Full Execution)

```python
table_config              # List of table configurations from JSON
control_table_name       # "retail.bronze.load_control"
get_incremental_records  # Function to create incremental views
update_control_table     # Function to update watermarks
load_scd2                # Function for SCD2 processing
load_append              # Function for append processing
run                      # Main orchestration function
```