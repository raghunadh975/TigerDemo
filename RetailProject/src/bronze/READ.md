# 🥉 Bronze Layer – Pseudocode & Working Explanation

## 📌 Overview

The Bronze layer is responsible for ingesting data from the **RAW layer** into Delta tables while preserving history and enabling incremental processing.

This implementation follows a **metadata-driven approach**, where all table-specific logic is controlled using configuration.

---

## ⚙️ High-Level Flow

```
FOR each table in configuration:
    READ data from RAW layer
    CHECK load strategy (SCD2 or APPEND)

    IF table does not exist in BRONZE:
        PERFORM initial load
    ELSE:
        PERFORM incremental load based on strategy

    STORE results and log status
```

---

## 🧾 Configuration Driven Design

Each table has:

* Table name
* Primary key(s)
* Load strategy (SCD2 / APPEND)
* Source schema (RAW)
* Target schema (BRONZE)

👉 This allows one single notebook to handle multiple tables dynamically.

---

## 🔄 Processing Logic

### 1. Read Source Data

```
READ table FROM retail.raw.<table_name>
```

---

### 2. Decide Load Strategy

```
IF load_strategy == "SCD2":
    CALL load_scd2()

ELSE IF load_strategy == "APPEND":
    CALL load_append()
```

---

## 🧠 SCD2 Logic (For Dimension Tables)

Used for:

* customers
* addresses

### 🪜 Steps:

```
IF target table does not exist:
    ADD columns:
        start_date
        end_date = NULL
        is_current = TRUE
        created_timestamp
        updated_timestamp

    WRITE full data to bronze

ELSE:
    READ current active records from bronze (is_current = TRUE)

    GENERATE hash for source and target rows

    IDENTIFY:
        changed_records → same PK but different data
        new_records → new PKs

    FOR changed_records:
        UPDATE existing bronze records:
            set is_current = FALSE
            set end_date = current timestamp

        INSERT new version:
            is_current = TRUE
            start_date = current timestamp

    FOR new_records:
        INSERT directly with:
            is_current = TRUE
```

---

## 📦 APPEND Logic (For Fact Tables)

Used for:

* orders
* payments
* refunds

### 🪜 Steps:

```
IF target table does not exist:
    ADD:
        created_timestamp
        updated_timestamp

    WRITE full data

ELSE:
    FIND new records using primary key (left anti join)

    IF new records exist:
        ADD timestamps
        APPEND to bronze table
```

---

## ⏱️ Execution Flow

```
START pipeline

FOR each table:
    PRINT table name and strategy
    START timer

    TRY:
        PROCESS table
        LOG success + record counts

    EXCEPT:
        LOG failure

    STOP timer

END FOR

PRINT summary:
    total tables
    success count
    failure count
    total execution time
```

---

## 📊 Output

Each Bronze table contains:

### For SCD2 tables:

* business columns
* start_date
* end_date
* is_current
* created_timestamp
* updated_timestamp

### For APPEND tables:

* business columns
* created_timestamp
* updated_timestamp

---

## 🎯 Key Design Highlights

* ✅ Metadata-driven (no hardcoding)
* ✅ Supports multiple tables in one pipeline
* ✅ Handles incremental loads
* ✅ Tracks history using SCD2
* ✅ Uses Delta tables for reliability
* ✅ Includes logging and error handling

---

## 🚀 Summary

The Bronze layer acts as:

* A **historical data store**
* A **foundation for Silver layer**
* A **scalable ingestion pipeline**

---
