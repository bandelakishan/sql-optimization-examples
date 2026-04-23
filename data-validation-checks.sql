-- ================================================
-- Data Validation & Quality Check Scripts
-- Author: Sree Kishan Bandela
-- Experience: RBC Bank & Medavie Blue Cross
-- Description: SQL-based data quality checks used
-- to validate pipelines before data reaches
-- business users and BI reports
-- ================================================

-- ================================================
-- 1. NULL CHECK - Find missing critical fields
-- ================================================
SELECT
    'Claims' AS TableName,
    COUNT(*)  AS TotalRecords,
    SUM(CASE WHEN ClaimID IS NULL 
        THEN 1 ELSE 0 END)    AS NullClaimID,
    SUM(CASE WHEN MemberID IS NULL 
        THEN 1 ELSE 0 END)    AS NullMemberID,
    SUM(CASE WHEN ClaimAmount IS NULL 
        THEN 1 ELSE 0 END)    AS NullClaimAmount,
    SUM(CASE WHEN ClaimDate IS NULL 
        THEN 1 ELSE 0 END)    AS NullClaimDate
FROM dbo.Claims;

-- ================================================
-- 2. DUPLICATE CHECK - Identify duplicate records
-- ================================================
SELECT
    ClaimID,
    COUNT(*) AS DuplicateCount
FROM dbo.Claims
GROUP BY ClaimID
HAVING COUNT(*) > 1
ORDER BY DuplicateCount DESC;

-- ================================================
-- 3. REFERENTIAL INTEGRITY CHECK
-- Orphaned records without matching member
-- ================================================
SELECT
    c.ClaimID,
    c.MemberID,
    c.ClaimAmount
FROM dbo.Claims c
LEFT JOIN dbo.Members m ON c.MemberID = m.MemberID
WHERE m.MemberID IS NULL;

-- ================================================
-- 4. ROW COUNT RECONCILIATION
-- Compare source vs target record counts
-- ================================================
SELECT
    'Source' AS Layer,
    COUNT(*) AS RecordCount
FROM dbo.Claims_Source
UNION ALL
SELECT
    'Target' AS Layer,
    COUNT(*) AS RecordCount
FROM dbo.Claims_Target;

-- ================================================
-- 5. SUM RECONCILIATION
-- Validate total amounts match source vs target
-- ================================================
SELECT
    'Source'            AS Layer,
    SUM(ClaimAmount)    AS TotalAmount
FROM dbo.Claims_Source
UNION ALL
SELECT
    'Target'            AS Layer,
    SUM(ClaimAmount)    AS TotalAmount
FROM dbo.Claims_Target;

-- ================================================
-- 6. VALUE DOMAIN CHECK
-- Ensure Status only contains valid values
-- ================================================
SELECT
    Status,
    COUNT(*) AS RecordCount
FROM dbo.Claims
WHERE Status NOT IN ('Approved', 'Rejected', 'Pending')
GROUP BY Status;

-- ================================================
-- 7. DATE RANGE CHECK
-- Flag records outside expected date range
-- ================================================
SELECT
    ClaimID,
    ClaimDate,
    ClaimAmount
FROM dbo.Claims
WHERE ClaimDate < '2000-01-01'
   OR ClaimDate > GETDATE();

-- ================================================
-- 8. INCREMENTAL LOAD VALIDATION
-- Confirm only new records were loaded today
-- ================================================
SELECT
    CAST(CreatedDate AS DATE) AS LoadDate,
    COUNT(*)                  AS RecordsLoaded
FROM dbo.Claims
WHERE CAST(CreatedDate AS DATE) = CAST(GETDATE() AS DATE)
GROUP BY CAST(CreatedDate AS DATE);
