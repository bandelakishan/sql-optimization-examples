-- ================================================
-- Incremental Load Pattern
-- Author: Sree Kishan Bandela
-- Experience: RBC Bank & Medavie Blue Cross
-- Description: Incremental load pattern using
-- watermark table to load only new/changed
-- records — avoids full refresh on large datasets
-- ================================================

-- ================================================
-- 1. CREATE WATERMARK TABLE
-- Tracks last successful load timestamp
-- ================================================
CREATE TABLE dbo.Watermark (
    TableName       VARCHAR(100)    NOT NULL,
    LastLoadDate    DATETIME        NOT NULL,
    CreatedDate     DATETIME        DEFAULT GETDATE(),
    UpdatedDate     DATETIME        DEFAULT GETDATE()
);

-- Initialize watermark for Claims table
INSERT INTO dbo.Watermark (TableName, LastLoadDate)
VALUES ('Claims', '2000-01-01 00:00:00');

-- ================================================
-- 2. INCREMENTAL LOAD STORED PROCEDURE
-- Loads only new or changed records since
-- last successful run
-- ================================================
CREATE PROCEDURE usp_IncrementalLoad_Claims
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LastLoadDate   DATETIME;
    DECLARE @CurrentDate    DATETIME = GETDATE();

    -- Step 1: Get last load date from watermark
    SELECT @LastLoadDate = LastLoadDate
    FROM dbo.Watermark
    WHERE TableName = 'Claims';

    -- Step 2: Load only new/changed records
    INSERT INTO dbo.Claims_Target (
        ClaimID,
        MemberID,
        ClaimAmount,
        ClaimDate,
        Status,
        BusinessUnit,
        CreatedDate,
        ModifiedDate
    )
    SELECT
        s.ClaimID,
        s.MemberID,
        s.ClaimAmount,
        s.ClaimDate,
        s.Status,
        s.BusinessUnit,
        s.CreatedDate,
        s.ModifiedDate
    FROM dbo.Claims_Source s
    LEFT JOIN dbo.Claims_Target t 
        ON s.ClaimID = t.ClaimID
    WHERE 
        -- New records
        t.ClaimID IS NULL
        OR
        -- Changed records (modified since last load)
        s.ModifiedDate > @LastLoadDate;

    -- Step 3: Update existing changed records (SCD Type 1)
    UPDATE t
    SET
        t.ClaimAmount   = s.ClaimAmount,
        t.Status        = s.Status,
        t.ModifiedDate  = s.ModifiedDate
    FROM dbo.Claims_Target t
    INNER JOIN dbo.Claims_Source s 
        ON t.ClaimID = s.ClaimID
    WHERE s.ModifiedDate > @LastLoadDate;

    -- Step 4: Update watermark to current timestamp
    UPDATE dbo.Watermark
    SET
        LastLoadDate    = @CurrentDate,
        UpdatedDate     = GETDATE()
    WHERE TableName = 'Claims';

    -- Step 5: Log summary
    SELECT
        'Incremental Load Complete'     AS Status,
        @LastLoadDate                   AS LoadedFrom,
        @CurrentDate                    AS LoadedTo,
        @@ROWCOUNT                      AS RecordsProcessed;

END;
GO

-- ================================================
-- 3. EXECUTE THE INCREMENTAL LOAD
-- ================================================
EXEC usp_IncrementalLoad_Claims;

-- ================================================
-- 4. VERIFY RESULTS
-- ================================================
SELECT
    CAST(ModifiedDate AS DATE)  AS LoadDate,
    COUNT(*)                    AS RecordsLoaded,
    SUM(ClaimAmount)            AS TotalAmount
FROM dbo.Claims_Target
WHERE ModifiedDate >= CAST(GETDATE() AS DATE)
GROUP BY CAST(ModifiedDate AS DATE);
