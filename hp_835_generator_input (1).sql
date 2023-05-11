WITH CTE_unposted_remits AS (
    SELECT hp.*, 
        fpd.payerbilledto, 
        fad.subscriberid,
        CAST(CASE
            WHEN CAST(hp.GrossChargesRollup AS float) < 0
                THEN CAST(hp.GrossChargesRollup AS float) * -1
            ELSE CAST(hp.GrossChargesRollup AS float)
        END AS float) AS GrossCharges,
        ROW_NUMBER() OVER (PARTITION BY CheckNumber, hp.Claim, CPTwModRollup ORDER BY GrossChargesRollup) AS ChargeRow
    FROM hpremits_04202023 hp

    LEFT JOIN fullpaymentdetail fpd
    ON LEFT(hp.CPTwModRollup, 5) = fpd.cpt
    AND fpd.checknum LIKE CONCAT(hp.CheckNumber, '-%')
    AND hp.Claim = fpd.claim

    LEFT JOIN (SELECT claim, subscriberid FROM fullardetail GROUP BY claim, subscriberid) fad
    ON fad.claim = hp.Claim

    WHERE hp.CheckNumber NOT LIKE 'GSLB%'
        AND hp.Claim NOT LIKE 'GSLB%'
        AND fpd.checknum IS NULL
        AND InsPayRollup <> '0'
), CTE_formatted_file AS (
    SELECT
        'inSync' AS 'Billing Source',
        'Health Partners' AS 'Payer Billed To',
        'C' AS 'TransactionHandlingCode',
        SUM(GrossCharges) OVER (PARTITION BY CheckNumber) AS 'TotalActualProviderPaymentAmount',
        'ACH' AS 'PaymentMethodCode',
        TRY_CONVERT(date, CheckDate, 126) AS 'CheckIssueorEFTEffectiveDate',
        1 AS 'ClaimStatusCode',
        SUM(CASE WHEN ChargeRow = 1 THEN GrossCharges ELSE 0 END) OVER (PARTITION BY Claim) AS 'TotalClaimChargeAmount',
        CONCAT(CheckNumber, '-HPRecoupProject') AS 'CheckorEFTTraceNumber',
        'R1' AS 'PayeeName',
        '1871106278' AS 'PayeeIdentificationCode',
        ClaimPaymentAmount,
        'Omaha' AS 'PayeeCityName',
        CONCAT(CheckNumber, '-', ROW_NUMBER() OVER (PARTITION BY CheckNumber ORDER BY Claim, CPTwModRollup)) AS 'PayerClaimControlNumber',
        CPTwModRollup AS 'AdjudicatedProcedureCode',
        ROW_NUMBER() OVER (PARTITION BY CheckNumber, Claim ORDER BY CPTwModRollup) AS 'LineItemControlNumber',
        Claim AS 'PatientControlNumber',
        TRY_CONVERT(date, DOS, 126) AS 'ServiceDate',
        GrossCharges AS 'LineItemChargeAmount',
        InsPayRollup AS 'LineItemProviderPaymentAmount',
        CASE
            WHEN AdjustmentAmountRollup IS NULL
                THEN NULL
            ELSE 'CO'
        END AS 'SVCAdjustmentGroupCode1',
        CASE
            WHEN AdjustmentAmountRollup IS NULL
                THEN NULL
            ELSE '45'
        END AS 'SVCAdjustmentReasonCode1',
        CAST(CASE
            WHEN AdjustmentAmountRollup LIKE '%;%'
                THEN CAST(SUBSTRING(AdjustmentAmountRollup, 0, CHARINDEX(';', AdjustmentAmountRollup, 0)) AS float) 
                    + CAST(SUBSTRING(AdjustmentAmountRollup, CHARINDEX(';', AdjustmentAmountRollup, 0)+1, LEN(AdjustmentAmountRollup)) AS float)
            ELSE AdjustmentAmountRollup 
        END AS float) AS 'SVCAdjustmentAmount1',
        1 AS 'SVCQuantity1',
        NULL AS SVCAdjustmentGroupCode2,
        NULL AS SVCAdjustmentReasonCode2,
        NULL AS SVCAdjustmentAmount2,
        NULL AS	SVCQuantity2,
        NULL AS	SVCAdjustmentGroupCode3,
        NULL AS	SVCAdjustmentReasonCode3,
        NULL AS	SVCAdjustmentAmount3,
        NULL AS	SVCQuantity3,
        subscriberid AS 'SubscriberIdentifier'
    FROM CTE_unposted_remits

    WHERE CheckAmount <> 'CheckAmount'
        AND Claim <> '0'
)

SELECT * FROM CTE_formatted_file
