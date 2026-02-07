/****** Script to identify Potential Duplicate Invoices
Data: Payment of Invoices of Last 24 months
APPROACH 1 to find Duplicate Invoices, PKEY: Same PLANT - Supplier - ABS(Invoice_Amount) - InvoiceNumber(Formated) - SAME Invoice Date considering in the analyses the records with PAID/VOIDED, IN/DM
******/
SELECT  
	   CONCAT([AP_PLANT], '-', VOUCHER, '-', CHECK_NUMBER, '-', SUPPLIER, '-', INVOICE_NUMBER, '-', INVOICE_AMOUNT, '-', INVOICE_DATE) as Primary_KEY 
	  ,[POSTING_YEAR]
      ,[POSTING_PERIOD]
      ,[AP_PLANT]
	  ,A.[STANDARDIZED_COMPANY_KEY_GROUP_ID]
      ,[VOUCHER]
      ,[PURCHASE_ORDER]
      ,[RELEASE]
      ,UPPER(LTRIM(RTRIM([INVOICE_NUMBER]))) as INVOICE_NUMBER
	  ,REPLACE (REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( LTRIM(RTRIM(UPPER([INVOICE_NUMBER]))), '''2', ''), '''1', ''), '~3', ''), '~2', ''), '~1', ''), '`',  '' ) as Invoice_Number_Format
      ,[INVOICE_DATE]
      ,[INVOICE_TYPE]
      ,[INVOICE_STATUS]
      ,[SUPPLIER]
      ,UPPER(LTRIM(RTRIM([SUPPLIER_NAME]))) as SUPPLIER_NAME
      ,CASE WHEN ( UPPER(LEFT(LTRIM(RTRIM([SUPPLIER_NAME])),3)) = 'ZZZ') THEN SUBSTRING(UPPER(LTRIM(RTRIM([SUPPLIER_NAME]))),4,LEN(UPPER(LTRIM(RTRIM([SUPPLIER_NAME]))))-4)
		    WHEN ( UPPER(LEFT(LTRIM(RTRIM([SUPPLIER_NAME])),2)) = 'ZZ' AND UPPER(LEFT(LTRIM(RTRIM([SUPPLIER_NAME])),3)) <> 'ZZZ') THEN UPPER(SUBSTRING(LTRIM(RTRIM([SUPPLIER_NAME])),3,LEN(LTRIM(RTRIM([SUPPLIER_NAME])))-3))
			ELSE UPPER(LTRIM(RTRIM([SUPPLIER_NAME]))) END AS SupplierName_Format
      ,[CHECK_NUMBER]
      ,[CHECK_DATE]
      ,[RECEIPT_DATE]
      ,[INVOICE_PAYMENT_TOTAL]
      ,[INVOICE_ENTERED_DATE]
      ,[INVOICE_AMOUNT]
      ,ABS(INVOICE_AMOUNT) as INVOICE_AMOUNT_ABS
      ,[CURRENCY_TYPE]
      ,A.[DATE_CREATED]
      ,A.[CREATED_BY]
      ,A.[DATE_UPDATED]
      ,A.[UPDATED_BY]
      ,[AXIOM_DISCOUNT_AMOUNT]
      ,[AXIOM_FREIGHT_AMOUNT]
      ,[AXIOM_MISC_AMOUNT]
      ,[AXIOM_TAX_AMOUNT]
      ,[CHECK_DESCRIPTION]
      ,[CHECK_ADDENDUM]
      ,[INVOICE_AMOUNT_INDICATOR]
      ,[MS_SOURCE]
      ,[PO_AND_RELEASE_NUM]
      ,[BANK_PAYMENT_TYPE]
      ,[BILL_OF_LADING_NUMBER]
      ,[DISCOUNT_TAKEN]
      ,[DUE_DATE]
      ,[QUANTITY_PAID]
      ,[SUPPLY_GROUP_ID]
      ,[ENTERED_BY]
  INTO #CheckDupl_1
  FROM [VENDOR_PAYMENTS].[dbo].[COMPLETED_INVOICES_MS] A (NOLOCK)

  WHERE [POSTING_YEAR] >= 2019
    AND INVOICE_TYPE in ( 'IN', 'DM')
    AND UPPER(INVOICE_STATUS) IN ('PAID','VOIDED')
    AND SUPPLY_GROUP_ID <> 'Employee' AND SUPPLY_GROUP_ID <> 'Ex Empl/Rel' AND SUPPLY_GROUP_ID <> 'Retiree' 
	AND INVOICE_AMOUNT <> 0
  ORDER BY AP_PLANT, SUPPLIER_NAME, ABS(INVOICE_AMOUNT), INVOICE_DATE , UPPER(LTRIM(RTRIM([INVOICE_NUMBER])))


   	SELECT 
	       Primary_KEY
		   , CASE WHEN ((RIGHT(Invoice_Number_Format,3) = 'VD1') OR (RIGHT(Invoice_Number_Format,3) = 'VD2')  OR (RIGHT(Invoice_Number_Format,3) = 'VD3') 
				OR (RIGHT(Invoice_Number_Format,3) = 'CR1')  OR (RIGHT(Invoice_Number_Format,3) = 'CR2') OR (RIGHT(Invoice_Number_Format,3) = 'CR3')
				) THEN LEFT(Invoice_Number_Format,LEN(Invoice_Number_Format)-3)
		          WHEN ((RIGHT(Invoice_Number_Format,2) = 'CR')  
				) THEN LEFT(Invoice_Number_Format,LEN(Invoice_Number_Format)-2)

		    ELSE Invoice_Number_Format END AS Invoice_Number_Format
		  
		  , CONCAT([AP_PLANT], '-', 
		  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
			REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(RTRIM(LTRIM(UPPER(SupplierName_Format))),
			'-',''),' ',''),'#',''),'!',''),'@',''),'*',''),'$',''),'%',''),'^',''),'&',''),CHAR(13),''),CHAR(10),''),'"',''),'/',''),'.',''),'+',''),'|',''),'''',''),')',''),'(',''),'\',''),'+',''),'_',''),'<',''),'>',''),'?',''),',',''),'[',''),']',''),';',''),':',''),'`','') 
		  ,'-',INVOICE_AMOUNT_ABS) as Duplicate_KEY_1

		, CASE WHEN ((RIGHT(Invoice_Number_Format,3) = 'VD1') OR (RIGHT(Invoice_Number_Format,3) = 'VD2')  OR (RIGHT(Invoice_Number_Format,3) = 'VD3')) 
				OR (RIGHT(Invoice_Number_Format,3) = 'CR1')  OR (RIGHT(Invoice_Number_Format,3) = 'CR2') OR (RIGHT(Invoice_Number_Format,3) = 'CR3')
		       THEN CONCAT([AP_PLANT], '-'
				, 		  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
					REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(RTRIM(LTRIM(UPPER(SupplierName_Format))),
					'-',''),' ',''),'#',''),'!',''),'@',''),'*',''),'$',''),'%',''),'^',''),'&',''),CHAR(13),''),CHAR(10),''),'"',''),'/',''),'.',''),'+',''),'|',''),'''',''),')',''),'(',''),'\',''),'+',''),'_',''),'<',''),'>',''),'?',''),',',''),'[',''),']',''),';',''),':',''),'`','') 
		  
				,'-',INVOICE_AMOUNT_ABS, '-', POSTING_PERIOD, '-', LEFT(Invoice_Number_Format,LEN(Invoice_Number_Format)-3))

		      WHEN ((RIGHT(Invoice_Number_Format,2) = 'CR'))
		       THEN CONCAT([AP_PLANT], '-'
				, 		  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
					REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(RTRIM(LTRIM(UPPER(SupplierName_Format))),
					'-',''),' ',''),'#',''),'!',''),'@',''),'*',''),'$',''),'%',''),'^',''),'&',''),CHAR(13),''),CHAR(10),''),'"',''),'/',''),'.',''),'+',''),'|',''),'''',''),')',''),'(',''),'\',''),'+',''),'_',''),'<',''),'>',''),'?',''),',',''),'[',''),']',''),';',''),':',''),'`','') 
		  
				,'-',INVOICE_AMOUNT_ABS, '-', POSTING_PERIOD, '-', LEFT(Invoice_Number_Format,LEN(Invoice_Number_Format)-2))

		    ELSE CONCAT([AP_PLANT], '-', 
							  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
			REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(RTRIM(LTRIM(UPPER(SupplierName_Format))),
			'-',''),' ',''),'#',''),'!',''),'@',''),'*',''),'$',''),'%',''),'^',''),'&',''),CHAR(13),''),CHAR(10),''),'"',''),'/',''),'.',''),'+',''),'|',''),'''',''),')',''),'(',''),'\',''),'+',''),'_',''),'<',''),'>',''),'?',''),',',''),'[',''),']',''),';',''),':',''),'`','') 
					,'-',INVOICE_AMOUNT_ABS, '-', POSTING_PERIOD, '-', Invoice_Number_Format) 
			END AS Duplicate_KEY_2


		, CASE WHEN ((RIGHT(Invoice_Number_Format,3) = 'VD1') OR (RIGHT(Invoice_Number_Format,3) = 'VD2')  OR (RIGHT(Invoice_Number_Format,3) = 'VD3')) 
				OR (RIGHT(Invoice_Number_Format,3) = 'CR1')  OR (RIGHT(Invoice_Number_Format,3) = 'CR2') OR (RIGHT(Invoice_Number_Format,3) = 'CR3') 
			THEN CONCAT([AP_PLANT], '-', 
							  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
					REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(RTRIM(LTRIM(UPPER(SupplierName_Format))),
					'-',''),' ',''),'#',''),'!',''),'@',''),'*',''),'$',''),'%',''),'^',''),'&',''),CHAR(13),''),CHAR(10),''),'"',''),'/',''),'.',''),'+',''),'|',''),'''',''),')',''),'(',''),'\',''),'+',''),'_',''),'<',''),'>',''),'?',''),',',''),'[',''),']',''),';',''),':',''),'`','') 
					,'-',INVOICE_AMOUNT_ABS, '-', INVOICE_DATE, '-', LEFT(Invoice_Number_Format,LEN(Invoice_Number_Format)-3))

			WHEN ((RIGHT(Invoice_Number_Format,2) = 'CR')) 
			THEN CONCAT([AP_PLANT], '-', 
							  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
					REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(RTRIM(LTRIM(UPPER(SupplierName_Format))),
					'-',''),' ',''),'#',''),'!',''),'@',''),'*',''),'$',''),'%',''),'^',''),'&',''),CHAR(13),''),CHAR(10),''),'"',''),'/',''),'.',''),'+',''),'|',''),'''',''),')',''),'(',''),'\',''),'+',''),'_',''),'<',''),'>',''),'?',''),',',''),'[',''),']',''),';',''),':',''),'`','') 
					,'-',INVOICE_AMOUNT_ABS, '-', INVOICE_DATE, '-', LEFT(Invoice_Number_Format,LEN(Invoice_Number_Format)-2))


		    ELSE CONCAT([AP_PLANT], '-', 
							  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
			REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(RTRIM(LTRIM(UPPER(SupplierName_Format))),
			'-',''),' ',''),'#',''),'!',''),'@',''),'*',''),'$',''),'%',''),'^',''),'&',''),CHAR(13),''),CHAR(10),''),'"',''),'/',''),'.',''),'+',''),'|',''),'''',''),')',''),'(',''),'\',''),'+',''),'_',''),'<',''),'>',''),'?',''),',',''),'[',''),']',''),';',''),':',''),'`','') 
					,'-',INVOICE_AMOUNT_ABS, '-', INVOICE_DATE, '-', Invoice_Number_Format )
			END AS Duplicate_KEY_3

        INTO #CheckDupl_2
	FROM #CheckDupl_1
	
	SELECT A.Primary_KEY
	  ,[POSTING_YEAR]
      ,[POSTING_PERIOD]
      ,[AP_PLANT]
	  ,[STANDARDIZED_COMPANY_KEY_GROUP_ID]
      ,[VOUCHER]
      ,[PURCHASE_ORDER]
      ,[RELEASE]
      ,Invoice_Number
      ,B.Invoice_Number_Format
      ,[INVOICE_DATE]
      ,[INVOICE_TYPE]
      ,[INVOICE_STATUS]
      ,[SUPPLIER]
      ,SUPPLIER_NAME
      ,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
			REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
		    RTRIM(LTRIM(UPPER(SupplierName_Format))),
			'-',''),' ',''),'#',''),'!',''),'@',''),'*',''),'$',''),'%',''),'^',''),'&',''),CHAR(13),''),CHAR(10),''),'"',''),'/',''),'.',''),'+',''),'|',''),'''',''),')',''),'(',''),'\',''),'+',''),'_',''),'<',''),'>',''),'?',''),',',''),'[',''),']',''),';',''),':',''),'`','') 
		 as SupplierName_Format
      ,[CHECK_NUMBER]
      ,[CHECK_DATE]
      ,[RECEIPT_DATE]
      ,[INVOICE_PAYMENT_TOTAL]
      ,[INVOICE_ENTERED_DATE]
      ,[INVOICE_AMOUNT]
      , INVOICE_AMOUNT_ABS
      ,[CURRENCY_TYPE]
      ,[DATE_CREATED]
      ,[CREATED_BY]
      ,[DATE_UPDATED]
      ,[UPDATED_BY]
      ,[AXIOM_DISCOUNT_AMOUNT]
      ,[AXIOM_FREIGHT_AMOUNT]
      ,[AXIOM_MISC_AMOUNT]
      ,[AXIOM_TAX_AMOUNT]
      ,[CHECK_DESCRIPTION]
      ,[CHECK_ADDENDUM]
      ,[INVOICE_AMOUNT_INDICATOR]
      ,[MS_SOURCE]
      ,[PO_AND_RELEASE_NUM]
      ,[BANK_PAYMENT_TYPE]
      ,[BILL_OF_LADING_NUMBER]
      ,[DISCOUNT_TAKEN]
      ,[DUE_DATE]
      ,[QUANTITY_PAID]
      ,[SUPPLY_GROUP_ID]
      ,[ENTERED_BY]
	  , B.Duplicate_KEY_1, B.Duplicate_KEY_2, B.Duplicate_KEY_3
	INTO #CheckDupl_3
	FROM #CheckDupl_1 A ,  #CheckDupl_2 B
	WHERE A.Primary_KEY = B.Primary_KEY


	SELECT A.Duplicate_KEY_3, SUM(A.INVOICE_AMOUNT) as Sum_Invoice_Amount, count(*) as Qty_Key
	INTO #CheckDupl_4
	FROM #CheckDupl_3 A 
	group by A.Duplicate_KEY_3
	Having count(*) > 1

	SELECT A.Duplicate_KEY_3, SUM(A.INVOICE_AMOUNT) as Sum_Invoice_Amount_PAID, count(*) as Qty_Key_PAID
	INTO #CheckDupl_4_PAID
	FROM #CheckDupl_3 A 
	WHERE INVOICE_STATUS = 'PAID'
	group by A.Duplicate_KEY_3
	Having count(*) > 1

	SELECT A.Duplicate_KEY_3, SUM(A.INVOICE_AMOUNT) as Sum_Invoice_Amount_VOIDED, count(*) as Qty_Key_VOIDED
	INTO #CheckDupl_4_VOIDED
	FROM #CheckDupl_3 A 
	WHERE INVOICE_STATUS = 'VOIDED'
	group by A.Duplicate_KEY_3
	Having count(*) > 1

	SELECT A.*, B.Sum_Invoice_Amount, B.Qty_Key , C.Sum_Invoice_Amount_PAID, C.Qty_Key_PAID , D.Sum_Invoice_Amount_VOIDED, D.Qty_Key_VOIDED
		, CASE 
		    WHEN ( ((B.Sum_Invoice_Amount < 0) and (ABS(B.Sum_Invoice_Amount) <> A.INVOICE_AMOUNT_ABS)) )  THEN 'Potential DM Duplicate-NOT Reviewed'

			-- -5, 5
			-5, 5, 5
			WHEN ( (B.Sum_Invoice_Amount = 0) OR 
			       ((B.Sum_Invoice_Amount > 0) and (ABS(B.Sum_Invoice_Amount) = A.INVOICE_AMOUNT_ABS)) OR 
		           ((B.Sum_Invoice_Amount < 0) and (ABS(B.Sum_Invoice_Amount) = A.INVOICE_AMOUNT_ABS)) OR
				   ( ((B.Sum_Invoice_Amount > 0)) AND (C.Qty_Key_PAID = D.Qty_Key_VOIDED) )
				 ) THEN 'IN Duplicate-REVERSED'

		    WHEN ( (B.Sum_Invoice_Amount > 0) and (ABS(B.Sum_Invoice_Amount) <> A.INVOICE_AMOUNT_ABS) )  THEN 'Potential IN Duplicate-NOT Reviewed'

	 	  ELSE 'Check' END AS Bucket_DuplInvoice

	FROM #CheckDupl_3 A 
	INNER JOIN #CheckDupl_4 B ON
	( A.Duplicate_KEY_3 = B.Duplicate_KEY_3 )

	LEFT JOIN #CheckDupl_4_PAID C ON
	( A.Duplicate_KEY_3 = C.Duplicate_KEY_3 )

	LEFT JOIN #CheckDupl_4_VOIDED D ON
	( A.Duplicate_KEY_3 = D.Duplicate_KEY_3 )

	ORDER BY A.Duplicate_KEY_3 