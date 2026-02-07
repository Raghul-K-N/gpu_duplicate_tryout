import json
from code1.logger import capture_log_message
class dataErrorOutput:
    ''' This is the example of what each object should look like
        nullCheck = {
            "status": "Pass|Fail",
            "message": "Null Check Passed|Null Column detected on {columns}",
            "data": [{"column":"ACCOUNTING_DOC",
            "count":"2", 
            "rows":[3,5]}, 
            {"column":"POSTED_DATE","count":"2", "rows":[0,5]}, 
            {"column":"ENTRY_DATE","count":"2", "rows":[1,2]}]
        }

        uniqueIdentifier={    
            "status": "Pass|Fail",
            "message": "Unique key identifier is accepted|The given column {columns} is not unique identifier",
            "data": ["column1", "column2", "column3"]
        }

        creditDebitIndicator = {
            "status": "Pass|Fail",
            "message": "{DEBIT_CREDIT_INDICATOR} and {AMOUNT} is present in the Data columns|{DEBIT_AMOUNT} and {CREDIT_AMOUNT} already present in the Data columns|Debit and Credit Amount not provided or cannot be created with current data",
            "data": ["column1", "column2", "column3"]
        }

        creditDebitBalance={
            "status": "Pass|Fail",
            "message": "The debit and credit amount are balanced|There are {imbalanced_records_count} accounting docs with imbalance records",
            "data": ["accountingDocId1", "accountingDocId2", "accountingDocId3"]

        }
        dateCheck={
            "status": "Pass|Fail",
            "message": "The date format is correct|The date format is incorrect",
            "data": [{"column":"POSTING_DATE","count":"2", "rows":[3,5]}, {"column":"ENTRY_DATE","count":"2", "rows":[0,5]}, {"column":"ENTRY_DATE","count":"2", "rows":[1,2]}]

        }
        manualEntryFlag={
            "status": "Pass|Fail",
            "message": "The manual entry flag exists for all records|There are {count} records without manual entry flag|Manual entry flag is not provided for the data(No Check Required)",
            "date": {"count":10, "rows":[1,2,3,4,5,6,7,8,9,10]}
        }
        dueDateCheck={
            "status": "Pass|Fail",
            "message": "Due Date check passed|Due Date column created with {cols[0]} and {cols[1]}",
            "data": ["column1", "column2"]
        }
        dataTypeCheck={
            "status": "Pass|Fail",
            "message": "Expected data type present|There are {count} columns with incorrect data type",
        "data": [{"column":"POSTING_DATE","count":"2", "rows":[3,5]}, {"column":"ENTRY_DATE","count":"2", "rows":[0,5]}, {"column":"ENTRY_DATE","count":"2", "rows":[1,2]}]
        }

        requiredColumnPresent={
            "status": "Pass|Fail",
            "message": "Missing {count} columns",
            "data": ["POSTING_DATE"]
        }
        mapping = [(ACCOUNTING_DOC,Financial Doc Id - PK),
(TRANSACTION_DESCRIPTION,Fin Doc Line Item Text),
(DOC_TYPE,Document Type Id),
(DOC_TYPE_DESCRIPTION,Document Type Description),
(AMOUNT,Line Amt LOC),
(ENTERED_BY,Parking Username),
(POSTED_BY,Username),
(SAP_ACCOUNT,Gl Account ID),
(ACCOUNT_DESCRIPTION,GL Account Name),
(SAP_COMPANY,Company Code Id),
(POSTED_LOCATION,Posting Location),
(ENTERED_LOCATION,Entered Location),
(POSTING_DATE,Posting Date),
(LINE_ITEM_IDENTIFIER,Line Item Id - PK),
(DEBIT_AMOUNT,Line Debit Amt LOC),
(ENTRY_DATE,Entry Date),
(ENTRY_TIME,Entry Time),
(MANUAL_ENTRY,Manual Entry),
(DEBIT_CREDIT_INDICATOR,DEBIT_CREDIT_INDICATOR)]
        '''
    def __init__(self):
        self.mapping = []
        self.nullCheck = {
                            "check":"nullCheck",
                            "status": "",
                            "message": "",
                            "data":[]
                        }
        self.uniqueIdentifier={  
                            "check":"uniqueIdentifier",  
                            "status": "",
                            "message": "",
                            "data": []
                        }
        self.creditDebitIndicator = {
                            "check":"creditDebitIndicator",
                            "status": "",
                            "message": "",
                            "data": []
                        }
        self.creditDebitBalance={
                            "check":"creditDebitBalance",   
                            "status": "",
                            "message": "",
                            "data": []
                        }
        self.dateCheck={
                            "check":"dateCheck",
                            "status": "",
                            "message": "",
                            "data": []
                        }
        self.manualEntryFlag={
                            "check":"manualEntryFlag",
                            "status": "",
                            "message": "",
                            "data": {}
                        }
        self.dueDateCheck={
                            "check":"dueDateCheck",
                            "status": "",
                            "message": "",
                            "data": []
                        }
        self.dataTypeCheck={
                            "check":"dataTypeCheck",
                            "status": "",
                            "message": "",
                            "data": []
                        }
        self.requiredColumnPresent={
                            "check":"requiredColumnPresent",
                            "status": "",
                            "message": "",
                            "data": []
                        }
        self.ConditionalNullCheck={
                            "check":"ConditionalNullCheck",
                            "status": "",
                            "message": "",
                            "data": []
                        }
        self.DocTypeCheck={
            "check":"DocTypeCheck",
            "status": "",
            "message": "",
            "data": []
                        }
        self.DateFormatCheck={
            "check":"DateFormatCheck",
            "status": "",
            "message": "",
            "data": []
                        }
        self.creditPeriodConsistency={
            "check":"creditPeriodConsistency",
            "status": "",
            "message": "",
            "data": []
                        }
        
    def updatecreditPeriodConsistency(self, status, message, data):
        self.creditPeriodConsistency["status"] = status
        self.creditPeriodConsistency["message"] = message
        self.creditPeriodConsistency["data"] = data

    def updateDateFormatCheck(self, status, message, data):
        self.DateFormatCheck["status"] = status
        self.DateFormatCheck["message"] = message
        self.DateFormatCheck["data"] = data
    def updateMapping(self, mapping):
        if not self.mapping:
            self.mapping = mapping
        else:
            self.mapping.extend(mapping)
    def updateConditionalNullCheck(self, status, message, data):
        self.ConditionalNullCheck["status"] = status
        self.ConditionalNullCheck["message"] = message
        self.ConditionalNullCheck["data"] = data
    def updateRequiredColumnPresent(self, status, message, data):
        self.requiredColumnPresent["status"] = status
        self.requiredColumnPresent["message"] = message
        self.requiredColumnPresent["data"] = data        
    def updateNullCheck(self, status, message, data):
        self.nullCheck["status"] = status
        self.nullCheck["message"] = message
        self.nullCheck["data"] = data
    def updateUniqueIdentifier(self, status, message, data):
        self.uniqueIdentifier["status"] = status
        self.uniqueIdentifier["message"] = message
        self.uniqueIdentifier["data"] = data
    def updateCreditDebitIndicator(self, status, message, data):
        self.creditDebitIndicator["status"] = status
        self.creditDebitIndicator["message"] = message
        self.creditDebitIndicator["data"] = data
    def updateCreditDebitBalance(self, status, message, data):
        self.creditDebitBalance["status"] = status
        self.creditDebitBalance["message"] = message
        self.creditDebitBalance["data"] = data
    def updateDateCheck(self, status, message, data):
        self.dateCheck["status"] = status
        self.dateCheck["message"] = message
        self.dateCheck["data"] = data
    def updateManualEntryFlag(self, status, message, data):
        self.manualEntryFlag["status"] = status
        self.manualEntryFlag["message"] = message
        self.manualEntryFlag["data"] = data
    def updateDueDateCheck(self, status, message, data):
        self.dueDateCheck["status"] = status
        self.dueDateCheck["message"] = message
        self.dueDateCheck["data"] = data
    def updateDataTypeCheck(self, status, message, data):
        self.dataTypeCheck["status"] = status
        self.dataTypeCheck["message"] = message
        self.dataTypeCheck["data"] = data
    def updateDocTypeFlag(self, status, message, data):
        self.DocTypeCheck["status"] = status
        self.DocTypeCheck["message"] = message
        self.DocTypeCheck["data"] = data
    
    def returnCDindicatorPresent(self):
        return self.creditDebitIndicator

    def finalOutput (self):
        '''return the final error list as a json ouput'''
        errorList = [self.creditPeriodConsistency,self.DateFormatCheck, self.requiredColumnPresent, self.DocTypeCheck,self.dataTypeCheck, self.uniqueIdentifier, self.nullCheck, self.creditDebitIndicator, self.creditDebitBalance, self.dateCheck, self.manualEntryFlag, self.dueDateCheck,self.ConditionalNullCheck]
        errorListJson = json.dumps(errorList)
        return errorListJson
    
    def generateColumnErrors(self):
        '''generate the column errors'''
        checks = [self.creditPeriodConsistency,self.DateFormatCheck, self.dataTypeCheck,self.DocTypeCheck, self.uniqueIdentifier, self.nullCheck, self.creditDebitIndicator, self.creditDebitBalance, self.dateCheck, self.manualEntryFlag, self.dueDateCheck,self.ConditionalNullCheck]
        final =[]
        for map in self.mapping:
            obj = {"trName": map[0],
                   "srcName": map[1],
                   "checks": []}
            for check in checks:
                if check["check"] == "dataTypeCheck":
                    if check["status"] == "Pass" and map[0] in check["data"]:
                        obj["checks"].append({str(check["check"])+'_PASS':"Passed Data Type Validation"})
                    elif check["status"] == "Fail":
                        for i in check["data"]:
                            if i["Column"] in map[0]:
                                obj["checks"].append({str(check["check"])+'_FAIL':"Data does not adhere to expected data format/pattern",'rows':str(i["Row"])})

                elif check["check"] == "uniqueIdentifier":
                    if check["status"] == "Pass" and map[0] in check["data"]:
                        if len(check["data"]) > 1:
                            obj["checks"].append({str(check["check"])+'_PASS':"Valid Unique Identifier generated from"+str(check["data"])})
                        elif len(check["data"]) == 1:
                            obj["checks"].append({str(check["check"])+'_PASS':"Valid Unique Identifier"})

                    elif check["status"] == "Fail" and map[0] in check["data"]:
                        if len(check["data"]) > 1:
                            obj["checks"].append({str(check["check"])+'_FAIL':"Unique Indentifier Validation Failed for"+str(check["data"])})
                        elif len(check["data"]) == 1:
                            obj["checks"].append({str(check["check"])+'_FAIL':"Unique Indentifier Validation Failed for"+str(check["data"])})

                elif check["check"] == "nullCheck":
                    null_list=[]
                    for i in  check["data"]:
                       null_list.append(str(i["Column"]))
                    if check['status'] == "Fail" and map[0] in null_list:
                        for i in  check["data"]:
                            if i["Column"]==map[0]:
                                obj["checks"].append({str(check["check"])+'_FAIL':str(i["Count"])+ " rows have failed the Null Value Validation.", 'rows': str(i["Row"])})           
                    if check["status"] == "Pass" and map[0] in null_list:
                        for i in  check["data"]:
                            if i["Column"]==map[0]:
                                obj["checks"].append({str(check["check"])+'_PASS':"Null Value Validation Passed"})
                
                elif check["check"] == "creditDebitIndicator":
                    if check["status"] == "Pass" and map[0] in check["data"]:
                            obj["checks"].append({str(check["check"])+'_PASS':"Data Quality check passed"})
                    if check["status"] == "Fail" and map[0] in check["data"]:
                            obj["checks"].append({str(check["check"])+'_FAIL':str(check["message"])})

                elif check["check"] == "creditDebitBalance":
                    #ACCOUNTING_DOC
                    if check["status"] == "Pass" and map[0] == "ACCOUNTING_DOC":
                        obj["checks"].append({str(check["check"])+'_PASS':str(check["message"])})
                    if check["status"] == "Fail" and map[0] == "ACCOUNTING_DOC":
                        for i in check["data"]:
                            obj["checks"].append({str(check["check"])+'_FAIL':str(check["message"])+" Accounting Doc IDs ",'rows':str(i["Doc"])})

                elif check["check"] == "dateCheck":
                    ex=[]
                    for i in  check["data"]:
                       ex.append(str(i["column"]))
                    if check['status'] == "Fail" and map[0] in ex:
                        for i in  check["data"]:
                            if i["column"]==map[0]:
                                obj["checks"].append({str(check["check"])+'_FAIL':str(i["count"])+ " rows have failed the Date Format Validation.", 'rows': str(i["rows"])})
                    if check["status"] == "Pass" and map[0] in ex:
                        for i in  check["data"]:
                            if i["column"]==map[0]:
                                obj["checks"].append({str(check["check"])+'_PASS':"Date Format Validation Passed"})
                
                elif check["check"] == "manualEntryFlag":
                    if check['status'] == "Fail" and map[0] in check["data"][0]['column']:
                        obj["checks"].append({str(check["check"])+'_FAIL':str(check["data"][0]["count"])+ " rows have failed  in Manual Entry column Validation.", 'rows':str(check["data"][0]["rows"])})          
                    if check["status"] == "Fail_Sub" and map[0] in check["data"][0]['column']:
                        obj["checks"].append({str(check["check"])+'_FAIL_SUB':"There are values other than YES or NO"})
                    if check["status"] == "Pass" and map[0] in check["data"][0]['column']:
                        obj["checks"].append({str(check["check"])+'_PASS':"Manual Entry Validation Passed"})
                

                elif check["check"] == "dueDateCheck":
                    if check["status"] == "Pass" and map[0] in check["data"]:
                        obj["checks"].append({str(check["check"])+'_PASS':"Due Date Check Passed"})
                    elif check["status"] == "Fail" and map[0] in check["data"]:
                        obj["checks"].append({str(check["check"])+'_FAIL':"Due Date Check Failed"})

                elif check["check"] == "ConditionalNullCheck":
                    ex=[]
                    for i in check["data"]:
                       ex.append(str(i["column"]))
                    if check['status'] == "Fail" and map[0] in ex:
                        for i in check["data"]:
                            if i["count"]>0:
                                if i["column"]==map[0]:
                                    obj["checks"].append({str(check["check"])+'_FAIL' : str(i["count"])+" rows have failed the Conditional Null Check Validation.", 'rows': str(i["row"])})
                            else:
                                if i["column"]==map[0]:
                                    obj["checks"].append({str(check["check"])+'_PASS' : "Conditional Null Check Validation Passed"})
                    elif check["status"] == "Pass" and map[0] in ex:
                        obj["checks"].append({str(check["check"])+'_PASS':"Conditional Null Check Validation Passed"})
                
                elif check["check"] == "DocTypeCheck":
                    ex=[]   
                    for i in check["data"]:
                       ex.append(str(i["column"]))
                    if check["status"] == "Pass" and map[0] in ex:
                        obj["checks"].append({str(check["check"])+'_PASS':"Document type Check Passed"})
                    elif check["status"] == "Fail" and map[0] in ex:
                        obj["checks"].append({str(check["check"])+'_FAIL' : str(i["count"])+" rows have failed the Document type Check Validation.", 'rows': str(i["row"])})

                elif check["check"] == "DateFormatCheck":
                    ex=[]
                    for i in check["data"]:
                       ex.append(str(i["column"][0]))
                    if check['status'] == "Fail" and map[0] in ex:
                        for i in  check["data"]:
                            if i["column"][0]==map[0]:
                                capture_log_message(f"DateFormatCheck Fail for {str(i['column'])}")
                                obj["checks"].append({str(check["check"])+'_FAIL':str(i["count"])+ " rows have failed the Date Format Validation.", 'rows': str(i["rows"])})
                    if check["status"] == "Pass" and map[0] in ex:
                        for i in  check["data"]:
                            if i["column"]==map[0]:
                                obj["checks"].append({str(check["check"])+'_PASS':"Date Format Validation Passed"})

                elif check["check"] == "creditPeriodConsistency":
                    ex=[]
                    for i in check["data"]:
                       ex.append(str(i["column"][0]))
                    if check['status'] == "Fail" and map[0] in ex:
                        for i in  check["data"]:
                            if i["column"][0]==map[0]:
                                capture_log_message(f"creditPeriodConsistency Fail for {str(i['column'])}")
                                obj["checks"].append({str(check["check"])+'_FAIL':str(i["count"])+ " rows have failed the Credit Period Consistency Check.", 'rows': str(i["rows"])})
                    if check["status"] == "Pass" and map[0] in ex:
                        for i in  check["data"]:
                            if i["column"]==map[0]:
                                obj["checks"].append({str(check["check"])+'_PASS':"Credit Period Consistency Check Passed"})
            final.append(obj)
        final.append({"checks":[self.uniqueIdentifier]})
        return final

#test = dataErrorOutput()
#test.updateRequiredColumnPresent("pass", "Missing 1 columns", ["POSTING_DATE"])

#print(test.finalOuput())




   