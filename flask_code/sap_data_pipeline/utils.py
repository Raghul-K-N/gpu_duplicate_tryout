Z_BLOCK_EXPECTED_TABLES = ['BKPF','Z_BKPF','BSEG','WTH','T003','VIM_','VIMT100','VIMT101','1LOG_','8LOG_',
                   'APRLOG','EKKO','EKPO','LFA1','LFB1','LFM1','LFBK','T052U','T042Z','T053S','T001','UDC',
                    'DOAREDEL','VRDOA','1LOGCOMM','8LOGCOMM','RETINV','APMEMO']


EXPECTED_TABLES = ['BKPF','Z_BKPF','BSEG','WTH','T003','VIM_','VIMT100','VIMT101','1LOG_','8LOG_',
                   'APRLOG','EKKO','EKPO','LFA1','LFB1', 'LFM1','LFBK','T052U','T042Z','T053S','T001','UDC']  


# FILES_THAT_ARRIVE_AT_2_HOUR_FREQUENCY = ['BKPF','VIM_','1LOG_','8LOG_','APRLOG','1LOGCOMM','8LOGCOMM','RETINV']

# Master files - arrive only few times a day
# Almost all tables are considered master files since we are gonna process whatever arrives in each run and store as Parquet files
MASTER_FILES = ['BSEG','UDC','WTH','EKKO','EKPO','LFA1','LFB1','LFM1','LFBK','VRDOA','DOAREDEL','T001','T003','T042Z','T052U','T053S','VIMT100','VIMT101','APMEMO',
                'VIM_','1LOG_','8LOG_','APRLOG','1LOGCOMM','8LOGCOMM','RETINV','APMEMO']

# Tables that append new rows after duplicate row check
DIRECT_APPEND_TABES = ['VRDOA','T001','T003','T042Z','T053S','T052U','VIMT100','VIMT101','VIM_',
                       '1LOG_','8LOG_','APRLOG','1LOGCOMM','8LOGCOMM','RETINV','BSEG','WTH','UDC','BKPF','Z_BKPF']

SPECIAL_CASE_TABLE = ['LFBK']

REPLACE_OLD_ROW_WITH_NEW_ROW_TABLES = ['EKKO','EKPO','LFA1','LFB1','LFM1','DOAREDEL']