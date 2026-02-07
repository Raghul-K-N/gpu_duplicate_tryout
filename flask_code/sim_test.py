from rapidfuzz.distance import Levenshtein
SCORE_THRESOLD = 0

from duplicate_invoices.model.duplicate_extract_helper import is_invoice_similar
from invoice_number_similarity.rule_based_model import rule_based_similarity
from invoice_number_similarity.predict import make_prediction

# INV-001 and INV-001 - Catboost 0.87
# INV-001 and INV-001A - Catboost 0.98


for each in [("INV-1234","INV-1295"),("INV-120034","INV-120095"),("INV-001","INV-002"),('INV-001','INV-001A'),("BTEC-001","BTEC-01"),('1234','1245'),('1234','1235')]:

    invoice1 = each[0]
    invoice2 = each[1]

    old_rule_based = rule_based_similarity(invoice1,invoice2)

    old_catboost_model_based = make_prediction(input_data=(invoice1,invoice2),model="ML")
    new_rule_based  = is_invoice_similar(invoice1,invoice2)

    print(each)
    print('OLD RULE BASED:',old_rule_based[0])

    print('OLD CATBOOST:',old_catboost_model_based['predictions'][1])

    print("NEW RULE BASED:",new_rule_based)
    
    print('\n')