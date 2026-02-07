import re
import spacy
import pandas as pd
from tqdm import tqdm
from cleanco import basename
from rapidfuzz import process, fuzz


nlp = spacy.load('en_core_web_sm')
tqdm.pandas()


toexclude = ['instruments','instrument','logistics', 'systems', 'engineering',
             'recycling', 'laboratories', 'associates', 'consulting', 'technology', 
             'technologies', 'services', 'manufacturing', 'associates', 'industrial',
             'trucking', 'county', 'machine', 'industries', 'electronics', 'instrumentation',
             'electric', 'transportation', 'industries', 'services', 'systems', 'industries',
             'constructors', 'chemical', 'transport', 'iron & metal', 'communications', 'environmental',
             'instruments', 'environmental', 'processing', 'products', 'communications', 
             'international', 'manufacturing', 'supply', 'county tax assessor', 'construction', 
             'systems', 'recycling', 'fabricators', 'equipment', 'machine', 'county treasurer', 
             'industrial services', 'energy group', 'industrial solutions', 'group', 'companies', 
             'electronics', 'trucking', 'sales', 'logistics', 'international', 'instruments', 'instrument',
             'logistics', 'systems', 'engineering', 'recycling', 'laboratories', 'associates', 'consulting',
             'technology', 'technologies', 'services', 'manufacturing', 'associates', 'industrial', 
             'trucking', 'county', 'machine', 'industries', 'electronics', 'instrumentation', 'electric', 
             'transportation', 'industries', 'services', 'systems', 'industries', 'constructors', 'chemical',
             'transport', 'iron & metal', 'communications', 'environmental', 'instruments', 'environmental',
             'processing', 'products', 'communications', 'international', 'manufacturing',
             'supply', 'county tax assessor', 'construction', 'systems', 'recycling', 'fabricators',
             'equipment', 'machine', 'county treasurer', 'industrial services', 'energy group',
             'industrial solutions', 'group', 'companies', 'electronics', 'trucking', 'sales', 
             'logistics', 'international', 'services', 'instruments', 'consulting associates',
             'engineering services', 'strategies', 'engineered products', 'industrial services', 'water technologies'] 


def set_supplier_type(df, column):
    supplier_types = []
    for doc in tqdm(nlp.pipe(df[column].to_list(), \
                        disable=["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer"]), total=len(df)):
        supplier_type = 'Organisation'
        for ent in doc.ents:
            supplier_type = 'Person' if ent.label_ =='PERSON' else 'Organisation'
        supplier_types.append(supplier_type)
    df[column] = supplier_types
    return df


def word_match(str1,str2, threshold):
    str1 =str1.lower()
    str2 = str2.lower()
    str1 = re.sub('[^A-Za-z0-9]+', '',str1)
    str2 = re.sub('[^A-Za-z0-9]+', '',str2) 
   
    if(fuzz.ratio(str1,str2)>threshold):
        return True
    
    elif(len(str1)==0 or (len(str2)==0)):
        return False
    
    elif(len(str2)==1):
        if(str1[0]==str2):
            return True
        else:
            return False
            
    elif(len(str1)==1):
        if(str2[0]==str1):
            return True
        else:
            return False
    else:
        return False


def string_detection(str1, str2,threshold):
    str1 =str1.lower()
    str2 = str2.lower()
    list1 = str1.split()
    list2 = str2.split()
    len_list1 = len(list1)
    len_list2 = len(list2)
    min_len = min(len_list1,len_list2)
    if min_len == 1:
        if len(list1) == 1:
            if list1[0] in list2:
                return True
        elif list2[0] in list1:
            return True
        return False
    else:
        count = 0
        for w in list1:
            temp = list(list2)
            for string in temp:
                if word_match(w,string,threshold):
                    list2.remove(string)
                    count+=1
                    if(count >= min_len):
                        return True
                    break
        return False


def is_similar_suppliername(source, dest, source_sup_type, dest_sup_type, \
    threshold, high_threshold, exact_matching=False):
    if exact_matching:
        return source.lower()==dest.lower()
    reslt = []
    
    if (source_sup_type=='Person' and dest_sup_type=='Organisation')or (source_sup_type=='Organisation' and dest_sup_type=='Person'):
        return fuzz.token_set_ratio(source, dest) > high_threshold
        
    elif source_sup_type=='Organisation' and dest_sup_type=='Organisation':      
        for l in [source,dest]:
            name = basename(l)
            querywords = name.split()
            resultwords  = [word for word in querywords if word.lower() not in toexclude]
            result = ' '.join(resultwords)
            reslt.append(result)
        return fuzz.ratio(reslt[0],reslt[1]) > high_threshold
       
    else:
        return string_detection(source,dest,threshold)


def get_similar_suppliernames(supplier_names, keys, supplier_name_types, threshold, high_threshold, exact_matching=False):
    similars = []
    found = set()
    for i, (source, source_key,source_type) in enumerate(zip(supplier_names, keys,supplier_name_types)):
        if source_key in found:
            continue
        current_similars = set()
        for j, (dest, dest_key, dest_type) in enumerate(zip(supplier_names[i+1:], keys[i+1:],supplier_name_types[i+1:])):
            if dest_key in found:
                continue
            if is_similar_suppliername(source, dest, source_type, dest_type, threshold, high_threshold, \
                exact_matching=exact_matching):
                current_similars.add(source_key)
                current_similars.add(dest_key)
                found.add(source_key)
                found.add(dest_key)
        if len(current_similars):
            similars.append(current_similars)
    return similars



def duplicate_suppliername_similar(df, column, grouping_columns, identifier, \
    threshold=60, high_threshold=90, exact_matching=False):
    df = set_supplier_type(df, column)

    t = df.groupby(list(grouping_columns))[column, 'PrimaryKeySimple','SUPPLIER_TYPE'].agg(lambda x: list(x)).reset_index()

    dupl = t[t['PrimaryKeySimple'].apply(lambda x: len(x)>1)]

    dupl['DUPLICATES'] = dupl[[column, 'PrimaryKeySimple','SUPPLIER_TYPE']].progress_apply(lambda x: \
        get_similar_suppliernames(x[column], x['PrimaryKeySimple'], x['SUPPLIER_TYPE'], \
            threshold, high_threshold, exact_matching=exact_matching), axis=1)

    actual_dupl = dupl[dupl['DUPLICATES'].apply(lambda x: len(x)>0)]
    dupl_dict = dict()
    i = 0
    for l in actual_dupl['DUPLICATES'].to_list():  
        for k in l: 
            for item in list(k):
                dupl_dict[item] = i
            i += 1

    df[f'DUPLICATE_ID_{identifier}'] = df['PrimaryKeySimple'].map(dupl_dict)
    return df






