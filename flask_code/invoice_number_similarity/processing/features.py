from invoice_number_similarity.config import logging_config, config
import logging
from rapidfuzz.distance import Levenshtein
import pandas as pd

_logger = logging.getLogger(__name__)
file_handler = logging.FileHandler(config.LOGS_DIR / f"{__name__}.txt")
formatter = logging_config.FORMATTER
file_handler.setFormatter(formatter)
_logger.addHandler(file_handler)


def char_type(c):
    """Returns the character type, whether alphabet, number or special"""
    if c == '':
        return 'empty'
    o = ord(c)
    if o < 65:
        if o < 48:
            return 'special'
        elif o <= 57:
            return 'number'
        return 'special'
    elif o <= 90:
        return 'alphabet'
    elif o <= 122:
        if o < 97:
            return 'special'
        return 'alphabet'
    return 'special'


def string_type(src):
    if len(src) == 0:
        return "empty"
    types = [char_type(c) for c in src]
    return ','.join(types)


def extract_features(src, dest, t=2):
    l = Levenshtein.editops(src, dest)

    features = dict()
    features['length_src'] = len(src)
    features['length_dest'] = len(dest)
    features['ratio_src_dest'] = features['length_src']/features['length_dest']

    current_cluster = 0
    current_src_cluster = 0
    current_dest_cluster = 0
    previous_pos = 0
    previous_src_pos = 0
    previous_dest_pos = 0

    diff_plus, diff_minus = [], []

    for i,op in enumerate(l):
        features[f'op_{i}'] = op.tag
        current_pos = max(op.src_pos, op.dest_pos)

        if op.tag == 'insert':
            diff_plus.append(dest[op.dest_pos])

            features[f'src_char_{i}'] = ''
            features[f'src_char_type_{i}'] = 'empty'

            dest_char = dest[op.dest_pos]
            features[f'dest_char_{i}'] = dest_char
            features[f'dest_char_type_{i}'] = char_type(dest_char)
        elif op.tag == 'delete':
            src_char = src[op.src_pos]
            features[f'src_char_{i}'] = src_char
            features[f'src_char_type_{i}'] = char_type(src_char)

            features[f'dest_char_{i}'] = ''
            features[f'dest_char_type_{i}'] = 'empty'

            diff_minus.append(src[op.src_pos])
        elif op.tag == 'replace':
            src_char = src[op.src_pos]
            features[f'src_char_{i}'] = src_char
            features[f'src_char_type_{i}'] = char_type(src_char)

            dest_char = dest[op.dest_pos]
            features[f'dest_char_{i}'] = dest_char
            features[f'dest_char_type_{i}'] = char_type(dest_char)
            diff_plus.append(dest[op.dest_pos])
            diff_minus.append(src[op.src_pos])


        if i==0:
            features['cluster_0_start'] = current_pos
            features[f'cluster_0_end'] = current_pos
            features[f'src_cluster_{current_src_cluster}_start'] = op.src_pos
            features[f'src_cluster_{current_src_cluster}_end'] = op.src_pos
            features[f'dest_cluster_{current_dest_cluster}_start'] = op.dest_pos
            features[f'dest_cluster_{current_dest_cluster}_end'] = op.dest_pos
            previous_src_pos = op.src_pos-1
            previous_dest_pos = op.dest_pos - 1
            previous_pos = current_pos-1

            if op.src_pos >= t:
                features[f'src_cluster_{current_src_cluster}_prev_{t}'] = src[op.src_pos-t:op.src_pos]
            else:
                features[f'src_cluster_{current_src_cluster}_prev_{t}'] = src[:op.src_pos]

            if op.dest_pos >= t:
                features[f'dest_cluster_{current_dest_cluster}_prev_{t}'] = dest[op.dest_pos-t:op.dest_pos]
            else:
                features[f'dest_cluster_{current_dest_cluster}_prev_{t}'] = dest[:op.dest_pos]

        features[f'src_pos_{i}'] = op.src_pos
        features[f'dest_pos_{i}'] = op.dest_pos

        
        if current_pos == previous_pos + 1:
            features[f'cluster_{current_cluster}_end'] = current_pos
        else:
            current_cluster += 1
            features[f'cluster_{current_cluster}_start'] = current_pos
            features[f'cluster_{current_cluster}_end'] = current_pos

        if op.src_pos == previous_src_pos + 1 or op.src_pos == previous_src_pos:
            features[f'src_cluster_{current_src_cluster}_end'] = op.src_pos
        else:
            current_src_cluster += 1
            features[f'src_cluster_{current_src_cluster}_start'] = op.src_pos
            features[f'src_cluster_{current_src_cluster}_end'] = op.src_pos
            if op.src_pos >= t:
                features[f'src_cluster_{current_src_cluster}_prev_{t}'] = src[op.src_pos-t:op.src_pos]
            else:
                features[f'src_cluster_{current_src_cluster}_prev_{t}'] = src[:op.src_pos]

            if op.tag == 'insert':
                features[f'len_src_cluster_{current_src_cluster-1}'] = 0
            else:
                features[f'len_src_cluster_{current_src_cluster-1}'] = \
                        features[f'src_cluster_{current_src_cluster-1}_end'] - \
                        features[f'src_cluster_{current_src_cluster-1}_start']

        if op.dest_pos == previous_dest_pos + 1 or op.dest_pos == previous_dest_pos:
            features[f'dest_cluster_{current_dest_cluster}_end'] = op.dest_pos
        else:
            current_dest_cluster += 1
            features[f'dest_cluster_{current_dest_cluster}_start'] = op.dest_pos
            features[f'dest_cluster_{current_dest_cluster}_end'] = op.dest_pos
            if op.dest_pos >= t:
                features[f'dest_cluster_{current_dest_cluster}_prev_{t}'] = dest[op.dest_pos-t:op.dest_pos]
            else:
                features[f'dest_cluster_{current_dest_cluster}_prev_{t}'] = dest[:op.dest_pos]

            if op.tag == 'delete':
                features[f'len_dest_cluster_{current_dest_cluster-1}'] = \
                        features[f'dest_cluster_{current_dest_cluster-1}_end'] - \
                        features[f'dest_cluster_{current_dest_cluster-1}_start']

        if op.dest_pos + t < len(dest):
            features[f'dest_cluster_{current_dest_cluster}_next_{t}'] = dest[op.dest_pos+1: op.dest_pos+t+1]
        else:
            features[f'dest_cluster_{current_dest_cluster}_next_{t}'] = dest[op.dest_pos+1:]

        if op.src_pos + t < len(src):
            features[f'src_cluster_{current_src_cluster}_next_{t}'] = src[op.src_pos+1: op.src_pos+t+1]
        else:
            features[f'src_cluster_{current_src_cluster}_next_{t}'] = src[op.src_pos+1:]

        features[f'src_cluster_{current_src_cluster}_vicinity_alpha'] = any([s.isalpha()\
             for s in set(features[f'src_cluster_{current_src_cluster}_next_{t}']+ features[f'src_cluster_{current_src_cluster}_prev_{t}'])])
        features[f'dest_cluster_{current_dest_cluster}_vicinity_alpha'] = any([s.isalpha()\
             for s in set(features[f'dest_cluster_{current_dest_cluster}_next_{t}']+ features[f'dest_cluster_{current_dest_cluster}_prev_{t}'])])
        features[f'src_cluster_{current_src_cluster}_vicinity_special'] = any([not s.isalnum()\
             for s in set(features[f'src_cluster_{current_src_cluster}_next_{t}']+ features[f'src_cluster_{current_src_cluster}_prev_{t}'])])
        features[f'dest_cluster_{current_dest_cluster}_vicinity_special'] = any([not s.isalnum()\
             for s in set(features[f'dest_cluster_{current_dest_cluster}_next_{t}']+ features[f'dest_cluster_{current_dest_cluster}_prev_{t}'])])
        features[f'src_cluster_{current_src_cluster}_vicinity_special_minus_hyphen'] = any([(not s.isalnum() and s!='-')\
             for s in set(features[f'src_cluster_{current_src_cluster}_next_{t}']+ features[f'src_cluster_{current_src_cluster}_prev_{t}'])])
        features[f'dest_cluster_{current_dest_cluster}_vicinity_special_minus_hyphen'] = any([(not s.isalnum() and s!='-')\
             for s in set(features[f'dest_cluster_{current_dest_cluster}_next_{t}']+ features[f'dest_cluster_{current_dest_cluster}_prev_{t}'])])
        
        features[f'src_cluster_{current_src_cluster}_prev_type'] = \
            string_type(features[f'src_cluster_{current_src_cluster}_prev_{t}'])
        features[f'dest_cluster_{current_dest_cluster}_prev_type'] = \
            string_type(features[f'dest_cluster_{current_dest_cluster}_prev_{t}'])

        features[f'src_cluster_{current_src_cluster}_next_type'] = \
            string_type(features[f'src_cluster_{current_src_cluster}_next_{t}'])
        features[f'dest_cluster_{current_dest_cluster}_next_type'] = \
            string_type(features[f'dest_cluster_{current_dest_cluster}_next_{t}'])
        


        features[f'delta_src_pos_{i}'] = op.src_pos - previous_src_pos
        features[f'delta_dest_pos_{i}'] = op.dest_pos - previous_dest_pos
        
        previous_pos = current_pos
        previous_src_pos = op.src_pos
        previous_dest_pos = op.dest_pos
    
    if current_src_cluster >= 1:
        features[f'len_src_cluster_{current_src_cluster}'] = \
                    features[f'src_cluster_{current_src_cluster}_end'] - \
                    features[f'src_cluster_{current_src_cluster}_start']
    if current_dest_cluster >= 1:
        features[f'len_dest_cluster_{current_dest_cluster}'] = \
                    features[f'dest_cluster_{current_dest_cluster}_end'] - \
                    features[f'dest_cluster_{current_dest_cluster}_start']


    features['num_src_clusters'] = current_src_cluster
    features['num_dest_clusters'] = current_dest_cluster


    features['distance'] = len(l)
    length = max(len(src), len(dest))
    score = (length - features['distance'])*100/length
    features['score'] = score

    set_diff = set(diff_plus).union(set(diff_minus))
    set_diff_no_numbers = set_diff - set("0123456789")

    features['only_diff_is_numbers'] = features['distance']>0 and not len(set_diff_no_numbers)


    features['first_diff_is_num'] = False

    if len(diff_minus):
        features['first_diff_is_num'] = diff_minus[0] in set("0123456789")
    if len(diff_plus):
        features['first_diff_is_num'] = diff_plus[0] in set("0123456789")

    return features

