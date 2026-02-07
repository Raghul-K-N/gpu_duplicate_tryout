from rapidfuzz.distance import Levenshtein
from duplicate_invoices.config import config

def get_diff(a,b):
    l = Levenshtein.editops(a,b)
    diff_plus = []
    diff_minus = []
    # diff_replace = []
    first_diff_pos = -1
    first_end_pos = -1
    last_pos = -1
    for i,op in enumerate(l):
        if i==0:
            first_diff_pos = min(op.src_pos, op.dest_pos)
            first_end_pos = first_diff_pos
        if op.tag == 'insert':
            diff_plus.append(b[op.dest_pos])
        elif op.tag == 'delete':
            diff_minus.append(a[op.src_pos])
        elif op.tag == 'replace':
            diff_plus.append(b[op.dest_pos])
            diff_minus.append(a[op.src_pos])
        last_pos = max(op.src_pos, op.dest_pos)
        if last_pos == first_end_pos + 1:
            first_end_pos += 1
    return diff_minus, diff_plus, len(l), first_diff_pos, first_end_pos, last_pos



def rule_based_similarity(source, dest):
    threshold = (config.THRESHOLD_VALUE)*100
    max_distance = 4
    diff_minus, diff_plus, distance, first_diff_pos, first_end_pos, last_pos = get_diff(source, dest)

    if distance == 0:
        return 1, 1

    first_diff_is_num = False

    if len(diff_minus):
        first_diff_is_num = diff_minus[0] in set("0123456789")
    if len(diff_plus):
        first_diff_is_num = diff_plus[0] in set("0123456789")

    length = max(len(source), len(dest))
    score = (length - distance)*100/length

    set_diff = set(diff_plus).union(set(diff_minus))
    set_diff_no_numbers = set_diff - set("0123456789")

    only_diff_is_numbers = distance>0 and not len(set_diff_no_numbers)

    if only_diff_is_numbers:
        if first_diff_pos == 0 and last_pos == first_end_pos and first_end_pos<3:
            return score/100, score
        else:
            return 0, score
    else:
        # if score > threshold and not first_diff_is_num:
        if (score > threshold or distance <= max_distance) and not first_diff_is_num:
            return score/100, score
        else:
            return 0, score