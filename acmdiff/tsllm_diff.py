import pandas as pd
import os
import re
import copy
from sqlalchemy import engine
import psycopg2
import openai
import functools
import signal
from ast import literal_eval
from utils.chat_utils import get_response, parse_el, parse_yn, parse_elv2
from utils.db_utils import PostgresAPI, views_are_equal
from acmdiff.diff_acms import diff_privs, \
                              privdiff_summary, \
                              rolediff_summary, \
                              viewdiff_summary, \
                              sqlvssql_views, \
                              sqlvssql_roles, \
                              nlvssql_roles, \
                              nlvsnl_roles
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception_type
)  # for exponential backoff

'''
Purpose: in this file, we difference ACMs by first using Jaccard similarity/SQL parsing to parse out DB literals,
and include only matches that contain the literals as options for the LLM to choose from.
'''

def get_groups(st, delim):
    groups = []
    found_start = False
    found_end = False
    cur_st = ''
    for ch in st:
        if found_start and not found_end and ch != delim:
            cur_st += ch
        elif found_start and not found_end and ch == delim:
            if cur_st == '':
                continue
            groups.append(cur_st)
            cur_st = ''
            found_end = True
        elif not found_start and ch == delim:
            found_start = True
        elif found_start and found_end and ch != delim:
            found_start = False
            found_end = False

    return groups

#TODO: I think the function get_groups above works and we don't need the below,
#but we may want to revisit it later.
# def extract_nested_quotes(st):
#     if '"' not in st and "'" not in st:
#         if 'CREATE VIEW' in st or 'SELECT' in st: #exclude nested queries
#             return []
#         return [st]
    
#     if '"' in st and "'" not in st:
#         st_ind = st.index('"')
#         end_ind = st.rindex('"')
#         if end_ind - st_ind < 2:
#             return []
#         else:
#             return extract_nested_quotes(st[st_ind : end_ind])
#     elif "'" in st and '"' not in st:
#         st_ind = st.index("'")
#         end_ind = st.rindex("'")
#         if end_ind - st_ind < 2:
#             return []
#         else:
#             return extract_nested_quotes(st[st_ind : end_ind])
#     elif '"' in st and "'" in st:
#         dq_st = st.index('"')
#         dq_en = st.rindex('"')
#         sq_st = st.index("'")
#         sq_en = st.rindex("'")
#         #figure out which one is inside, and which one is outside
#         if dq_st < sq_st and dq_en > sq_en:
#             return extract_nested_quotes(st[dq_st : dq_en])
#         elif sq_st < dq_st and sq_en > dq_en:
#             return extract_nested_quotes(st[sq_st : sq_en])
#         else: #single-quote and double-quote are different, and we should parse both

def find_floats(st):
    fpatt = r'\d+\.\d+'
    return re.findall(fpatt, st)

def find_ints(st):
    patt = r'\d+'
    return re.findall(patt, st)

def find_literals_from_st(st):
    lits = []
    fs = find_floats(st)
    ins = find_ints(st)
    
    #now, clean up the int list
    clean_ins = copy.deepcopy(ins)
    for i in range(len(ins) - 1):
        f_cand = ins[i] + '.' + ins[i + 1]
        if f_cand in fs:
            if ins[i] in clean_ins:
                clean_ins.remove(ins[i])
            if ins[i + 1] in clean_ins:
                clean_ins.remove(ins[i + 1])
    
    sts = get_groups(st, '"')
    sts += get_groups(st, "'")
    
    lits = fs + clean_ins + sts
    return lits

def find_literals_from_sql(query):
    return find_literals_from_st(query)

def find_literals_from_nl(sent):
    return find_literals_from_st(sent)

#GPT-4-generated
def extract_literals_from_sql(query):
    # Regular expression to match literals in SQL query
    # This regex matches integers, floating point numbers, and strings in single or double quotes
    regex = r"(\d+\.\d+|\d+|'[^']*'|\"[^\"]*\")"

    # Find all matches using the regex
    matches = re.findall(regex, query)

    # Processing matches to remove quotes from strings
    processed_matches = [match.strip("'\"") for match in matches]

    return processed_matches

def extract_literals_from_nl(sent):
    # Regular expression to match literals in SQL query
    # This regex matches integers, floating point numbers, and strings in single or double quotes
    regex = r"(\d+\.\d+|\d+|'[^']*'|\"[^\"]*\")"

    # Find all matches using the regex
    matches = re.findall(regex, sent)

    # Processing matches to remove quotes from strings
    processed_matches = [match.strip("'\"") for match in matches]

    return processed_matches



# Test the function with the provided example
# test_query = "select * from customer where temperature > 37.5"
# extract_literals_from_sql(test_query)

def nlvsnl_views(nldf1, nldf2, nl1_cols, nl2_cols, outdir, outname):
    views1 = [c for c in nldf1.columns.tolist() if c != 'Role']
    views2 = nl2_cols
    for i,v1 in enumerate(views1):
        if v1 not in nl1_cols:
            continue
        outfile = os.path.join(outdir, outname + '_nlvsnl_view' + str(i) + '.txt')
        outchat = os.path.join(outdir, outname + '_nlvsnl_view' + str(i) + '_chat.json')
        if os.path.exists(outfile) and os.path.exists(outchat):
            continue
        
        #first, check for string similarity.
        if v1 in views2:
            answer = (v1, 'Exact Match')
            with open(outfile, 'w+') as fh:
                print(answer, file=fh)
        else:
            #next, prune from the list and parse literals
            literal_lst = [find_literals_from_nl(sent) for sent in views2]
            v1lits = find_literals_from_nl(v1)
            print("Literals of v1 {}: {}".format(v1, v1lits))
            print("Literal List for views2 {}: {}".format(views2, literal_lst))
            if v1lits == []:
                matching_lits = [views2[i] for i,lits in enumerate(literal_lst) if lits == []]
            else:
                matching_lits = [views2[i] for i,lits in enumerate(literal_lst) if len(set(lits).intersection(set(v1lits))) > 0]
            
            if matching_lits == []:
                #then we should assume nothing about which sentences might match. Just use all of them.
                matching_lits = views2
            
            chat = [{'role' : 'system', 'content' : 'You are a helpful assistant.'}]
            prompt = 'Consider the following list of descriptions for tables and views in a database:\n\n' + str(matching_lits) + '\n\n'
            prompt += 'Consider the following phrase describing a table or view: ' + v1 + '. '
            prompt += 'Which database table or view description from the list most likely describes the same table or view as this phrase? Begin your answer with your chosen description from the list. If none of them match, begin your answer with None.'
            prompt += ' If you are unsure, make your best guess.'
            
            chat += [{'role' : 'user', 'content' : prompt}]
            raw_resp = get_response(chat, 0.0, write_dir=outdir, write_file=outname + '_nlvsnl_view' + str(i) + '_chat.json')
            chat += [{'role' : 'assistant', 'content' : raw_resp}]
            parsed = parse_elv2(raw_resp, views2, 'None')
            answer = (parsed, raw_resp)
            with open(outfile, 'w+') as fh:
                print(answer, file=fh)
            
            # with open(outchat, 'w+') as fh:
            #     print(chat, file=fh)

def sqlvsnl_views(nl_acmdf, sql_acmdf, lst_acmpath, nl_cols, sql_cols, outdir, outname):
    #TODO 1: this uses a hybrid generation-embedding approach. Would NL2SQL embeddings be useful for comparing NL and SQL here?
    #if so, the semantic similarity-only baseline would ideally only use this, and not the LLM.
    #TODO 2: (shorter-term) Semantic similarity may fail here, simply because the capacity of the model whose embedding we're using is small.
    #we'll need to try many different models (hopefully including GPT embeddings)
    acm_views = [c for c in nl_acmdf.columns.tolist() if c != 'Role']
    db_views = sql_cols
    
    for i, db_v in enumerate(db_views):
        if db_v not in sql_cols:
            continue
        outfile = os.path.join(outdir, outname + '_sqlvsnl_view' + str(i) + '.txt')
        outchat = os.path.join(outdir, outname + '_sqlvsnl_view' + str(i) + '_chat.json')
        if os.path.exists(outfile) and os.path.exists(outchat):
            continue
        
        #first, let's prune out NL not containing the same literals as the original
        literal_lst = [find_literals_from_nl(sent) for sent in acm_views]
        v1lits = find_literals_from_sql(db_v)
        if v1lits == []:
            matching_lits = [acm_views[i] for i,lits in enumerate(literal_lst) if lits == []]
        else:
            matching_lits = [acm_views[i] for i,lits in enumerate(literal_lst) if len(set(lits).intersection(set(v1lits))) > 0]
        
        chat = [{'role' : 'system', 'content' : 'You are a helpful assistant.'}]
        
        prompt1 = 'Consider the following SQL for a table or view: ' + db_v + '. '
        prompt1 += 'Explain what this query does in plain english.'
        chat += [{'role' : 'user', 'content' : prompt1}]
        raw_resp1 = get_response(chat, 0.0, write_dir=outdir, write_file=outname + '_sqlvsnl_view' + str(i) + '_chat.json')
        chat += [{'role' : 'assistant', 'content' : raw_resp1}]
        
        prompt2 = 'Consider the following list of descriptions of views in a database:\n\n' + str(matching_lits) + '\n\n'
        prompt2 += 'Which description from the list does the given query most likely describe? Begin your answer with this description. If none of them match, begin your answer with None.'
        prompt2 += ' If you are unsure, make your best guess.'
        
        chat += [{'role' : 'user', 'content' : prompt2}]
        raw_resp = get_response(chat, 0.0, write_dir=outdir, write_file=outname + '_sqlvsnl_view' + str(i) + '_chat.json')
        chat += [{'role' : 'assistant', 'content' : raw_resp}]
        parsed = parse_elv2(raw_resp, acm_views, 'None')
        print("Parsed Answer: {}".format(parsed))
        answer = (parsed, raw_resp, lst_acmpath, db_v)
        with open(outfile, 'w+') as fh:
            print(answer, file=fh)
        
        # with open(outchat, 'w+') as fh:
        #     print(chat, file=fh)

def diff_views(acmpath1, acmpath2, outdir, outname, db_details):
    acmdf1 = pd.read_csv(acmpath1)
    acmdf2 = pd.read_csv(acmpath2)
    cols1 = acmdf1.columns.tolist()
    cols2 = acmdf2.columns.tolist()
    
    pgapi = PostgresAPI(db_details)
    schema = pgapi.get_schema()
    sql1_cols = [c for c in cols1 if 'CREATE VIEW' in c or c in schema]
    sql2_cols = [c for c in cols2 if 'CREATE VIEW' in c or c in schema]
    nl1_cols = [c for c in cols1 if c not in sql1_cols and c != 'Role']
    nl2_cols = [c for c in cols2 if c not in sql2_cols and c != 'Role']
    
    #NL vs NL
    if nl1_cols != [] and nl2_cols != []:
        nlvsnl_views(acmdf1, acmdf2, nl1_cols, nl2_cols, outdir, outname)
    
    #NL vs SQL
    if nl1_cols != [] and sql2_cols != []:
        # nlvssql_views(acmdf1, acmdf2, nl1_cols, sql2_cols, outdir, outname)
        sqlvsnl_views(acmdf1, acmdf2, acmpath1, nl1_cols, sql2_cols, outdir, outname)
    
    #SQL vs NL
    if nl2_cols != [] and sql1_cols != []:
        # nlvssql_views(acmdf2, acmdf1, nl2_cols, sql1_cols, outdir, outname)
        sqlvsnl_views(acmdf2, acmdf1, acmpath2, nl2_cols, sql1_cols, outdir, outname)
    
    #SQL vs SQL
    if sql1_cols != [] and sql2_cols != []:
        sqlvssql_views(acmdf1, acmdf2, sql1_cols, sql2_cols, outdir, outname, db_details)

def diff_roles(acmpath1, acmpath2, outdir, outname, db_details):
    acmdf1 = pd.read_csv(acmpath1)
    acmdf2 = pd.read_csv(acmpath2)
    roles1 = acmdf1['Role'].tolist()
    roles2 = acmdf2['Role'].tolist()
    
    pgapi = PostgresAPI(db_details)
    privs = pgapi.get_privs()
    db_roles = [tup[0] for tup in privs]
    
    sql1_roles = [r1 for r1 in roles1 if r1 in db_roles or 'CREATE USER' in r1]
    sql2_roles = [r2 for r2 in roles2 if r2 in db_roles or 'CREATE USER' in r2]
    nl1_roles = [r1 for r1 in roles1 if r1 not in sql1_roles]
    nl2_roles = [r2 for r2 in roles2 if r2 not in sql2_roles]
    
    if nl1_roles != [] and nl2_roles != []:
        nlvsnl_roles(acmdf1, acmdf2, nl1_roles, nl2_roles, outdir, outname)
    
    if nl1_roles != [] and sql2_roles != []:
        nlvssql_roles(acmdf1, acmdf2, nl1_roles, sql2_roles, outdir, outname)
    
    if nl2_roles != [] and sql1_roles != []:
        nlvssql_roles(acmdf2, acmdf1, nl2_roles, sql1_roles, outdir, outname)
    
    if sql1_roles != [] and sql2_roles != []:
        sqlvssql_roles(acmdf1, acmdf2, sql1_roles, sql2_roles, outdir, outname, db_details)

def gen_completediff(acmpath1, acmpath2, indir, inname, outdir, outname, db_details):
    if not os.path.exists(indir):
        os.mkdir(indir)
    
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    
    diff_views(acmpath1, acmpath2, indir, inname, db_details)
    viewdiff_summary(acmpath1, acmpath2, indir, inname, db_details)
    
    diff_roles(acmpath1, acmpath2, indir, inname, db_details)
    rolediff_summary(acmpath1, acmpath2, indir, inname, db_details)
    
    diff_privs(acmpath1, acmpath2, indir, inname, outdir, outname)
    privdiff_summary(acmpath1, acmpath2, indir, inname, outdir, outname)