import pandas as pd
import os
from sqlalchemy import engine
import psycopg2
import openai
import signal
from ast import literal_eval
from utils.chat_utils import get_response, parse_el, parse_yn
from utils.db_utils import PostgresAPI
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception_type
)  # for exponential backoff

def sql_to_type0(db_details : dict, outname : str):
    priv_query = 'SELECT grantee, privilege_type, table_name FROM information_schema.role_table_grants where table_schema = \'public\''
    pgapi = PostgresAPI(db_details)
    db_schema = pgapi.get_schema()
    priv_tups = pgapi.query(priv_query)
    out_dct = {}
    all_cols = []
    #first, get view definitions for each view
    tbls = set([tup[2] for tup in priv_tups])
    for tbl in tbls:
        if tbl in db_schema.keys():
            all_cols.append(tbl)
        else:
            view_def = 'select pg_get_viewdef(\'' + tbl + '\', true)'
            def_tups = pgapi.query(view_def)
            vdef = [tup[0] for tup in def_tups][0]
            full_def = 'CREATE VIEW ' + tbl + ' AS ' + vdef
            full_def = '"' + full_def + '"'
            all_cols.append(full_def)
    
    #now, fill in the dict
    out_schema = ['Role'] + list(set(all_cols))
    for o in out_schema:
        out_dct[o] = []
    
    role_dct = {}
    for tup in priv_tups:
        if tup[0] == 'postgres' or tup[2] in ['TRIGGER', 'REFERENCES', 'TRUNCATE']:
            continue
        if tup[0] in role_dct:
            if tup[2] in role_dct[tup[0]]:
                role_dct[tup[0]][tup[2]].append(tup[1])
            else:
                role_dct[tup[0]][tup[2]] = [tup[1]]
        else:
            role_dct[tup[0]] = {}
            role_dct[tup[0]][tup[2]] = [tup[1]]
    
    #fill in missing entries
    for role in role_dct:
        missing = [tbl for tbl in role_dct[role] if tbl not in all_cols]
        for m in missing:
            role_dct[role][m] = []
    
    #construct entries
    role_ents = []
    for role in role_dct:
        new_ent = {}
        new_ent['Role'] = role
        for tbl in role_dct[role]:
            new_ent[tbl] = role_dct[role][tbl]
        
        role_ents.append(new_ent)
    
    out_df = pd.DataFrame.from_records(role_ents)
    out_df.to_csv(outname + '_type0.csv', index=False)

def compare_type0roles(type0df, acmdf, outdir : str, outname : str):
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    db_roles = type0df['Role'].tolist()
    acm_roles = acmdf['Role'].tolist()
    for i,acm_r in enumerate(acm_roles):
        outfile = os.path.join(outdir, outname + '_role' + str(i) + '.txt')
        if os.path.exists(outfile):
            continue
        chat = [{'role' : 'system', 'content' : 'You are a helpful assistant.'}]
        prompt = 'Consider the following list of roles in a database:\n\n' + str(db_roles) + '\n\n'
        prompt += 'Consider the following phrase describing a role: ' + acm_r + '. '
        prompt += 'Which database role from the list does this phrase most likely describe? Begin your answer with this role. If none of them match, begin your answer with None.'
        prompt += ' If you are unsure, make your best guess.'
        
        chat += [{'role' : 'user', 'content' : prompt}]
        raw_resp = get_response(chat, 0.0)
        parsed = parse_el(raw_resp, db_roles, 'None')
        answer = (parsed, raw_resp)
        with open(outfile, 'w+') as fh:
            print(answer, file=fh)

def compare_type0views(type0df, acmdf, outdir : str, outname : str):
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    db_views = [c for c in type0df.columns if c != 'Role']
    acm_views = [c for c in acmdf.columns if c != 'Role']
    for i,acm_v in enumerate(acm_views):
        outfile = os.path.join(outdir, outname + '_view' + str(i) + '.txt')
        if os.path.exists(outfile):
            continue
        chat = [{'role' : 'system', 'content' : 'You are a helpful assistant.'}]
        prompt = 'Consider the following list of tables and views in a database:\n\n' + str(db_views) + '\n\n'
        prompt += 'Consider the following phrase describing a table or view: ' + acm_v + '. '
        prompt += 'Which database table or view from the list does this phrase most likely describe? Begin your answer with this table/view. If none of them match, begin your answer with None.'
        prompt += ' If you are unsure, make your best guess.'
        
        chat += [{'role' : 'user', 'content' : prompt}]
        raw_resp = get_response(chat, 0.0)
        parsed = parse_el(raw_resp, db_views, 'None')
        answer = (parsed, raw_resp)
        with open(outfile, 'w+') as fh:
            print(answer, file=fh)

def compare_type0privs(type0df, acmdf, in_dir, in_pref : str, outdir, outname):
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    # before running this: we must have checked which views and roles are the same between
    #type0df and acmdf
    acm_cols = []
    acm_roles = []
    v_cnt = 0
    while os.path.exists(in_dir + os.sep + in_pref + '_view' + str(v_cnt) + '.txt'):
        inname = in_dir + os.sep + in_pref + '_view' + str(v_cnt) + '.txt'
        with open(inname, 'r') as fh:
            st = fh.read()
        view_st = st.replace('\n', '')
        acm_cols.append(view_st)
        v_cnt += 1
    
    r_cnt = 0
    while os.path.exists(in_dir + os.sep + in_pref + '_role' + str(r_cnt) + '.txt'):
        inname = in_dir + os.sep + in_pref + '_role' + str(r_cnt) + '.txt'
        with open(inname, 'r') as fh:
            st = fh.read()
        role_st = st.replace('\n', '')
        acm_roles.append(role_st)
        r_cnt += 1
    
    tp0_roles = type0df['Role'].tolist()
    tp0_views = type0df.columns.tolist()
    
    shr_roles = set(tp0_roles).intersection(set(acm_roles))
    shr_views = set(tp0_views).intersection(set(acm_cols))
    
    for i,role in enumerate(shr_roles):
        acm_row = acmdf[acmdf['Role'] == role].todict(orient='records')[0]
        tp0_row = type0df[type0df['Role'] == role].todict(orient='records')[0]
        for j,view in enumerate(shr_views):
            outfile = os.path.join(outdir, outname + '_role' + str(i) + '_view' + str(j) + '.txt')
            if os.path.exists(outfile):
                continue
            acm_ent = acm_row[view]
            tp0_ent = tp0_row[view]
            chat = [{'role' : 'system', 'content' : 'You are a helpful assistant.'}]
            prompt = 'Consider the following description of privileges for role ' + role + ' on a table/view ' + view + ':\n\n' + acm_ent + '\n\n'
            prompt += 'Is the following list of database privileges sufficient for assigning the privileges mentioned in the description? ' + str(tp0_ent) + '. '
            prompt += 'Begin your answer with YES or NO, and then explain.'
            prompt += ' If you are unsure, make your best guess.'
            
            chat += [{'role' : 'user', 'content' : prompt}]
            raw_resp = get_response(chat, 0.0)
            answer = parse_yn(raw_resp)
            out_tup = (answer, raw_resp)
            with open(outfile, 'w+') as fh:
                print(out_tup, file=fh)
    



