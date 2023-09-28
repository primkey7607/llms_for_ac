import pandas as pd
import os
from sqlalchemy import engine
import psycopg2
import openai
import signal
from ast import literal_eval
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception_type
)  # for exponential backoff

from c3_utils import c3_e2e

#TODO: for now, we'll hardcode it. But this needs to come from postgres.
def get_schema(db_details):
    #for now, let's just hardcode it
    return str(['supplier', 'customer', 'lineitem','region',
            'orders', 'partsupp', 'part', 'nation'])

class MyTimeoutException(Exception):
    pass

#register a handler for the timeout
def handler(signum, frame):
    print("Waited long enough!")
    raise MyTimeoutException("STOP")
    
@retry(retry=retry_if_exception_type(Exception), wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(6))
def get_response(chat, temp_val, timeout=30):
    
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=chat,
        temperature=temp_val
    )
    chat_response = response["choices"][0]["message"]["content"]
    print("Received response: {}".format(chat_response))
    return chat_response

def parse_lst(lst_resp):
    #chatgpt will delimit code by three apostrophes
    query_st = lst_resp
    if '```' in query_st:
        query_parts = query_st.split('```')
        queries = [qp for i,qp in enumerate(query_parts) if i % 2 == 1]
        out_st = queries[0]
    else:
        out_st = query_st
    out_st = out_st[out_st.index('[') : out_st.index(']') + 1]
    out_st.replace('\n', '')
    out_st.replace('\r', '')
    out_st.replace(' ', '')
    out_lst = literal_eval(out_st)
    return out_lst

def parse_privs(lst_resp : str):
    raw_lst = parse_lst(lst_resp)
    if 'UPDATE' in raw_lst and 'INSERT' in raw_lst:
        raw_lst.append('UPDATE and INSERT')
        raw_lst.remove('UPDATE')
        raw_lst.remove('INSERT')
    
    return raw_lst

def retry_chat(chat, lst_resp, attempts=5):
    new_chat = chat + [{'role' : 'assistant', 'content' : lst_resp}]
    for i in range(attempts):
        new_sent = 'You did not provide a python list of database operations in your response. Please provide your response as a python list.'
        new_sent += ' If you feel it is unclear, make your best guess.'
        new_chat += [{'role' : 'user', 'content' : new_sent}]
        new_resp = get_response(new_chat, 0.0)
        if '[' in new_resp and ']' in new_resp:
            return new_resp
    
    #otherwise, we'll be extra conservative, and not give any privileges
    print("Failed to Acquire parseable response")
    return '[]'

def t1sent_to_t0(sentence, role, view, role_tok, view_tok):
    chat = [{'role' : 'system', 'content' : 'You are a helpful assistant.'}]
    reduction = 'Consider the following natural language statement:\n'
    reduction += '"' + sentence + '"\n\n'
    reduction += 'According to this, which of the database operations SELECT, UPDATE/INSERT, DELETE, CREATE, GRANT are permitted '
    reduction += 'for ' + role_tok + ' ' + role + ' on ' + view_tok + ' ' + view + '?'
    followup = ' If you are unsure, make your best guess.'
    reduction += followup
    chat += [{'role' : 'user', 'content' : reduction}]
    init_resp = get_response(chat, 0.0)
    
    chat += [{'role' : 'assistant', 'content' : init_resp}]
    chat += [{'role' : 'user', 'content' : 'List the permitted operations as a python list.'}]
    lst_resp = get_response(chat, 0.0)
    if '[' not in lst_resp or ']' not in lst_resp:
        lst_resp = retry_chat(chat, lst_resp)
    privs = parse_privs(lst_resp)
    
    return privs
    
def user_to_nl(user_st):
    if 'IN ROLE' not in user_st:
        u_parts = user_st.split(' ')
        name = u_parts[-1][:-1]
        user_nl = 'Create user ' + name + '.'
    else:
        u_parts = user_st.split(' ')
        name = u_parts[2]
        role = u_parts[-1][:-1] #exclude the semicolon
        
        user_nl = 'Create user ' + name + ' in the role ' + role + '.'
    
    return user_nl, name

def view_to_nl(view_st):
    v_parts = view_st.split(' ')
    name = v_parts[2]
    cols = []
    tbl_name = v_parts[-1][:-1]
    start = False
    end = False
    for part in v_parts:
        if part == 'SELECT':
            start = True
        elif part == 'FROM':
            end = True
        elif start and not end:
            cols.append(part)
        elif end:
            break
    
    col_st = ', '.join(cols)
    view_nl = 'Create a view of the ' + tbl_name + ' table called ' + name + ' with columns ' + col_st + '.'
    return view_nl, name

def tp1totp0(tp1acmpath, outname, max_rows=None, max_cols=None):
    df = pd.read_csv(tp1acmpath)
    new_dct = {}
    for c in df.columns:
        new_dct[c] = []
    row_cnt = 0
    for row in df.to_dict(orient='records'):
        if max_rows != None and row_cnt >= max_rows:
            continue
        
        col_cnt = 0
        for view in row:
            outfile = outname + '_row' + str(row_cnt) + '_col' + str(col_cnt)
            if max_cols != None and col_cnt >= max_cols:
                continue
            
            if os.path.exists(outfile + '.txt'):
                col_cnt += 1
                continue
            
            if view == 'Role':
                continue
            
            tp1privs = row[view]
            if pd.isna(tp1privs):
                col_cnt += 1
                continue
            
            user_nl = ''
            view_nl = ''
            is_user = False
            is_view = False
            if 'CREATE USER' in row['Role']:
                user_nl, user_name = user_to_nl(row['Role'])
                is_user = True
            else:
                user_name = row['Role']
            
            if 'CREATE VIEW' in view:
                view_nl, view_name = view_to_nl(view)
                is_view = True
            else:
                view_name = view
            
            role_tok = 'user' if is_user else 'role'
            view_tok = 'view' if is_view else 'table'
            
            tp0privs = t1sent_to_t0(tp1privs, user_name, view_name, role_tok, view_tok)
            tp0priv_st = ', '.join(tp0privs)
            
            with open(outfile + '.txt', 'w+') as fh:
                print([tp0priv_st], file=fh)
            
            col_cnt += 1
        
        row_cnt += 1

def reconstruct_type0(tp1_path, new_name):
    #we also want to store the mappings of privileges to type 1 sentences.
    #this will help later.
    acm_path = tp1_path
    df = pd.read_csv(acm_path)
    new_dct = {}
    new_dct['Role'] = df['Role'].tolist()
    for c in df.columns:
        if c != 'Role':
            new_dct[c] = [None] * len(new_dct['Role'])
    
    row_cnt = 0
    tp1totp0_gt = {}
    for row in df.to_dict(orient='records'):
        col_cnt = 0
        for view in row:
            if view == 'Role':
                continue
            outfile = new_name + '_row' + str(row_cnt) + '_col' + str(col_cnt) + '.txt'
            if os.path.exists(outfile):
                with open(outfile, 'r') as fh:
                    lst = literal_eval(fh.read())
                
                new_dct[view][row_cnt] = lst[0]
                tp1totp0_gt[lst[0]] = row[view]
            
            col_cnt += 1
        
        row_cnt += 1
    
    print(new_dct)
    new_df = pd.DataFrame(new_dct)
    new_df.to_csv(new_name + '.csv', index=False)
    with open(new_name + '_tp0totp1.json', 'w+') as fh:
        print(tp1totp0_gt, file=fh)


def tp0tosentences(tp0acmpath):
    df = pd.read_csv(tp0acmpath)
    sentences = []
    
    for row in df.to_dict(orient='records'):
        cur_role = row['Role']
        user_nl = ''
        user_name = None
        if 'CREATE USER' in cur_role:
            user_nl, user_name = user_to_nl(cur_role)
        if user_nl != '':
            sentences.append(user_nl)
            
        for view in row:
            if pd.isna(row[view]):
                continue
            if view == 'Role':
                continue
            view_nl = ''
            view_name = None
            if 'CREATE VIEW' in view:
                view_nl, view_name = view_to_nl(view)
            
            user_tok = cur_role if user_name == None else user_name
            view_tok = view if view_name == None else view_name
            usr_ind = 'role' if user_name == None else 'user'
            v_ind = 'table' if view_name == None else 'view'
            privs = row[view]
            tp0_sent = 'The ' + usr_ind + ' ' + user_tok + ' has ' + privs + ' access to ' + v_ind + ' ' + view_tok + ' with the option of passing on this privilege.'
            
            if view_nl != '':
                sentences.append(view_nl)
            
            sentences.append(tp0_sent)
    
    return sentences

#use C3 to generate sentences
def gen_tp0sql(tp0acmpath, schema):
    #TODO: this method heavily reuses code from data generation in type1acm.py.
    #we may want to refactor this.
    sentences = tp0tosentences(tp0acmpath)
    print(sentences)
    
    #generate the SQL files on disk for these sentences by running c3 end-to-end
    #this will return a dictionary of each sentence to its query
    nl2sql = c3_e2e(sentences, outdir='c3tp1_raw', inprefix='c3tp1_', outprefix='c3tp1_complete', compdir='c3tp1complete', sql_prefix='c3sql_tp1', sqldir='c3tp1sql', dctname='c3tp1_nl2sql.json')
    
    return nl2sql

if __name__=='__main__':
    tpch_schema = get_schema({})
    # gen_tp0sql('acm_datagen/dacview_test_type0acm.csv', tpch_schema)
    # sentences = tp0tosentences('acm_datagen/dacview_test_type0acm.csv')
    # with open('type0acm_allsentences.txt', 'w+') as fh:
    #     print(sentences, file=fh)
    # gen_tp0sql('acm_datagen/dacview_test_type0acm.csv', tpch_schema)
    # tp1totp0('acm_datagen/dacview_test_type1acm.csv', 'dacview_tp1')
    # reconstruct_type0('acm_datagen/dacview_test_type1acm.csv', 'dacview_tp1')
    gen_tp0sql('dacview_tp1.csv', tpch_schema)
    
    
    
    

