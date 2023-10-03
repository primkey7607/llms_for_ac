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

#Precondition: I am assuming there is an OpenAI token for API usage stored
#in the OpenAI environment variable already.

class MyTimeoutException(Exception):
    pass

#register a handler for the timeout
def handler(signum, frame):
    print("Waited long enough!")
    raise MyTimeoutException("STOP")
    
def get_schema(db_details):
    #for now, let's just hardcode it
    return str(['supplier', 'customer', 'lineitem','region',
            'orders', 'partsupp', 'part', 'nation'])

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

def kw_syn(sentence):
    all_privs = ['SELECT', 'UPDATE and INSERT', 'DELETE', 'CREATE']
    privs = [p for p in all_privs if p in sentence]
    priv_st = ', '.join(privs)
    req = 'Consider the following sentence:\n\n'
    req += sentence
    req += 'Give 5 rephrasings of this sentence where the keyword indicators ' + priv_st + ' are replaced with synonyms.'
    chat = [{'role' : 'system', 'content' : 'You are a helpful assistant.'}]
    chat += [{'role' : 'user', 'content' : req}]
    init_resp = get_response(chat, 0.0)
    
    chat += [{'role' : 'assistant', 'content' : init_resp}]
    followup = 'List the sentences as a python list.'
    chat += [{'role' : 'user', 'content' : followup}]
    lst_resp = get_response(chat, 0.0)
    
    sent_lst = parse_lst(lst_resp)
    
    return sent_lst

def kw_inf(sentence):
    all_privs = ['SELECT', 'UPDATE and INSERT', 'DELETE', 'CREATE']
    privs = [p for p in all_privs if p in sentence]
    priv_st = ', '.join(privs)
    req = 'Consider the following sentence:\n\n'
    req += sentence
    req += 'Give 5 rephrasings of this sentence where the keyword indicators ' + priv_st + ' are implied by carrier phrases. '
    req += 'For example, replace "SELECT access on the view internview0" with "the ability to analyze the data contained in internview0".'
    chat = [{'role' : 'system', 'content' : 'You are a helpful assistant.'}]
    chat += [{'role' : 'user', 'content' : req}]
    init_resp = get_response(chat, 0.0)
    
    chat += [{'role' : 'assistant', 'content' : init_resp}]
    followup = 'List the sentences as a python list.'
    chat += [{'role' : 'user', 'content' : followup}]
    lst_resp = get_response(chat, 0.0)
    
    sent_lst = parse_lst(lst_resp)
    
    return sent_lst

def perturb_tp0(sentence, perturb_types):
    out_sents = []
    for pt in perturb_types:
        if pt == 'kw_syn':
            new_sents = kw_syn(sentence)
            out_sents += new_sents
        elif pt == 'kw_inf':
            new_sents = kw_inf(sentence)
            out_sents += new_sents
            
    
    return out_sents
        
    

#given the path to a type 0 ACM, translate it to a type 1 ACM.
#in this case, the meanings of rows and columns stay the same, but the 
# cells become natural language.
#we generate natural language using natural language perturbations described
#in Dr. Spider, here: https://openreview.net/forum?id=Wc5bmZZU9cy
def type0totype1(acm_path, perturb_types, outname, max_rows=None, max_cols=None):
    df = pd.read_csv(acm_path)
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
            
            privs = row[view]
            if pd.isna(privs):
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
            
            tp0_sent = 'The ' + role_tok + ' ' + user_name + ' has ' + privs + ' access to ' + view_tok + ' ' + view_name + ' with the option of passing on this privilege.'
            tp1_sents = perturb_tp0(tp0_sent, perturb_types)
            with open(outfile + '.txt', 'w+') as fh:
                print(tp1_sents, file=fh)
            
            col_cnt += 1
        
        row_cnt += 1
            # print(tp1_sents)

def construct_type1(acm_path, lst_pos, new_name):
    #we also want to store the mappings of privileges to type 1 sentences.
    #this will help later.
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
                
                new_dct[view][row_cnt] = lst[lst_pos]
                tp1totp0_gt[lst[lst_pos]] = row[view]
            
            col_cnt += 1
        
        row_cnt += 1
    
    print(new_dct)
    new_df = pd.DataFrame(new_dct)
    new_df.to_csv(new_name + '.csv', index=False)
    with open(new_name + '_tp1totp0.json', 'w+') as fh:
        print(tp1totp0_gt, file=fh)
    

if __name__=='__main__':
    # type0totype1('dacview_test_type0acm.csv', ['kw_inf'], 'dacview_test_type1acm', max_rows=1, max_cols=1)
    # type0totype1('dacview_test_type0acm.csv', ['kw_inf'], 'dacview_test_type1acm')
    construct_type1('dacview_test_type0acm.csv', 0, 'dacview_test_type1acm')
            
            
            
            




