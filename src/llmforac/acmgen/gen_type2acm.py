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

def queries_to_tp2views(view_queries : dict):
    #TODO: maybe not in this function, but we want to store this dictionary,
    #as this already gives us a direct mapping from Type 0 to Type 2.
    tp2views = view_queries.values()
    tp0views = view_queries.keys()
    
    return tp2views

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
    req = 'Consider the following sentence:\n\n'
    req += sentence
    req += 'Give 5 rephrasings of this sentence where the column names are replaced with synonyms.'
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
    req = 'Consider the following sentence:\n\n'
    req += sentence
    req += 'Give 5 rephrasings of this sentence where the column names are implied by carrier phrases. '
    req += 'For example, replace "Create a view of the customer table with all columns except NAME, ADDRESS, and PHONE" with "with columns containing data that is not sensitive".'
    chat = [{'role' : 'system', 'content' : 'You are a helpful assistant.'}]
    chat += [{'role' : 'user', 'content' : req}]
    init_resp = get_response(chat, 0.0)
    
    chat += [{'role' : 'assistant', 'content' : init_resp}]
    followup = 'List the sentences as a python list.'
    chat += [{'role' : 'user', 'content' : followup}]
    lst_resp = get_response(chat, 0.0)
    
    sent_lst = parse_lst(lst_resp)
    
    return sent_lst

def tp0view_to_tp2(view_st : str, perturb_types : list):
    view_nl, view_name = view_to_nl(view_st)
    sentences = []
    for pt in perturb_types:
        if pt == 'kw_inf':
            new_sent = kw_inf(view_nl)
            sentences += new_sent
        elif pt == 'kw_syn':
            new_sent = kw_syn(view_nl)
            sentences += new_sent
    
    return sentences

#given the path to a type 0 ACM, translate it to a type 2 ACM.
#in this case, the meanings of rows and columns stay the same, but the 
# cells become natural language.
#we generate natural language using natural language perturbations described
#in Dr. Spider, here: https://openreview.net/forum?id=Wc5bmZZU9cy
def type1totype2(acm_path, perturb_types, outdir, outname, max_rows=None, max_cols=None):
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    df = pd.read_csv(acm_path)
    col_cnt = 0
    for c in df.columns:
        outfile = outname + '_col' + str(col_cnt) + '.txt'
        outfile = os.path.join(outdir, outfile)
        if os.path.exists(outfile):
            col_cnt += 1
            continue
        if c == 'Role':
            col_cnt += 1
            continue
        else:
            if 'CREATE VIEW' in c:
                tp2_sents = tp0view_to_tp2(c, perturb_types)
            else:
                tp2_sents = [c]
            with open(outfile, 'w+') as fh:
                print(tp2_sents, file=fh)
            
            col_cnt += 1

def construct_type2(acm_path, lst_pos, sent_dir, sent_name, new_name):
    df = pd.read_csv(acm_path)
    new_dct = {}
    new_dct['Role'] = df['Role'].to_list()
    tp1totp2 = {}
    col_cnt = 0
    for c in df.columns:
        outfile = sent_name + '_col' + str(col_cnt) + '.txt'
        outfile = os.path.join(sent_dir, outfile)
        if c == 'Role':
            col_cnt += 1
            continue
        else:
            with open(outfile, 'r') as fh:
                sent_lst = literal_eval(fh.read())
            cur_pos = min(lst_pos, len(sent_lst) - 1)
            cur_el = sent_lst[cur_pos]
            new_dct[cur_el] = df[c].to_list()
            tp1totp2[c] = cur_el
            col_cnt += 1
    
    new_df = pd.DataFrame(new_dct)
    new_df.to_csv(new_name + '.csv', index=False)
    with open(new_name + '_tp1totp2.json', 'w+') as fh:
        print(tp1totp2, file=fh)
            
        

if __name__=='__main__':
    # type1totype2('dacview_test_type1acm.csv', ['kw_syn'], 'dacview_type2_allsents', 'dacview_test_type2')
    construct_type2('dacview_test_type1acm.csv', 0, 'dacview_type2_allsents', 'dacview_test_type2', 'dacview_test_type2acm')
            
    
    
    
    

