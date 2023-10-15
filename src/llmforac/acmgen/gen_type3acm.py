import pandas as pd
import os
from sqlalchemy import engine
import psycopg2
import openai
import signal
from ast import literal_eval
from utils.chat_utils import get_response, parse_lst
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception_type
)  # for exponential backoff

# class MyTimeoutException(Exception):
#     pass

# #register a handler for the timeout
# def handler(signum, frame):
#     print("Waited long enough!")
#     raise MyTimeoutException("STOP")
    
# def get_schema(db_details):
#     #for now, let's just hardcode it
#     return str(['supplier', 'customer', 'lineitem','region',
#             'orders', 'partsupp', 'part', 'nation'])

# @retry(retry=retry_if_exception_type(Exception), wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(6))
# def get_response(chat, temp_val, timeout=30, write_dir=None):
    
#     response = openai.ChatCompletion.create(
#         model="gpt-3.5-turbo",
#         messages=chat,
#         temperature=temp_val
#     )
#     chat_response = response["choices"][0]["message"]["content"]
#     print("Received response: {}".format(chat_response))
#     return chat_response

# def parse_lst(lst_resp):
#     #chatgpt will delimit code by three apostrophes
#     query_st = lst_resp
#     if '```' in query_st:
#         query_parts = query_st.split('```')
#         queries = [qp for i,qp in enumerate(query_parts) if i % 2 == 1]
#         out_st = queries[0]
#     else:
#         out_st = query_st
#     out_st = out_st[out_st.index('[') : out_st.index(']') + 1]
#     out_st.replace('\n', '')
#     out_st.replace('\r', '')
#     out_st.replace(' ', '')
#     out_lst = literal_eval(out_st)
#     return out_lst

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

def kw_syn(role_name : str):
    sentence = role_name
    req = 'Consider the following phrase:\n\n'
    req += sentence
    req += 'Give 5 synonyms of this phrase.'
    chat = [{'role' : 'system', 'content' : 'You are a helpful assistant.'}]
    chat += [{'role' : 'user', 'content' : req}]
    init_resp = get_response(chat, 0.0)
    
    chat += [{'role' : 'assistant', 'content' : init_resp}]
    followup = 'List the synonyms as a python list.'
    chat += [{'role' : 'user', 'content' : followup}]
    lst_resp = get_response(chat, 0.0)
    
    sent_lst = parse_lst(lst_resp)
    
    return sent_lst

def desc_replace(role_name : str):
    req = 'Give 5 different descriptions of the following phrase without explicitly using any word in the phrase: '
    req += '"' + role_name + '"' + '.'
    chat = [{'role' : 'system', 'content' : 'You are a helpful assistant.'}]
    chat += [{'role' : 'user', 'content' : req}]
    init_resp = get_response(chat, 0.0)
    
    chat += [{'role' : 'assistant', 'content' : init_resp}]
    followup = 'List the descriptions as a python list.'
    chat += [{'role' : 'user', 'content' : followup}]
    lst_resp = get_response(chat, 0.0)
    
    sent_lst = parse_lst(lst_resp)
    
    return sent_lst

def tp0role_to_tp3(role_st : str, perturb_types : list):
    sentences = []
    for p in perturb_types:
        if p == 'kw_syn':
            sentences += kw_syn(role_st)
        elif p == 'desc_replace':
            sentences += desc_replace(role_st)
    
    return sentences

#given the path to a type 2 ACM, translate it to a type 3 ACM.
#in this case, the meanings of rows and columns stay the same, but the 
# cells become natural language.
#we generate natural language using natural language perturbations described
#in Dr. Spider, here: https://openreview.net/forum?id=Wc5bmZZU9cy
def type2totype3(acm_path, perturb_types, outdir, outname, max_rows=None, max_cols=None):
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    df = pd.read_csv(acm_path)
    tp0_roles = df['Role'].tolist()
    
    for i,role in enumerate(tp0_roles):
        new_roles = tp0role_to_tp3(role, perturb_types)
        outfile = outname + '_row' + str(i) + '.txt'
        outfile = os.path.join(outdir, outfile)
        with open(outfile, 'w+') as fh:
            print(new_roles, file=fh)

def construct_type3(acm_path, lst_pos, sent_dir, sent_name, new_name):
    #we also want to store the mappings of privileges to type 1 sentences.
    #this will help later.
    df = pd.read_csv(acm_path)
    new_dct = {}
    tp2totp3 = {}
    for c in df.columns:
        new_dct[c] = df[c].to_list()
    
    tp0_roles = df['Role'].tolist()
    tp3_roles = []
    for i, role in enumerate(tp0_roles):
        outfile = sent_name + '_row' + str(i) + '.txt'
        outfile = os.path.join(sent_dir, outfile)
        with open(outfile, 'r') as fh:
            new_roles = literal_eval(fh.read())
        
        #we should not the use alternatives for CREATE USER...because those are confusing,
        #and it's unclear what the synonym of a name, like "Gisela" is.
        if 'CREATE' in role:
            new_role = role
        else:
            new_role = new_roles[lst_pos]
        
        tp3_roles.append(new_role)
        
        tp2totp3[role] = new_role
    
    new_dct['Role'] = tp3_roles
    new_df = pd.DataFrame(new_dct)
    new_df.to_csv(new_name + '.csv', index=False)
    
    with open(new_name + '_tp2totp3.json', 'w+') as fh:
        print(tp2totp3, file=fh)

if __name__=='__main__':
    type2totype3('dacview_test_type2acm.csv', ['desc_replace'], 'dacview_type3_allsents', 'dacview_test_type3')
    construct_type3('dacview_test_type2acm.csv', 0, 'dacview_type3_allsents', 'dacview_test_type3', 'dacview_test_type3acm')
            
