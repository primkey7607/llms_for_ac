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

class MyTimeoutException(Exception):
    pass

#register a handler for the timeout
def handler(signum, frame):
    print("Waited long enough!")
    raise MyTimeoutException("STOP")
    
@retry(retry=retry_if_exception_type(Exception), wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(6))
def get_response(chat, temp_val, timeout=30, write_dir=None, write_pref=None):
    
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=chat,
        temperature=temp_val
    )
    chat_response = response["choices"][0]["message"]["content"]
    print("Received response: {}".format(chat_response))
    
    if write_dir != None and write_pref != None:
        if not os.path.exists(write_dir):
            os.mkdir(write_dir)
            chat_ind = 0
        else:
            all_fs = os.listdir(write_dir)
            all_inds = [int(f[f.index('chat') + 4 :-5]) for f in all_fs]
            chat_ind = max(all_inds) + 1
        
        full_chat = chat + [{'role' : 'assistant', 'content' : chat_response}]
        
        outname = os.path.join(write_dir, write_pref + '_chat' + str(chat_ind) + '.json')
        with open(outname, 'w+') as fh:
            print(full_chat, file=fh)
    elif write_dir != None or write_pref != None:
        raise Exception("Both write_dir and write_pref must be specified: {}, {}".format(write_dir, write_pref))
        
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

def parse_el(raw_resp : str, lst : list, neg_code : str):
    lower_lst = [el.lower() for el in lst]
    l_neg = neg_code.lower()
    l_resp = raw_resp.lower()
    present_els = [lst[i] for i,el in enumerate(lower_lst) if el in l_resp]
    absent = (l_neg in l_resp)
    
    if absent and present_els != []:
        if l_resp.startswith(l_neg):
            return neg_code
        print("WARNING: assuming first element found in response is correct: {}, {}".format(present_els[0], raw_resp))
        return present_els[0]
    elif absent and present_els == []:
        return neg_code
    elif not absent and present_els == []:
        print("WARNING: Assuming none, but retry may be needed: {}".format(raw_resp))
        return neg_code
    elif not absent and present_els != []:
        if len(present_els) > 1:
            print("WARNING: Assuming first element found in response is correct: {}, {}".format(present_els, raw_resp))
            return present_els[0]
    else:
        raise Exception("Not all cases captured: {}, {}".format(absent, present_els))

def parse_yn(raw_resp : str):
    if 'YES' in raw_resp and 'NO' not in raw_resp:
        return 'YES'
    elif 'YES' not in raw_resp and 'NO' in raw_resp:
        return 'NO'
    elif 'YES' not in raw_resp and 'NO' not in raw_resp:
        print("WARNING: response is unclear, so defaulting to 'no': {}".format(raw_resp))
        return 'NO'
    elif 'YES' in raw_resp and 'NO' in raw_resp:
        print("WARNING: choosing earlier token: {}".format(raw_resp))
        y_ind = raw_resp.index('YES')
        n_ind = raw_resp.index('NO')
        if y_ind < n_ind:
            return 'YES'
        else:
            return 'NO'
    else:
        raise Exception("Not all cases captured: {}".format(raw_resp))
    
    
    

