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

