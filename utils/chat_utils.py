import pandas as pd
import os
from sqlalchemy import engine
import psycopg2
import openai
import signal
import tiktoken
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
def fancy_get_response(chat, temp_val, timeout=30, write_dir=None, write_pref=None):
    
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo-0301",
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

@retry(retry=retry_if_exception_type(Exception), wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(6))
def get_response(chat, temp_val, timeout=30, write_dir=None, write_file=None):
    
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo-0125",
        messages=chat,
        temperature=temp_val
    )
    chat_response = response["choices"][0]["message"]["content"]
    print("Received response: {}".format(chat_response))
    
    if write_dir != None and write_file != None:
        if not os.path.exists(write_dir):
            os.mkdir(write_dir)
        # else:
            #let's keep this simple--we know outdir, we know outpref upstream
            #so we can just write those, and ignore the below three lines.
            # all_fs = os.listdir(write_dir)
            # all_inds = [int(f[f.index('chat') + 4 :-5]) for f in all_fs]
            # chat_ind = max(all_inds) + 1
        
        full_chat = chat + [{'role' : 'assistant', 'content' : chat_response}]
        
        outname = os.path.join(write_dir, write_file)
        with open(outname, 'w+') as fh:
            print(full_chat, file=fh)
    elif write_dir != None or write_file != None:
        raise Exception("Both write_dir and write_file must be specified: {}, {}".format(write_dir, write_file))
        
    return chat_response

#this function was written by
# https://stackoverflow.com/questions/75804599/openai-api-how-do-i-count-tokens-before-i-send-an-api-request
def num_tokens_from_string(string: str, encoding_name: str) -> int:
    encoding = tiktoken.encoding_for_model(encoding_name)
    num_tokens = len(encoding.encode(string))
    return num_tokens


def chunk_response(prompt : str, context_prompt : str, context : list, model_name, temp_val, chunkdir, chunkpref, timeout=30):
    if not os.path.exists(chunkdir):
        os.mkdir(chunkdir)
    
    #we'll need to play with the below constant to see how much perf degrades as we add more context
    max_tokens = 500 
    full_st = ' '.join(context)
    context_size = num_tokens_from_string(full_st, model_name)
    
    if context_size > max_tokens:
        #then, chunk it
        sent_sizes = [num_tokens_from_string(sent, model_name) for sent in context]
        chunks = []
        chunk_sz = 0
        cur_chunk = []
        for i,sent in enumerate(context):
            if sent_sizes[i] > max_tokens:
                raise Exception("There's no reason why a sentence expressing a single privilege should be this long: {}".format(sent))
            
            if chunk_sz + sent_sizes[i] > max_tokens:
                chunks.append(cur_chunk)
                chunk_sz = 0
                cur_chunk = []
                
                chunk_sz = sent_sizes[i]
                cur_chunk.append(sent)
            else:
                chunk_sz += sent_sizes[i]
                cur_chunk.append(sent)
        
        if cur_chunk != []:
            chunks.append(cur_chunk)
        
    else:
        chunks = [[full_st]]
    
    chunk_resps = []
    for i,chunk in enumerate(chunks):
        chunkfile = os.path.join(chunkdir, chunkpref + '_chunk' + str(i) + '.json')
        if os.path.exists(chunkfile):
            with open(chunkfile, 'r') as fh:
                chunk_resps.append(fh.read())
        else:
            chat = [{'role' : 'system', 'content' : 'You are a helpful assistant.'}]
            cur_context = ' '.join(chunk)
            full_prompt = context_prompt + '\n' + cur_context + '\n' + prompt
            chat += [{'role' : 'user', 'content' : full_prompt}]
            cur_resp = get_response(chat, temp_val)
            chunk_resps.append(cur_resp)
            with open(chunkfile, 'w+') as fh:
                print(cur_resp, file=fh)
    
    return chunk_resps
            

def parse_lst(lst_resp):
    #chatgpt will delimit code by three apostrophes
    print("Parsing response: {}".format(lst_resp))
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
    print("present_els: {}".format(present_els))
    absent = (l_neg in l_resp)
    print("absent: {}".format(absent))
    
    if absent and present_els != []:
        if l_resp.startswith(l_neg):
            print("Response started with negative token: {}, {}".format(l_resp, l_neg))
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
        
        return present_els[0]
    else:
        raise Exception("Not all cases captured: {}, {}".format(absent, present_els))

def approx_present(el, full_st):
    if len(el) < 5 or len(full_st) < 5:
        return (el in full_st)
    else:
        if el in full_st:
            return True
        
        most_len = int(len(el) * 0.8)
        if el[:most_len] in full_st:
            return True
        
        return False
        

#this is like parse_el, but we want to find strings that are most likely to match some element,
#but maybe not exact matches.
def parse_elv2(raw_resp : str, lst : list, neg_code : str):
    #first, properly process the raw response
    proc_resp = raw_resp.lower()
    proc_resp = proc_resp.replace(' ', '')
    proc_resp = proc_resp.replace('\n', '')
    proc_resp = proc_resp.replace('\t', '')
    
    
    l_lst = [el.lower() for el in lst]
    proc_lst = [el.replace(' ', '') for el in l_lst]
    proc_lst = [el.replace('\n', '') for el in proc_lst]
    proc_lst = [el.replace('\t', '') for el in proc_lst]
    
    l_neg = neg_code.lower()
    present_els = [lst[i] for i,el in enumerate(proc_lst) if approx_present(el, proc_resp)]
    print("present_els: {}".format(present_els))
    absent = (l_neg in proc_resp)
    print("absent: {}".format(absent))
    
    if absent and present_els != []:
        if proc_resp.startswith(l_neg):
            print("Response started with negative token: {}, {}".format(proc_resp, l_neg))
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
        
        return present_els[0]
    else:
        raise Exception("Not all cases captured: {}, {}".format(absent, present_els))

#this is like parse_el, but we want to find strings that are most likely to match some element,
#but maybe not exact matches.
#TODO: adapt this for roles and users, e.g., ChatGPT only returned the role r, 
#but the full phrase is 'u has role r', so we parse to None incorrectly.
def parse_role(raw_resp : str, lst : list, neg_code : str):
    #first, properly process the raw response
    proc_resp = raw_resp.lower()
    proc_resp = proc_resp.replace(' ', '')
    proc_resp = proc_resp.replace('\n', '')
    proc_resp = proc_resp.replace('\t', '')
    
    
    l_lst = [el.lower() for el in lst]
    proc_lst = [el.replace(' ', '') for el in l_lst]
    proc_lst = [el.replace('\n', '') for el in proc_lst]
    proc_lst = [el.replace('\t', '') for el in proc_lst]
    
    l_neg = neg_code.lower()
    present_els = [lst[i] for i,el in enumerate(proc_lst) if approx_present(el, proc_resp)]
    #search also for cases where the model only gave the role for a user
    
    print("present_els: {}".format(present_els))
    absent = (l_neg in proc_resp)
    print("absent: {}".format(absent))
    
    if absent and present_els != []:
        if proc_resp.startswith(l_neg):
            print("Response started with negative token: {}, {}".format(proc_resp, l_neg))
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
        
        return present_els[0]
    else:
        raise Exception("Not all cases captured: {}, {}".format(absent, present_els))

#given a directory of files, clean the results using a bash script
def clean_badchars(rawdir):
    print("Fixing '^M' characters...")
    os.system(f"bash ./fix_newline.sh {rawdir}")
    

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
    
    
    

