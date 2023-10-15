import pandas as pd
import os
from sqlalchemy import engine
import psycopg2
import openai
import signal
from ast import literal_eval
from type1tosql import tp1totp0, reconstruct_type0, tp0tosentences
from c3_utils import get_schema, c3_e2e
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception_type
)  # for exponential backoff

#TODO: for now, we'll hardcode it. But this needs to come from postgres.
def get_tbl_schema(db_details):
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

def schema_to_st(db_schema : dict):
    schema = ""
    for tab, cols in db_schema.items():
        schema += '# ' + tab + ' ( '
        for i, col in enumerate(cols):
            schema += col
            schema += ', '
        schema = schema[:-2] + ' )\n'
    
    return schema
        

def t2sent_to_t1(view_st, db_schema):
    chat = [{'role' : 'system', 'content' : 'You are a helpful assistant.'}]
    print("Schema String: {}".format(schema_to_st(db_schema)))
    prompt = '### Complete postgres SQL statement only and with no explanation, and do not grant privileges on tables, roles, and users that are not explicitly requested in the statement. \n ### Postgres SQL tables, with their properties:'
    prompt += ' \n ' + schema_to_st(db_schema) + '\n#\n### ' + view_st +  '\nCREATE VIEW'
    chat += [{'role' : 'user', 'content' : prompt}]
    view_sql = get_response(chat, 0.0)
    if view_sql.startswith('CREATE CREATE'):
        view_sql = view_sql.replace('CREATE', '')
        view_sql = 'CREATE ' + view_sql
    elif not view_sql.startswith('CREATE VIEW'):
        view_sql = 'CREATE VIEW ' + view_sql
    
    return view_sql

def tp2totp1(tp2acmpath, outdir, outname, max_rows=None, max_cols=None):
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    df = pd.read_csv(tp2acmpath)
    tbls = get_tbl_schema({})
    db_schema = get_schema()
    col_cnt = 0
    for c in df.columns:
        outfile = outname + '_tp2totp1col' + str(col_cnt) + '.txt'
        outfile = os.path.join(outdir, outfile)
        if os.path.exists(outfile):
            col_cnt += 1
            continue
        final_sent = ''
        if c in tbls:
            final_sent = c
        else:
            final_sent = t2sent_to_t1(c, db_schema)
        
        with open(outfile, 'w+') as fh:
            print(final_sent, file=fh)
        
        col_cnt += 1

def reconstruct_type1(tp2_path, new_name):
    #we also want to store the mappings of privileges to type 1 sentences.
    #this will help later.
    acm_path = tp2_path
    df = pd.read_csv(acm_path)
    new_dct = {}
    tp2totp1 = {}
    new_dct['Role'] = df['Role'].tolist()
    col_cnt = 0
    for c in df.columns:
        outfile = new_name + '_tp2totp1col' + str(col_cnt) + '.txt'
        if c == 'Role':
            col_cnt += 1
            continue
        
        with open(outfile, 'r') as fh:
            new_tp1 = fh.read()
        
        new_tp1 = new_tp1.replace('\n', '')
        
        new_dct[new_tp1] = df[c].tolist()
        
        tp2totp1[c] = new_tp1
        
        col_cnt += 1
    
    new_df = pd.DataFrame(new_dct)
    new_df.to_csv(new_name + '.csv', index=False)
    with open(new_name + '_tp1totp2.json', 'w+') as fh:
        print(tp2totp1, file=fh)

#use C3 to generate sentences
def gen_tp1sql(new_tp1acmpath):
    #TODO: this method heavily reuses code in type1tosql.py.
    #we may want to refactor this.
    tp1_noext = new_tp1acmpath.split('.')[:-1]
    tp1_outname = '.'.join(tp1_noext) + '_totp0'
    tp1totp0(new_tp1acmpath, tp1_outname)
    reconstruct_type0(new_tp1acmpath, tp1_outname)
    tp0acmpath = tp1_outname + '.csv'
    
    sentences = tp0tosentences(tp0acmpath)
    print(sentences)
    
    #generate the SQL files on disk for these sentences by running c3 end-to-end
    #this will return a dictionary of each sentence to its query
    nl2sql = c3_e2e(sentences, outdir='c3newtp1_raw', inprefix='c3newtp1_', outprefix='c3newtp1_complete', compdir='c3newtp1complete', sql_prefix='c3sql_newtp1', sqldir='c3newtp1sql', dctname='c3newtp1_nl2sql.json')
    
    return nl2sql

if __name__=='__main__':
    tp2totp1('../acmgen/dacview_test_type2acm.csv', 'dacview_test_tp2totp1_all', 'dacview_tp2tp1')
    reconstruct_type1('../acmgen/dacview_test_type2acm.csv', 'dacview_test_tp2totp1_all/dacview_tp2tp1')
    gen_tp1sql('dacview_test_tp2totp1_all/dacview_tp2tp1.csv')
    
        
        
        
        
    
    
            
            
            
            
            

