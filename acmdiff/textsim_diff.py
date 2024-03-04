import os
import pandas as pd
from utils.db_utils import PostgresAPI
from utils.chat_utils import get_response
from compare_jaccard import jaccard_on_lst
from acmdiff.diff_acms import diff_privs, privdiff_summary, rolediff_summary, viewdiff_summary, sqlvssql_views, sqlvssql_roles

'''
Purpose: in this file, we difference ACMs only using word-based semantic similarity, and nothing else.
Currently, we take a simple average over word embeddings in a sentence. We may replace this with something more principled later.
'''

def nlvsnl_views(nldf1, nldf2, nl1_cols, nl2_cols, outdir, outname):
    views1 = [c for c in nldf1.columns.tolist() if c != 'Role']
    views2 = nl2_cols
    for i,v1 in enumerate(views1):
        if v1 not in nl1_cols:
            continue
        outfile = os.path.join(outdir, outname + '_nlvsnl_view' + str(i) + '.txt')
        if os.path.exists(outfile):
            continue
        
        #first, check for string similarity.
        if v1 in views2:
            answer = (v1, 1.0)
            with open(outfile, 'w+') as fh:
                print(answer, file=fh)
        else:
            sem_sent, sem_score = jaccard_on_lst(v1, views2)
            answer = (sem_sent, sem_score)
            with open(outfile, 'w+') as fh:
                print(answer, file=fh)

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
        if os.path.exists(outfile):
            continue
        chat = [{'role' : 'system', 'content' : 'You are a helpful assistant.'}]
        
        prompt1 = 'Consider the following SQL for a table or view: ' + db_v + '. '
        prompt1 += 'Explain what this query does in plain english.'
        chat += [{'role' : 'user', 'content' : prompt1}]
        raw_resp1 = get_response(chat, 0.0)
        chat += [{'role' : 'assistant', 'content' : raw_resp1}]
        
        sem_sent, sem_score = jaccard_on_lst(db_v, acm_views)
        answer = (sem_sent, sem_score, lst_acmpath)
        with open(outfile, 'w+') as fh:
            print(answer, file=fh)
        
        with open(outchat, 'w+') as fh:
            print(chat, file=fh)

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

def nlvsnl_roles(nl1_acmdf, nl2_acmdf, nl1_roles, nl2_roles, outdir, outname):
    roles1 = nl1_acmdf['Role'].tolist()
    roles2 = nl2_roles
    for i,r1 in enumerate(roles1):
        if r1 not in nl1_roles:
            continue
        outfile = os.path.join(outdir, outname + '_nlvsnl_role' + str(i) + '.txt')
        if os.path.exists(outfile):
            continue
        
        #first, check for exact match
        if r1 in roles2:
            answer = (r1, 1.0)
            with open(outfile, 'w+') as fh:
                print(answer, file=fh)
        else:
            
            sem_role, sem_score = jaccard_on_lst(r1, roles2)
            answer = (sem_role, sem_score)
            with open(outfile, 'w+') as fh:
                print(answer, file=fh)

def nlvssql_roles(nl_acmdf, sql_acmdf, nl_roles, sql_roles, outdir, outname):
    acm_roles = nl_acmdf['Role'].tolist()
    db_roles = sql_roles
    
    for i,acm_r in enumerate(acm_roles):
        if acm_r not in nl_roles:
            continue
        outfile = os.path.join(outdir, outname + '_nlvssql_role' + str(i) + '.txt')
        if os.path.exists(outfile):
            continue
        
        #TODO: what's the principled way to compare SQL roles to NL roles? For now, we treat both as sentences and compare.
        #we do this for roles and not views because SQL role definitions resemble english more than view definitions.
        sem_role, sem_score = jaccard_on_lst(acm_r, db_roles)
        answer = (sem_role, sem_score)
        with open(outfile, 'w+') as fh:
            print(answer, file=fh)

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