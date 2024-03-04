import pandas as pd
import os
import re
from sqlalchemy import engine
import psycopg2
import openai
import functools
import signal
from ast import literal_eval
from utils.chat_utils import get_response, parse_el, parse_yn, parse_elv2
from utils.db_utils import PostgresAPI, views_are_equal
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception_type
)  # for exponential backoff

def nlvssql_views(nl_acmdf, sql_acmdf, nl_cols, sql_cols, outdir, outname):
    acm_views = nl_acmdf.columns.tolist()
    db_views = sql_cols
    
    for i,acm_v in enumerate(acm_views):
        if acm_v not in nl_cols:
            continue
        outfile = os.path.join(outdir, outname + '_nlvssql_view' + str(i) + '.txt')
        if os.path.exists(outfile):
            continue
        chat = [{'role' : 'system', 'content' : 'You are a helpful assistant.'}]
        prompt = 'Consider the following list of tables and views in a database:\n\n' + str(db_views) + '\n\n'
        prompt += 'Consider the following phrase describing a table or view: ' + acm_v + '. '
        prompt += 'Which database table or view from the list does this phrase most likely describe? Begin your answer with this table/view. If none of them match, begin your answer with None.'
        prompt += ' If you are unsure, make your best guess.'
        
        chat += [{'role' : 'user', 'content' : prompt}]
        raw_resp = get_response(chat, 0.0, write_dir=outdir, write_file=outname + '_nlvssql_view' + str(i) + '_chat.json')
        parsed = parse_elv2(raw_resp, db_views, 'None')
        print("Parsed Answer: {}".format(parsed))
        answer = (parsed, raw_resp)
        with open(outfile, 'w+') as fh:
            print(answer, file=fh)

def sqlvsnl_views(nl_acmdf, sql_acmdf, lst_acmpath, nl_cols, sql_cols, outdir, outname):
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
        
        prompt2 = 'Consider the following list of descriptions of views in a database:\n\n' + str(acm_views) + '\n\n'
        prompt2 += 'Which description from the list does the given query most likely describe? Begin your answer with this description. If none of them match, begin your answer with None.'
        prompt2 += ' If you are unsure, make your best guess.'
        
        chat += [{'role' : 'user', 'content' : prompt2}]
        raw_resp = get_response(chat, 0.0, write_dir=outdir, write_file=outname + '_sqlvsnl_view' + str(i) + '_chat.json')
        chat += [{'role' : 'assistant', 'content' : raw_resp}]
        parsed = parse_elv2(raw_resp, acm_views, 'None')
        print("Parsed Answer: {}".format(parsed))
        answer = (parsed, raw_resp, lst_acmpath)
        with open(outfile, 'w+') as fh:
            print(answer, file=fh)
        
        # with open(outchat, 'w+') as fh:
        #     print(chat, file=fh)

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
            answer = (v1, 'Exact Match')
            with open(outfile, 'w+') as fh:
                print(answer, file=fh)
        else:
            chat = [{'role' : 'system', 'content' : 'You are a helpful assistant.'}]
            prompt = 'Consider the following list of descriptions for tables and views in a database:\n\n' + str(views2) + '\n\n'
            prompt += 'Consider the following phrase describing a table or view: ' + v1 + '. '
            prompt += 'Which database table or view description from the list most likely describes the same table or view as this phrase? Begin your answer with your chosen description from the list. If none of them match, begin your answer with None.'
            prompt += ' If you are unsure, make your best guess.'
            
            chat += [{'role' : 'user', 'content' : prompt}]
            raw_resp = get_response(chat, 0.0, write_dir=outdir, write_file=outname + '_nlvsnl_view' + str(i) + '_chat.json')
            parsed = parse_elv2(raw_resp, views2, 'None')
            answer = (parsed, raw_resp)
            with open(outfile, 'w+') as fh:
                print(answer, file=fh)

def sqlvssql_views(sqldf1, sqldf2, sql1_cols, sql2_cols, outdir, outname, db_details):
    cols1 = sqldf1.columns.tolist()
    cols2 = sql2_cols
    
    for i,c1 in enumerate(cols1):
        print("Currently processing: {}, {}".format(i, c1))
        if c1 not in sql1_cols:
            continue
        outfile = os.path.join(outdir, outname + '_sqlvssql_view' + str(i) + '.txt')
        if os.path.exists(outfile):
            continue
        
        if c1 in cols2:
            with open(outfile, 'w+') as fh:
                print((c1, 'Table of Same Name is in Both ACMs'), file=fh)
        elif 'CREATE VIEW' in c1:
            c2_views = [c for c in cols2 if 'CREATE VIEW' in c]
            same_views = []
            for c2v in c2_views:
                if views_are_equal(c1, c2v, db_details):
                    same_views.append(c2v)
            
            #for now, we will assume that each column of a dataframe
            #represents a distinct view whose records will always be distinct
            #from all other views under every instance of the database.
            #we may want to relax this assumption later.
            #in this case, there should only be one matching view
            if same_views != []:
                match_v = same_views[0]
                with open(outfile, 'w+') as fh:
                    print((match_v, 'Views matched on current database.'), file=fh)
            else:
                with open(outfile, 'w+') as fh:
                    print(('None', 'No views matched on current database.'), file=fh)
        
        elif len(c1.split(' ')) == 1:
            #then, we guess that c1 is a table, but c1 is not in cols_2
            with open(outfile, 'w+') as fh:
                print(('None', 'Table not found in one of the ACMs'), file=fh)
        
        else:
            raise Exception("SQL Case not captured: {}".format(c1))
                    
            
        

def diff_views(acmpath1, acmpath2, outdir, outname, db_details):
    '''
    We'll use prompting to compare views.
    But we need a prompt (or going straight to the DB) that compares SQL queries if they are equivalent,
    we need a prompt for ACM to SQL (that is the sqltoacm prompt), and we need a prompt for comparing natural language
    '''
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

def pattern_matches(pattern, st):
    match_obj = pattern.match(st)
    if match_obj == None:
        print("Match object was false, no match")
        return False
    elif match_obj.group() == st:
        print("Match object was true, and there was a match")
        return True
    
    print("Match object matched only on part of the string, but not the whole thing: {}, {}".format(match_obj, st))
    return False

#what views in acmdf1 are contained in acmdf2, and what views are not?
def viewdiff_summary(acmpath1, acmpath2, outdir, outname, db_details):
    out_schema = ['Response File', 'ACM 1 Column', 'ACM 2 Column', 'Explanation']
    out_dct = {}
    for o in out_schema:
        out_dct[o] = []
    
    acmdf1 = pd.read_csv(acmpath1)
    acmdf2 = pd.read_csv(acmpath2)
    # cols1 = acmdf1.columns.tolist()
    cols1 = [c for c in acmdf1.columns.tolist() if c != 'Role']
    cols2 = [c for c in acmdf2.columns.tolist() if c != 'Role']
    for f in os.listdir(outdir):
        pattern = re.compile('^(.*)_view[0-9]+.txt$')
        if f.startswith(outname) and pattern_matches(pattern, f):
            #first, determine what ACM column f contains the mapping for.
            v_ind = f.rindex('_view')
            c_ind_st = f[v_ind + 5 : -4]
            c_ind = int(c_ind_st)
            
            outfile = os.path.join(outdir, f)
            print("Reading file: {}".format(outfile))
            with open(outfile, 'r') as fh:
                tup = literal_eval(fh.read())
            
            #first, we need to decide which set of columns tup came from,
            #because it could be either in the case of SQL vs NL.
            if len(tup) > 2 and tup[2] == acmpath1:
                rel_col = cols2[c_ind]
                out_dct['Response File'] += [outfile]
                out_dct['ACM 1 Column'] += [tup[0]]
                out_dct['ACM 2 Column'] += [rel_col]
                out_dct['Explanation'] += [tup[1]]
            elif len(tup) > 2 and tup[2] == acmpath2:
                rel_col = cols1[c_ind]
                out_dct['Response File'] += [outfile]
                out_dct['ACM 1 Column'] += [rel_col]
                out_dct['ACM 2 Column'] += [tup[0]]
                out_dct['Explanation'] += [tup[1]]
            else:
                rel_col = cols1[c_ind]
                out_dct['Response File'] += [outfile]
                out_dct['ACM 1 Column'] += [rel_col]
                out_dct['ACM 2 Column'] += [tup[0]]
                out_dct['Explanation'] += [tup[1]]
    
    out_df = pd.DataFrame(out_dct)
    outpath = os.path.join(outdir, outname + '_viewcomplete.csv')
    out_df.to_csv(outpath, index=False)

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
            answer = (r1, 'Exact Match.')
            with open(outfile, 'w+') as fh:
                print(answer, file=fh)
        else:
            
            chat = [{'role' : 'system', 'content' : 'You are a helpful assistant.'}]
            prompt = 'Consider the following list of descriptions for roles in a database:\n\n' + str(roles2) + '\n\n'
            prompt += 'Consider the following phrase describing a database role: ' + r1 + '. '
            prompt += 'Which database role description from the list most likely describes the same role as this phrase? Begin your answer with your chosen description from the list. If none of them match, begin your answer with None.'
            prompt += ' If you are unsure, make your best guess.'
            
            chat += [{'role' : 'user', 'content' : prompt}]
            raw_resp = get_response(chat, 0.0, write_dir=outdir, write_file=outname + '_nlvsnl_role' + str(i) + '_chat.json')
            parsed = parse_elv2(raw_resp, roles2, 'None')
            if parsed == None:
                print("Got nothing! {}, {}, {}".format(parsed, raw_resp, roles2))
            answer = (parsed, raw_resp)
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
        chat = [{'role' : 'system', 'content' : 'You are a helpful assistant.'}]
        prompt = 'Consider the following list of roles in a database:\n\n' + str(db_roles) + '\n\n'
        prompt += 'Consider the following phrase describing a role: ' + acm_r + '. '
        prompt += 'Which database role from the list does this phrase most likely describe? Begin your answer with this role. If none of them match, begin your answer with None.'
        prompt += ' If you are unsure, make your best guess.'
        
        chat += [{'role' : 'user', 'content' : prompt}]
        raw_resp = get_response(chat, 0.0, write_dir=outdir, write_file=outname + '_nlvssql_role' + str(i) + '_chat.json')
        parsed = parse_elv2(raw_resp, db_roles, 'None')
        if parsed == None:
            print("Got nothing! {}, {}, {}".format(parsed, raw_resp, db_roles))
        answer = (parsed, raw_resp)
        with open(outfile, 'w+') as fh:
            print(answer, file=fh)

def sqlvssql_roles(sqldf1, sqldf2, sql1_roles, sql2_roles, outdir, outname, db_details):
    roles1 = sqldf1['Role'].tolist()
    roles2 = sql2_roles
    
    pgapi = PostgresAPI(db_details)
    db_privs = pgapi.get_privs()
    db_roles = [tup[0] for tup in db_privs]
    
    for i,r1 in enumerate(roles1):
        if r1 not in sql1_roles:
            continue
        outfile = os.path.join(outdir, outname + '_sqlvssql_role' + str(i) + '.txt')
        if os.path.exists(outfile):
            continue
        
        if r1 in roles2:
            with open(outfile, 'w+') as fh:
                print((r1, 'Database Role of Same Name is in Both ACMs'), file=fh)
        elif 'CREATE USER' in r1:
            usr_name = r1.split(' ')[2].replace(';', '')
            r2_names = [r2.split(' ')[2].replace(';', '') for r2 in roles2 if 'CREATE USER' in r2]
            r2_raw = [r2 for r2 in roles2 if r2 not in r2_names]
            if usr_name in r2_names or usr_name in r2_raw:
                with open(outfile, 'w+') as fh:
                    print((r1, 'Same user is created in both ACMs'), file=fh)
            else:
                with open(outfile, 'w+') as fh:
                    print(('None', 'Database role not found in both ACMs.'), file=fh)
        elif r1 in db_roles:
            #at this point, we know that r1 is not in roles2, but r1 is not a create user statement.
            with open(outfile, 'w+') as fh:
                print(('None', 'Database role not found in both ACMs.'), file=fh)
        elif r1 not in db_roles:
            with open(outfile, 'w+') as fh:
                print(('None', 'Given role is not a database role.'), file=fh)
        else:
            raise Exception("DB Role Case not captured: {}".format(r1))

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

#what roles in acmdf1 are contained in acmdf2, and what roles are not?
def rolediff_summary(acmpath1, acmpath2, outdir, outname, db_details, reparse=False):
    acmdf1 = pd.read_csv(acmpath1)
    acmdf2 = pd.read_csv(acmpath2)
    out_schema = ['ACM 1 Role', 'ACM 2 Role', 'Explanation']
    out_dct = {}
    for o in out_schema:
        out_dct[o] = []
        
    roles1 = acmdf1['Role'].tolist()
    roles2 = acmdf2['Role'].tolist()
    for f in os.listdir(outdir):
        pattern = re.compile('^(.*)_role[0-9]+.txt$')
        if f.startswith(outname) and pattern_matches(pattern, f) and f.endswith('.txt'):
            #first, determine what ACM column f contains the mapping for.
            v_ind = f.rindex('_role')
            c_ind_st = f[v_ind + 5 : -4]
            c_ind = int(c_ind_st)
            rel_col = roles1[c_ind]
            
            outfile = os.path.join(outdir, f)
            with open(outfile, 'r') as fh:
                raw_tup = literal_eval(fh.read())
            
            if reparse:
                raw_resp = raw_tup[1]
                new_ans = parse_elv2(raw_resp, roles2, 'None')
                tup = (new_ans, raw_resp)
            else:
                tup = raw_tup
            
            print("Current Tuple: {}".format(tup))
            first_tup = rel_col
            second_tup = tup[0]
            if tup[0] not in roles2 and tup[0] in roles1:
                #then, the answer actually came from roles1
                first_tup = tup[0]
                second_tup = roles2[c_ind]
            
            out_dct['ACM 1 Role'] += [first_tup]
            out_dct['ACM 2 Role'] += [second_tup]
            out_dct['Explanation'] += [tup[1]]
    
    out_df = pd.DataFrame(out_dct)
    outpath = os.path.join(outdir, outname + '_rolecomplete.csv')
    out_df.to_csv(outpath, index=False)

def nlvsnl_privs(nlpriv1, nlpriv2, outdir, outfile):
    chat = [{'role' : 'system', 'content' : 'You are a helpful assistant.'}]
    prompt = 'Consider the following sentence/phrase describing database permissions for a role on a table: Privilege 1-' + nlpriv1 + '.'
    prompt += 'Consider the following other sentence/phrase describing database permissions for the same role on the same table: Privilege 2-' + nlpriv2 + '.'
    prompt += ' Of Privilege 1 and Privilege 2, which is more permissive? Begin your response with Privilege 1, Privilege 2, or Same. If you are unsure, make your best guess.'
    chat += [{'role' : 'user', 'content' : prompt}]
    raw_resp = get_response(chat, 0.0, write_dir=outdir, write_file=outfile)
    answer = parse_elv2(raw_resp, ['Privilege 1', 'Privilege 2', 'Same'], 'None')
    if answer == 'Privilege 1' or answer == 'Same':
        final_ans = (False, raw_resp)
    elif answer == 'Privilege 2':
        final_ans = (True, raw_resp)
    else:
        final_ans = (True, "WARNING: Assuming violation due to unclear explanation, shown below:\n" + raw_resp)
    
    return final_ans

def nlvssql_privs(nlpriv1, sqlpriv2, outdir, outfile):
    chat = [{'role' : 'system', 'content' : 'You are a helpful assistant.'}]
    prompt = 'Consider the following sentence/phrase describing database permissions for a role on a table: Privilege 1-' + nlpriv1 + '.'
    prompt += 'Consider the following list of database permissions for the same role on the same table: Privilege 2-' + sqlpriv2 + '.'
    prompt += ' Is Privilege 2 a sufficient set of permissions for allowing Privilege 1? Begin your answer with YES or NO. If you are unsure, make your best guess.'
    chat += [{'role' : 'user', 'content' : prompt}]
    raw_resp1 = get_response(chat, 0.0, write_dir=outdir, write_file=outfile)
    ans1 = parse_yn(raw_resp1)
    if ans1 == 'NO':
        final_ans = (False, raw_resp1)
        return final_ans
    else:
        prompt2 = ' Following the principle of least privilege, is Privilege 2 the least possible set of permissions needed to allow Privilege 1?'
        prompt2 += ' Begin your answer with YES or NO. If you are unsure, make your best guess.'
        chat += [{'role' : 'assistant', 'content' : raw_resp1}]
        chat += [{'role' : 'user', 'content' : prompt2}]
        raw_resp2 = get_response(chat, 0.0)
        ans2 = parse_yn(raw_resp2)
        if ans2 == 'YES':
            final_ans = (False, raw_resp2)
        else:
            final_ans = (True, raw_resp2)
        return final_ans

def sqlvssql_privs(sqlpriv1, sqlpriv2):
    sql_privs = ['SELECT', 'GRANT', 'UPDATE', 'INSERT', 'DELETE', 'CREATE']
    privs1 = [p for p in sql_privs if p in sqlpriv1]
    privs2 = [p for p in sql_privs if p in sqlpriv2]
    
    if len(privs1) >= len(privs2):
        return (False, 'First list of database permissions is larger than the second.')
    else:
        return (True, 'Second list of database permissions is larger than the first.')
        

#with respect to acmdf1, which rules expressed in acmdf2 are in violation?
#in order to test this, we need to look at the privileges described on the shared
#roles and views, and point out the extra roles and views defined by acmdf2
#and the missing roles and views from acmdf2.
def diff_privs(acmpath1, acmpath2, indir, inname, outdir, outname):
    acmdf1 = pd.read_csv(acmpath1)
    acmdf2 = pd.read_csv(acmpath2)
    sql_privs = ['SELECT', 'GRANT', 'UPDATE', 'INSERT', 'DELETE', 'CREATE']
    inpref = os.path.join(indir, inname)
    viewdf = pd.read_csv(inpref + '_viewcomplete.csv')
    roledf = pd.read_csv(inpref + '_rolecomplete.csv')
    v1tov2 = {}
    r1tor2 = {}
    view_matches = viewdf[viewdf['ACM 2 Column'] != 'None']
    role_matches = roledf[roledf['ACM 2 Role'] != 'None']
    for vrow in view_matches.to_dict(orient='records'):
        v1tov2[vrow['ACM 1 Column']] = vrow['ACM 2 Column']
    
    for rrow in role_matches.to_dict(orient='records'):
        r1tor2[rrow['ACM 1 Role']] = rrow['ACM 2 Role']
    
    for i,row in enumerate(acmdf1.to_dict(orient='records')):
        if row['Role'] not in r1tor2:
            continue
        
        r2 = r1tor2[row['Role']]
        
        for j,c in enumerate(row):
            outfile = os.path.join(outdir, outname + '_row' + str(i) + '_col' + str(j) + '.txt')
            if os.path.exists(outfile):
                continue
            ent1 = row[c]
            if c not in v1tov2:
                continue
            v2 = v1tov2[c]
            #get the corresponding entry from acmdf2
            print("Getting entry for role, view: {}, {}".format(r2, v2))
            ent2 = acmdf2[acmdf2['Role'] == r2][v2].tolist()[0]
            #TODO: the below does not catch natural language cases that use query keywords, like 
            # 'Everything except SELECT privileges'
            print("sql_privs: {}".format(sql_privs))
            print("ent1: {}".format(ent1))
            #first, handle NaN cases
            if pd.isna(ent1) and pd.isna(ent2):
                ans = (False, 'No privileges have been given in either ACM.')
            elif ent1 == '[]' and ent2 == '[]':
                ans = (False, 'No privileges have been given in either ACM.')
            elif pd.isna(ent1) and not pd.isna(ent2):
                ans = (True, 'Blatant violation: no privileges should be given, but they are given in the second ACM: ' + ent2)
            elif ent1 == '[]' and ent2 != '[]':
                ans = (True, 'Blatant violation: no privileges should be given, but they are given in the second ACM: ' + ent2)
            elif not pd.isna(ent1) and pd.isna(ent2):
                ans = (False, 'No privileges given in second ACM, but privileges are given in the first: ' + ent1 + '. While this is not a violation, this may merit further investigation.')
            elif ent1 != '[]' and ent2 == '[]':
                 ans = (False, 'No privileges given in second ACM, but privileges are given in the first: ' + ent1 + '. While this is not a violation, this may merit further investigation.')
            elif not pd.isna(ent1) and not pd.isna(ent2) and ent1 != '[]' and ent2 != '[]':
                is_sql1 = functools.reduce(lambda a, b: a or b, [priv in ent1 for priv in sql_privs])
                is_sql2 = functools.reduce(lambda a, b: a or b, [priv in ent2 for priv in sql_privs])
                
                if not is_sql1 and not is_sql2:
                    ans = nlvsnl_privs(ent1, ent2, outdir, outname + '_row' + str(i) + '_col' + str(j) + '_chat.json')
                elif not is_sql1 and is_sql2:
                    ans = nlvssql_privs(ent1, ent2, outdir, outname + '_row' + str(i) + '_col' + str(j) + '_chat.json')
                elif is_sql1 and not is_sql2:
                    ans = nlvssql_privs(ent2, ent1, outdir, outname + '_row' + str(i) + '_col' + str(j) + '_chat.json')
                elif is_sql1 and is_sql2:
                    ans = sqlvssql_privs(ent1, ent2)
            
            with open(outfile, 'w+') as fh:
                print(ans, file=fh)

def privdiff_summary(acmpath1, acmpath2, indir, inname, outdir, outname):
    acmdf1 = pd.read_csv(acmpath1)
    acmdf2 = pd.read_csv(acmpath2)
    out_schema = ['ACM 1 Role', 'ACM 1 View', 'ACM 2 Role', 'ACM 2 View',
                  'ACM 1 Privilege', 'ACM 2 Privilege', 'Violation', 'Explanation']
    out_dct = {}
    for o in out_schema:
        out_dct[o] = []
    #now, we have all the answers already. we just need to put them together
    sql_privs = ['SELECT', 'GRANT', 'UPDATE', 'INSERT', 'DELETE', 'CREATE']
    inpref = os.path.join(indir, inname)
    viewdf = pd.read_csv(inpref + '_viewcomplete.csv')
    roledf = pd.read_csv(inpref + '_rolecomplete.csv')
    v1tov2 = {}
    r1tor2 = {}
    view_matches = viewdf[viewdf['ACM 2 Column'] != 'None']
    role_matches = roledf[roledf['ACM 2 Role'] != 'None']
    for vrow in view_matches.to_dict(orient='records'):
        v1tov2[vrow['ACM 1 Column']] = vrow['ACM 2 Column']
    
    for rrow in role_matches.to_dict(orient='records'):
        r1tor2[rrow['ACM 1 Role']] = rrow['ACM 2 Role']
    
    for i,row in enumerate(acmdf1.to_dict(orient='records')):
        if row['Role'] not in r1tor2:
            continue
        
        r2 = r1tor2[row['Role']]
        
        for j,c in enumerate(row):
            outfile = os.path.join(outdir, outname + '_row' + str(i) + '_col' + str(j) + '.txt')
            ent1 = row[c]
            if c not in v1tov2:
                continue
            v2 = v1tov2[c]
            #get the corresponding entry from acmdf2
            ent2 = acmdf2[acmdf2['Role'] == r2][v2].tolist()[0]
            # print("Processing file: {}".format(outfile))
            with open(outfile, 'r') as fh:
                viol_tup = literal_eval(fh.read())
            
            out_dct['ACM 1 Role'] += [row['Role']]
            out_dct['ACM 2 Role'] += [r2]
            out_dct['ACM 1 View'] += [c]
            out_dct['ACM 2 View'] += [v2]
            out_dct['ACM 1 Privilege'] += [ent1]
            out_dct['ACM 2 Privilege'] += [ent2]
            out_dct['Violation'] += [viol_tup[0]]
            out_dct['Explanation'] += [viol_tup[1]]
    
    out_df = pd.DataFrame(out_dct)
    out_df.to_csv(os.path.join(outdir, outname + '_privcomplete.csv'), index=False)

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
