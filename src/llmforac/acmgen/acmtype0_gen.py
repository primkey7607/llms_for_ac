import pandas as pd
import os
from ast import literal_eval

def privilege_to_policyblock_fmt(priv_st):
    out_lst = []
    if 'C' in priv_st:
        out_lst.append('CREATE')
    if 'R' in priv_st:
        out_lst.append('SELECT')
    if 'U' in priv_st:
        out_lst.append('UPDATE and INSERT')
    if 'D' in priv_st:
        out_lst.append('DELETE')
    
    out_st = ', '.join(out_lst)
    return out_st

def doc2type0acm(gt_path, dct_path, outname):
    out_schema = ['Role']
    out_dct = {}
    for o in out_schema:
        out_dct[o] = []
    
    df = pd.read_csv(gt_path)
    with open(dct_path, 'r') as fh:
        sent2sql = literal_eval(fh.read())
        
    #first, add all roles and views to the dictionary
    for row in df.to_dict(orient='records'):
        if row['isView']:
            view_query_sts = [sent2sql[s] for s in sent2sql if 'CREATE VIEW ' + row['Table'] in sent2sql[s]]
            view_query_st = view_query_sts[0]
        else:
            view_query_st = row['Table']
        
        if view_query_st not in out_dct:
            out_dct[view_query_st] = []
        
        if row['isUser']:
            user_sts = [sent2sql[s] for s in sent2sql if 'CREATE USER ' + row['Role'] in sent2sql[s]]
            user_st = user_sts[0]
        else:
            user_st = row['Role']
        
        if user_st not in out_dct['Role']:
            out_dct['Role'].append(user_st)
    
    #now fill in blanks for views
    for o in out_dct:
        if o == 'Role':
            continue
        out_dct[o] = [None] * len(out_dct['Role'])
    
    #now, construct a dictionary representing a dataframe for the ACM type 0 matrix...
    for row in df.to_dict(orient='records'):
        if row['isView']:
            view_query_sts = [sent2sql[s] for s in sent2sql if 'CREATE VIEW ' + row['Table'] in sent2sql[s]]
            view_query_st = view_query_sts[0]
        else:
            view_query_st = row['Table']
        
        privs = privilege_to_policyblock_fmt(row['Privilege'])
        
        if row['isUser']:
            user_sts = [sent2sql[s] for s in sent2sql if 'CREATE USER ' + row['Role'] in sent2sql[s]]
            user_st = user_sts[0]
        else:
            user_st = row['Role']
        
        #now, we need to know where to put the privileges.
        row_pos = out_dct['Role'].index(user_st)
        out_dct[view_query_st][row_pos] = privs
        
    
    acm_df = pd.DataFrame(out_dct)
    acm_df.to_csv(outname + '_type0acm.csv', index=False)

if __name__=='__main__':
    #as a test
    doc2type0acm('../dacview_wjson100_dacviews_gt.csv', '../t5train_jsons/dacview_wjson100_sent2sql.json', 'dacview_test')
    
            
        
    
    
        

