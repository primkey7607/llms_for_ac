import pandas as pd
import os
from ast import literal_eval
from perturb import priv_inf, priv_syn, role_syn, roledesc_replace

'''
Purpose: take the pre-perturbed Dr. Spider datasets and, using the same naming conventions,
generate ACMs with role and privilege perturbations.
It shouldn't matter which pre-perturbed ACM we choose, because the roles and privileges should all be the same.'
'''

def create_privsent(priv_lst):
    if priv_lst == []:
        return 'This role should not be given any privileges on this database view.'
    sent = 'This role has ' + ', '.join(priv_lst) + ' privileges on this database view.'
    return sent

def run_role_pert(basedf, outdir, db_name, pf):
    resdir = outdir + '_' + db_name + '_' + pf.__name__ + '_rolesents'
    if not os.path.exists(resdir):
        os.mkdir(resdir)
    
    orig_roles = basedf['Role'].tolist()
    for i,r in enumerate(orig_roles):
        resfile = os.path.join(resdir, 'role' + str(i) + '.txt')
        if os.path.exists(resfile):
            continue
        if 'has role' in r:
            cur_role = r.split(' ')[-1]
        else:
            cur_role = r
        new_roles = pf(cur_role)
        
        with open(resfile, 'w+') as fh:
            print(new_roles, file=fh)

def construct_roledf(basedf, outdir, db_name, pf):
    resdir = outdir + '_' + db_name + '_' + pf.__name__ + '_rolesents'
    out_dct = {}
    for c in basedf.columns:
        if c == 'Role':
            out_dct[c] = []
        else:
            out_dct[c] = basedf[c].tolist()
    
    orig_roles = basedf['Role'].tolist()
    for i,r in enumerate(orig_roles):
        resfile = os.path.join(resdir, 'role' + str(i) + '.txt')
        with open(resfile, 'r') as fh:
            new_roles = literal_eval(fh.read())
        if 'has role' in r:
            cur_r_pts = r.split(' ')
            user_pts = cur_r_pts[:-1]
            nr_pts = new_roles[0].split(' ')
            if len(nr_pts) > 4: #TODO: this is a hack. how would we tell the difference between a role phrase, and a description
                new_r = ' '.join(user_pts) + ' with the following description: ' + new_roles[0]
            else:
                new_r = ' '.join(user_pts) + ' ' + new_roles[0]
        else:
            new_r = new_roles[0]
            
        out_dct['Role'] += [new_r]
    
    out_df = pd.DataFrame(out_dct)
    return out_df
            
def run_priv_pert(basedf, outdir, db_name, pf):
    resdir = outdir + '_' + db_name + '_' + pf.__name__ + '_privsents'
    if not os.path.exists(resdir):
        os.mkdir(resdir)
    
    for i,row in enumerate(basedf.to_dict(orient='records')):
        for j,c in enumerate(row):
            if c == 'Role':
                continue
            resfile = os.path.join(resdir, 'role' + str(i) + 'view' + str(j) + '.txt')
            if os.path.exists(resfile):
                continue
            cur_priv_st = row[c]
            cur_priv = literal_eval(cur_priv_st)
            cur_priv_sent = create_privsent(cur_priv)
            new_privs = pf(cur_priv_sent)
            with open(resfile, 'w+') as fh:
                print(new_privs, file=fh)
            
def construct_privdf(basedf, outdir, db_name, pf):
    resdir = outdir + '_' + db_name + '_' + pf.__name__ + '_privsents'
    out_dct = {}
    for c in basedf.columns:
        if c == 'Role':
            out_dct[c] = basedf[c].tolist()
        else:
            out_dct[c] = []
    
    for i,row in enumerate(basedf.to_dict(orient='records')):
        for j,c in enumerate(row):
            if c == 'Role':
                continue
            resfile = os.path.join(resdir, 'role' + str(i) + 'view' + str(j) + '.txt')
            with open(resfile, 'r') as fh:
                new_privs = literal_eval(fh.read())
            
            out_dct[c] += [new_privs[0]]
    
    out_df = pd.DataFrame(out_dct)
    return out_df
    
    

def generate_acms_todir(base_path, query_path, db_name, outdir, outpref, role_perts, priv_perts):
    #the design is: we'll pick a base csv from which to perturb the roles and privileges.
    #then, we'll perturb the roles and privileges here.
    #then, we'll create a new dataframe
    #we'll write the original to outpref + db_name + _nl with new name outpref + func_name
    #we'll then write the perturbed version to outpref + db_name + _post
    #and we'll write the SQL version to outpref + db_name + _sql
    #this way, these perturbations will be executed alongside all the others.
    fullnldir = outdir + '_' + db_name + '_nl'
    fullpostdir = outdir + '_' + db_name + '_post'
    fullsqldir = outdir + '_' + db_name + '_sql'
    nlpref = outpref + '_nl'
    postpref = outpref + '_post'
    sqlpref = outpref + '_sql'
    basedf = pd.read_csv(base_path)
    sqldf = pd.read_csv(query_path)
    
    for pf in role_perts:
        func_name = pf.__name__
        pre_f = os.path.join(fullnldir, nlpref + '_' + func_name + '.csv')
        post_f = os.path.join(fullpostdir, postpref + '_' + func_name + '.csv')
        sql_f = os.path.join(fullsqldir, sqlpref + '_' + func_name + '.csv')
        
        #first, the basedf is the pre-perturbation version of this role perturbation
        basedf.to_csv(pre_f, index=False)
        
        #perturb the basedf to get a new df with perturbed roles
        run_role_pert(basedf, outdir, db_name, pf)
        new_roledf = construct_roledf(basedf, outdir, db_name, pf)
        new_roledf.to_csv(post_f, index=False)
        
        #sqldf is also the SQL version of this new df.
        sqldf.to_csv(sql_f, index=False)
    
    for pf in priv_perts:
        func_name = pf.__name__
        pre_f = os.path.join(fullnldir, nlpref + '_' + func_name + '.csv')
        post_f = os.path.join(fullpostdir, postpref + '_' + func_name + '.csv')
        sql_f = os.path.join(fullsqldir, sqlpref + '_' + func_name + '.csv')
        
        #first, the basedf is the pre-perturbation version of this role perturbation
        basedf.to_csv(pre_f, index=False)
        
        #perturb the basedf to get a new df with perturbed roles
        run_priv_pert(basedf, outdir, db_name, pf)
        new_privdf = construct_privdf(basedf, outdir, db_name, pf)
        new_privdf.to_csv(post_f, index=False)
        
        #sqldf is also the SQL version of this new df.
        sqldf.to_csv(sql_f, index=False)
        

if __name__=='__main__':
    db_name = 'orchestra'
    outdir = 'nlpert_10_10'
    outpref = 'nlpert_10_10'
    base_path = outdir + '_' + db_name + '_nl/' + outpref + '_nl_others.csv'
    query_path = outdir + '_' + db_name + '_sql/' + outpref + '_sql_others.csv'
    role_perts = [role_syn, roledesc_replace]
    priv_perts = [priv_inf, priv_syn]
    generate_acms_todir(base_path, query_path, db_name, outdir, outpref, role_perts, priv_perts)



