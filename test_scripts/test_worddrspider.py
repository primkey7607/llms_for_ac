import os
import shutil
from zipfile import ZipFile
import pandas as pd
from acmdiff.word_diff import gen_completediff
from evaldiff import perteq_mainstats

'''
Purpose: run and evaluate ACM differencing specifically for ACMs organized using Dr. Spider
'''

def pre_vs_post(nl_predir, nl_postdir, pert_names, db_name, db_details):
    
    #the convention used for the dir names is that they have a prefix, then the db_name
    #so, we should take this prefix + db_name as the prefix for all directories needed to store results and compute stats.
    outpref = nl_predir[:nl_predir.index(db_name)] + db_name
    
    for p in pert_names:
        ppref = outpref + 'word_nlvsnl_' + p
        pre_f = [os.path.join(nl_predir, f) for f in os.listdir(nl_predir) if f.endswith(p + '.csv')][0] #there should only be one element
        post_f = [os.path.join(nl_postdir, f) for f in os.listdir(nl_postdir) if f.endswith(p + '.csv')][0]
        pre_df = pd.read_csv(pre_f)
        post_df = pd.read_csv(post_f)
        
        indir = ppref + '_comps'
        inname = ppref
        outdir = ppref + '_diff'
        outname = ppref
        
        gen_completediff(pre_f, post_f, indir, inname, outdir, outname, db_details)
        

def pre_vs_sql(nl_predir, sql_dir, pert_names, db_name, db_details):
    #the convention used for the dir names is that they have a prefix, then the db_name
    #so, we should take this prefix + db_name as the prefix for all directories needed to store results and compute stats.
    outpref = nl_predir[:nl_predir.index(db_name)] + db_name
    
    for p in pert_names:
        ppref = outpref + 'word_nlvssql_' + p
        pre_f = [os.path.join(nl_predir, f) for f in os.listdir(nl_predir) if f.endswith(p + '.csv')][0] #there should only be one element
        sql_f = [os.path.join(sql_dir, f) for f in os.listdir(sql_dir) if f.endswith(p + '.csv')][0]
        pre_df = pd.read_csv(pre_f)
        sql_df = pd.read_csv(sql_f)
        
        indir = ppref + '_comps'
        inname = ppref
        outdir = ppref + '_diff'
        outname = ppref
        
        gen_completediff(pre_f, sql_f, indir, inname, outdir, outname, db_details)

def run_nlq(drspider_dir, nl_predir, nl_postdir, sql_dir, db_name, db_details):
    #we'll need to create a lot of directories, but I really think we should automatically name and create them.
    #they should not be parameters we need to think about.
    nlq_directories = [d for d in os.listdir(os.path.join(drspider_dir, "data")) if d.startswith("NLQ")]
    pert_names = [d[4:] for d in nlq_directories] # skip the NLQ_
    #add in the role and privilege perturbations
    pert_names += ['role_syn', 'roledesc_replace', 'priv_syn', 'priv_inf']
    db_details['database'] = db_name
    
    #for each perturbation, there is a pre NL ACM, a post NL ACM, and a SQL ACM.
    #experiment 1: pre vs post
    #experiment 2: pre vs SQL
    pre_vs_post(nl_predir, nl_postdir, pert_names, db_name, db_details)
    pre_vs_sql(nl_predir, sql_dir, pert_names, db_name, db_details)

def eval_prevspost(nl_predir, nl_postdir, sql_dir, pert_names, db_name, db_details):
    outpref = nl_predir[:nl_predir.index(db_name)] + db_name
    out_dct = {}
    out_dct['Perturbation'] = []
    for p in pert_names:
        #first, set up all the directories where the results would have been stored.
        ppref = outpref + 'word_nlvsnl_' + p
        pre_f = [os.path.join(nl_predir, f) for f in os.listdir(nl_predir) if f.endswith(p + '.csv')][0] #there should only be one element
        post_f = [os.path.join(nl_postdir, f) for f in os.listdir(nl_postdir) if f.endswith(p + '.csv')][0]
        sql_f = [os.path.join(sql_dir, f) for f in os.listdir(sql_dir) if f.endswith(p + '.csv')][0]
        indir = ppref + '_comps'
        inname = ppref
        outdir = ppref + '_diff'
        outname = ppref
        
        statsdir = ppref + '_stats'
        statspref = ppref
        view_res = os.path.join(indir, inname + '_viewcomplete.csv')
        role_res = os.path.join(indir, inname + '_rolecomplete.csv')
        priv_res = os.path.join(outdir, outname + '_privcomplete.csv')
        
        statsdf = perteq_mainstats(pre_f, post_f, sql_f, role_res, view_res, priv_res, statsdir, statspref)
        for row in statsdf.to_dict(orient='records'):
            out_dct['Perturbation'] += [p]
            for k in row:
                if k in out_dct:
                    out_dct[k] += [row[k]]
                else:
                    out_dct[k] = [row[k]]
    
    fullfname = outpref + 'word_nlvsnl_fullstats.csv'
    out_df = pd.DataFrame(out_dct)
    out_df.to_csv(fullfname, index=False)

def eval_prevssql(nl_predir, sql_dir, pert_names, db_name, db_details):
    outpref = nl_predir[:nl_predir.index(db_name)] + db_name
    out_dct = {}
    out_dct['Perturbation'] = []
    for p in pert_names:
        #first, set up all the directories where the results would have been stored.
        ppref = outpref + 'word_nlvssql_' + p
        pre_f = [os.path.join(nl_predir, f) for f in os.listdir(nl_predir) if f.endswith(p + '.csv')][0] #there should only be one element
        sql_f = [os.path.join(sql_dir, f) for f in os.listdir(sql_dir) if f.endswith(p + '.csv')][0]
        indir = ppref + '_comps'
        inname = ppref
        outdir = ppref + '_diff'
        outname = ppref
        
        statsdir = ppref + '_stats'
        statspref = ppref
        view_res = os.path.join(indir, inname + '_viewcomplete.csv')
        role_res = os.path.join(indir, inname + '_rolecomplete.csv')
        priv_res = os.path.join(outdir, outname + '_privcomplete.csv')
        
        statsdf = perteq_mainstats(pre_f, sql_f, sql_f, role_res, view_res, priv_res, statsdir, statspref)
        for row in statsdf.to_dict(orient='records'):
            out_dct['Perturbation'] += [p]
            for k in row:
                if k in out_dct:
                    out_dct[k] += [row[k]]
                else:
                    out_dct[k] = [row[k]]
    
    fullfname = outpref + 'word_nlvssql_fullstats.csv'
    out_df = pd.DataFrame(out_dct)
    out_df.to_csv(fullfname, index=False)
        
        

def eval_nlq(drspider_dir, nl_predir, nl_postdir, sql_dir, db_name, db_details):
    #the idea is, we want to construct the results directories for each perturbation, so we can create stats dirs for each perturbation.
    #then, the output will be a dataframe where each row is the stats for one perturbation.
    nlq_directories = [d for d in os.listdir(os.path.join(drspider_dir, "data")) if d.startswith("NLQ")]
    pert_names = [d[4:] for d in nlq_directories] # skip the NLQ_
    #add in the role and privilege perturbations
    pert_names += ['role_syn', 'roledesc_replace', 'priv_syn', 'priv_inf']
    db_details['database'] = db_name
    
    eval_prevspost(nl_predir, nl_postdir, sql_dir, pert_names, db_name, db_details)
    eval_prevssql(nl_predir, sql_dir, pert_names, db_name, db_details)

#ChatGPT-generated
def copy_and_zip(source_directories, destination_directory, zip_filename, additional_files=None, source_directory_names=None):
    # Create the destination directory if it doesn't exist
    if not os.path.exists(destination_directory):
        os.makedirs(destination_directory)

    # Copy directories to the destination directory
    for i, source_directory in enumerate(source_directories):
        source_name = os.path.basename(source_directory) if source_directory_names is None else source_directory_names[i]
        shutil.copytree(source_directory, os.path.join(destination_directory, source_name))

    # Copy additional files to the destination directory
    if additional_files:
        for file_path in additional_files:
            shutil.copy2(file_path, destination_directory)

    # Zip the copied directories and additional files
    with ZipFile(zip_filename, 'w') as zipf:
        for root, _, files in os.walk(destination_directory):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, destination_directory)
                zipf.write(file_path, arcname=arcname)

def zip_nlq(drspider_dir, nl_predir, nl_postdir, sql_dir, db_name):
    nlq_directories = [d for d in os.listdir(os.path.join(drspider_dir, "data")) if d.startswith("NLQ")]
    pert_names = [d[4:] for d in nlq_directories] # skip the NLQ_
    #add in the role and privilege perturbations
    pert_names += ['role_syn', 'roledesc_replace', 'priv_syn', 'priv_inf']
    
    res_dirs = []
    stats_dirs = []
    fullstats_files = []
    
    outpref = nl_predir[:nl_predir.index(db_name)] + db_name
    for p in pert_names:
        #first, set up all the directories where the results would have been stored.
        ppref = outpref + 'word_nlvsnl_' + p
        ppref2 = outpref + 'word_nlvssql_' + p
        
        indir = ppref + '_comps'
        outdir = ppref + '_diff'
        
        indir2 = ppref2 + '_comps'
        outdir2 = ppref2 + '_diff'
        
        res_dirs += [indir, indir2, outdir, outdir2]
        
        statsdir = ppref + '_stats'
        statsdir2 = ppref2 + '_stats'
        
        stats_dirs += [statsdir, statsdir2]
        
    
    fullfname = outpref + 'word_nlvssql_fullstats.csv'
    fullfname2 = outpref + 'word_nlvsnl_fullstats.csv'
    fullstats_files += [fullfname, fullfname2]
    
    copy_and_zip(res_dirs + stats_dirs, outpref + 'word_allresults', outpref + 'word_allresults.zip', additional_files=fullstats_files)

if __name__=='__main__':
    drspider_dir = os.path.expanduser('~/diagnostic-robustness-text-to-sql')
    #test credentials below. replace with database credentials where your spider databases are stored
    pg_details = {'user' : 'postgres', 'password' : 'dbrocks33', 'host' : '127.0.0.1', 'port' : '5432', 'database' : 'bike_1'}
    db_name = 'orchestra'
    nl_predir = 'nlpert_10_10_' + db_name + '_nl'
    nl_postdir = 'nlpert_10_10_' + db_name + '_post'
    sql_dir = 'nlpert_10_10_' + db_name + '_sql'
    run_nlq(drspider_dir, nl_predir, nl_postdir, sql_dir, db_name, pg_details)
    eval_nlq(drspider_dir, nl_predir, nl_postdir, sql_dir, db_name, pg_details)
    
    

