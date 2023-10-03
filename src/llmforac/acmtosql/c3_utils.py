import os
from ast import literal_eval
import pandas as pd
import json

#efficient version of getting tabcols
def gen_c3dct(tab_dct, statement, outname):
    out_dct = {}
    out_dct['schema'] = {}
    out_dct['db_contents'] = {}
    out_dct['question'] = statement
    out_dct['fk'] = []
    for tname in tab_dct:
        cols = tab_dct[tname]
        out_dct['schema'][tname] = cols
        out_dct['db_contents'][tname] = {}
        for i,c in enumerate(cols):
            out_dct['db_contents'][tname][int(i)] = None

    out_obj = [out_dct]

    with open(outname + '.json', 'w+') as fh:
        json.dump(out_obj, fh, indent=2)

def get_tablecols_from_csvs(csv_dir):
    tbl_dct = {}
    full_dir = os.path.expanduser(csv_dir)
    for f in os.listdir(full_dir):
        if f.endswith('.csv'):
            tname = f[:-4]
            fullf = os.path.join(full_dir, f)
            df = pd.read_csv(fullf, nrows=0)
            tbl_dct[tname] = df.columns.tolist()
    
    return tbl_dct

def get_schema(schema_details={'mode' : 'csv', 'path' : '~/tpch-kit/scale1data/tpchcsvs'}):
    tbl_dct = {}
    if schema_details['mode'] == 'csv':
        tbl_dct = get_tablecols_from_csvs(schema_details['path'])
    
    return tbl_dct

def fmt_c3dcts(sentences : list, outdir, prefix='c3test_', schema_details={'mode' : 'csv', 'path' : '~/tpch-kit/scale1data/tpchcsvs'}):
    #first, make the directory
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    
    schema = get_schema(schema_details)
    
    for i,sent in enumerate(sentences):
        outname = os.path.join(outdir, prefix) + 'sentence' + str(i)
        gen_c3dct(schema, sent, outname)

def generate_allprompts(indir, inprefix, outdir, outprefix):
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    
    for f in os.listdir(indir):
        if f.startswith(inprefix) and f.endswith('.json'):
            suffix = f[len(inprefix):]
            fullf = os.path.join(indir, f)
            cmd = "python c3files/c3_prompt_generate.py --input_dataset_path " + fullf
            outpath = os.path.join(outdir, outprefix + suffix)
            cmd += " --output_dataset_path " + outpath
            os.system(cmd)

def generate_allsql(indir, inprefix, outdir, outprefix):
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    # sample command: 
    #python generate_sqls_by_gpt3.5.py --input_dataset_path ../testtemp1.json --output_dataset_path ../testsql1.json
    for f in os.listdir(indir):
        if f.startswith(inprefix) and f.endswith('.json'):
            suffix = f[len(inprefix):-5]
            outfile = os.path.join(outdir, outprefix + suffix + '.sql')
            if os.path.exists(outfile):
                continue
            
            fullf = os.path.join(indir, f)
            cmd = "python c3files/generate_sqls_by_gpt3.5.py --input_dataset_path " + fullf
            cmd += " --output_dataset_path " + outfile
            os.system(cmd)

def sql2sent(sentences, sqldir, sql_prefix):
    out_dct = {}
    for f in os.listdir(sqldir):
        if f.endswith('.sql'):
            start = f.index('sentence') + len('sentence')
            end = -4
            sentno_st = f[start:end]
            print('Sentence number (should be an integer): {}'.format(sentno_st))
            sentno = int(sentno_st)
            cur_sent = sentences[sentno]
            fullf = os.path.join(sqldir, f)
            with open(fullf, 'r') as fh:
                sql_st = fh.read()
            out_dct[cur_sent] = sql_st
    
    return out_dct

def c3_e2e(sentences : list, \
           outdir='c3test_raw', \
           inprefix='c3test_', \
           outprefix='c3complete', \
           compdir='c3complete_test', \
           sql_prefix='c3sql_test', \
           sqldir='c3sql_test',
           dctname='c3test_nl2sql.json'):
    fmt_c3dcts(sentences, outdir, prefix=inprefix)
    generate_allprompts(outdir, inprefix, compdir, outprefix)
    generate_allsql(compdir, outprefix, sqldir, sql_prefix)
    
    sent2sql = sql2sent(sentences, sqldir, sql_prefix)
    with open(dctname, 'w+') as fh:
        print(sent2sql, file=fh)
    return sent2sql
        
    
    
    
    
if __name__=='__main__':
    with open('type0acm_allsentences.txt', 'r') as fh:
        test_lst = literal_eval(fh.read())
    # fmt_c3dcts(test_lst, 'c3test_raw')
    # generate_allprompts('c3test_raw', 'c3test_', 'c3complete_test', 'c3complete')
    # generate_allsql('c3complete_test', 'c3complete', 'c3sql_test', 'c3sql_')
    sent2sql = sql2sent(test_lst, 'c3sql_test', 'c3sql_')
    dctname = 'c3test_nl2sql.json'
    with open(dctname, 'w+') as fh:
        print(sent2sql, file=fh)
    