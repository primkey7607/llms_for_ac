from acmdiff.diff_acms import gen_completediff
import pandas as pd

def test_diff(acmpath1, acmpath2, db_details):
    acmdf1 = pd.read_csv(acmpath1)
    acmdf2 = pd.read_csv(acmpath2)
    test_indir = 'compdiff_test'
    test_inname = 'difftest'
    test_outdir = 'fulldiff_test'
    test_outname = 'fulltest'
    
    gen_completediff(acmdf1, acmdf2, test_indir, test_inname, test_outdir, test_outname, db_details)

if __name__=='__main__':
    testpath1 = 'acm_datagen/dacview_test_type0acm.csv'
    testpath2 = 'acm_datagen/dacview_test_type3acm.csv'
    pg_details = {'user' : 'postgres', 'password' : 'dbrocks33', 'host' : '127.0.0.1', 'port' : '5432', 'database' : 'tpch_db'}
    test_diff(testpath1, testpath2, pg_details)

