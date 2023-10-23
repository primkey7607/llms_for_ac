from sqltoacm.sqltoacm import db_vs_acm
import pandas as pd

def test_db2acm(acmpath, db_details):
    acmdf = pd.read_csv(acmpath)
    indir = 'test_sqltoacm_rolesviews'
    inpref = 'test_sqltoacm'
    outdir = 'test_sqltoacm_privs'
    outpref = 'test_sqltoacm_priv'
    db_vs_acm(acmdf, indir, inpref, outdir, outpref, db_details)

if __name__=='__main__':
    pg_details = {'user' : 'postgres', 'password' : 'dbrocks33', 'host' : '127.0.0.1', 'port' : '5432', 'database' : 'tpch_db'}
    testpath = 'acm_datagen/dacview_test_type3acm.csv'
    test_db2acm(testpath, pg_details)
    

