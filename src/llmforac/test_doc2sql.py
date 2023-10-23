from doc2db.doc2sql import doc2sql

def test_doc2sql(docpath):
    test_outdir = 'c3doctest_raw'
    test_inprefix = 'c3doctest_'
    test_outprefix = 'c3doccomplete'
    test_compdir = 'c3doccomplete_test'
    test_sql_prefix = 'c3docsql_test'
    test_sqldir = 'c3docsql_test'
    test_dctname = 'c3doctest_nl2sql.json'
    pg_details = {'user' : 'postgres', 'password' : 'dbrocks33', 'host' : '127.0.0.1', 'port' : '5432', 'database' : 'tpch_db'}
    doc2sql(docpath, test_outdir, test_inprefix, test_outprefix, test_compdir, test_sql_prefix, test_sqldir, test_dctname, db_details=pg_details)

if __name__=='__main__':
    test_doc = 'q6test/q6dacview100_dacviews_doc.txt'
    test_doc2sql(test_doc)