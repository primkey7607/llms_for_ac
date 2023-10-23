import spacy
from c3_utils import c3_e2e

'''Current Assumption: each sentence in a document can be parsed into a 
single entry of a Type X ACM. In reality, things can get much worse much faster,
so we will need to relax this.

Also, we assume the spacy model en_core_web_sm has already been downloaded.
'''

def doc2sent(docpath):
    nlp = spacy.load("en_core_web_sm")
    with open(docpath, 'r') as fh:
        doc_st = fh.read()
    
    doc = nlp(doc_st)
    sentences = [sent.text for sent in doc.sents]
    return sentences

def doc2sql(docpath, c3_outdir, \
            c3_inprefix, \
            c3_outprefix, \
            c3_compdir, \
            c3_sql_prefix, \
            c3_sqldir, \
            c3_dctname,
            db_details=None):
    
    sentences = doc2sent(docpath)
    nl2sql = c3_e2e(sentences, outdir=c3_outdir, 
                    inprefix=c3_inprefix, 
                    outprefix=c3_outprefix,
                    compdir=c3_compdir,
                    sql_prefix=c3_sql_prefix,
                    sqldir=c3_sqldir,
                    dctname=c3_dctname,
                    db_details=db_details)
    
    return nl2sql

if __name__=='__main__':
    test_doc = '../q6test/q6dacview100_dacviews_doc.txt'
    test_outdir = 'c3doctest_raw'
    test_inprefix = 'c3doctest_'
    test_outprefix = 'c3doccomplete'
    test_compdir = 'c3doccomplete_test'
    test_sql_prefix = 'c3docsql_test'
    test_sqldir = 'c3docsql_test'
    test_dctname = 'c3doctest_nl2sql.json',
    pg_details = {'user' : 'postgres', 'password' : 'dbrocks33', 'host' : '127.0.0.1', 'port' : '5432', 'database' : 'tpch_db'}
    doc2sql(test_doc, test_outdir, test_inprefix, test_outprefix, test_compdir, test_sql_prefix, test_sqldir, test_dctname, db_details=pg_details)