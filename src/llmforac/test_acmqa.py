from acmqa.acmqa import llm_answer

def ask_question(question, db_details):
    chatdir = 'test_acmqa_chat'
    chatpref = 'test_qa_chat'
    outdir = 'test_acmqa'
    outname = 'test_acmqa'
    
    resps = llm_answer(question, chatdir, chatpref, outdir, outname, db_details)
    print("Received Responses: {}".format(resps))

if __name__=='__main__':
    pg_details = {'user' : 'postgres', 'password' : 'dbrocks33', 'host' : '127.0.0.1', 'port' : '5432', 'database' : 'tpch_db'}
    question = 'Do database security administrators have access to customer information in this database?'
    ask_question(question, pg_details)

