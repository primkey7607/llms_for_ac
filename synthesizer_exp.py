import os
import copy
import pandas as pd
from ast import literal_eval
import json
from type2tosql import t2sent_to_t1
from utils.db_utils import views_are_equal, \
                           fixed_sql, \
                           fix_pgcaps, \
                           sqlite2postgres, \
                           PostgresAPI

def spider_to_postgres(spider_dbs : list, pg_details : dict):
    for sdb in spider_dbs:
        db_name = sdb.split(os.sep)[-1].split('.')[0]
        pg_details['database'] = db_name
        sqlite2postgres(sdb, pg_details)
    

#we generate a test that specifically checks whether the view definitions
#found by LLM4AC matches those of the ground truth.
def gen_type2spider(spider_dir : str, pg_details : dict, spider_nl : list, outname : str):
    sp_queries = os.path.join(spider_dir, 'train_spider.json')
    with open(sp_queries, 'r') as fh:
        sp_dct = json.load(fh)
    view_dct = {}
    for i,nl in enumerate(spider_nl):
        vname = 'newview' + str(i)
        print(nl)
        cur_query = [ent['query'] for ent in sp_dct if ent['question'] == nl][0]
        cur_db = [ent['db_id'] for ent in sp_dct if ent['question'] == nl][0]
        fullnl = 'Create a view called ' + vname + ' for the following query: ' + nl
        fullquery = 'CREATE VIEW ' + vname + ' AS ' + cur_query
        view_dct[fullnl] = (cur_db, fullquery)
    
    with open(outname + '_simplebench.json', 'w+') as fh:
        print(view_dct, file=fh)

def run_type2spider(dct_path : str, pg_details : dict, out_pref : str):
    with open(dct_path, 'r') as fh:
        desc2gt = literal_eval(fh.read())
    
    for i,desc in enumerate(desc2gt):
        outname = out_pref + '_entry' + str(i) + '.json'
        cur_db = desc2gt[desc][0]
        if os.path.exists(outname):
            continue
        db_details = copy.deepcopy(pg_details)
        db_details['database'] = cur_db
        pgapi = PostgresAPI(db_details)
        db_schema = pgapi.get_schema()
        new_query = t2sent_to_t1(desc, db_schema)
        new_ent = {}
        new_ent[desc] = (cur_db, new_query)
        with open(outname, 'w+') as fh:
            print(new_ent, file=fh)

def test_type2spider(in_pref, pg_details, dct_path, outname):
    correct = []
    incorrect = []
    
    gen_dct = {}
    gt_dct = {}
    for f in os.listdir():
        if f.startswith(in_pref) and f.endswith('.json') and 'entry' in f:
            with open(f, 'r') as fh:
                cur_ent = literal_eval(fh.read())
                for k in cur_ent:
                    gen_dct[k] = cur_ent[k]
    
    with open(dct_path, 'r') as fh:
        gt_dct = literal_eval(fh.read())
    
    gengt_zip = [(gen_dct[k], gt_dct[k]) for k in gen_dct]
    gen_cols = [t[0] for t in gengt_zip]
    gt_cols = [t[1] for t in gengt_zip]
    
    for i,tup in enumerate(gen_cols):
        c = tup[1]
        cur_db = tup[0]
        db_details = copy.deepcopy(pg_details)
        db_details['database'] = cur_db
        # fixed_c = fixed_sql(c)
        # fixed_c = fix_pgcaps(fixed_c, db_details)
        fixed_c = c
        print("Fixed String: {}".format(fixed_c))
        gt_c = gt_cols[i]
        fixed_gt = fix_pgcaps(gt_c, db_details)
        are_same = views_are_equal(fixed_c, fixed_gt, db_details)
        if are_same:
            correct.append((fixed_c, fixed_gt))
        else:
            incorrect.append((fixed_c, fixed_gt))
    
    with open(outname + '_correct.txt', 'w+') as fh:
        print(correct, file=fh)
    
    with open(outname + '_incorrect.txt', 'w+') as fh:
        print(incorrect, file=fh)

def gen_type2tpch(query_dir : str, qnums : list, pg_details : dict):
    desc2gt = {}
    for n in qnums:
        pref = 'tpch_q' + str(n)
        dfile = os.path.join(query_dir, pref + '_desc.txt')
        qfile = os.path.join(query_dir, pref + '.sql')
        with open(dfile, 'r') as fh:
            desc = fh.read()
        
        with open(qfile, 'r') as fh:
            query = fh.read()
        
        desc2gt[desc] = query
        
    with open('tpch_simplebench.json', 'w+') as fh:
        print(desc2gt, file=fh)

def run_type2tpch(dct_path : str, pg_details : dict, out_pref : str):
    with open(dct_path, 'r') as fh:
        desc2gt = literal_eval(fh.read())
    
    pgapi = PostgresAPI(pg_details)
    
    for i,desc in enumerate(desc2gt):
        outname = out_pref + '_entry' + str(i) + '.json'
        if os.path.exists(outname):
            continue
        db_schema = pgapi.get_schema()
        new_query = t2sent_to_t1(desc, db_schema)
        new_ent = {}
        new_ent[desc] = new_query
        with open(outname, 'w+') as fh:
            print(new_ent, file=fh)

def test_type2tpch(in_pref, pg_details, dct_path, outname):
    correct = []
    incorrect = []
    
    gen_dct = {}
    gt_dct = {}
    for f in os.listdir():
        if f.startswith(in_pref) and f.endswith('.json') and 'entry' in f:
            with open(f, 'r') as fh:
                cur_ent = literal_eval(fh.read())
                for k in cur_ent:
                    gen_dct[k] = cur_ent[k]
    
    with open(dct_path, 'r') as fh:
        gt_dct = literal_eval(fh.read())
    
    gengt_zip = [(gen_dct[k], gt_dct[k]) for k in gen_dct]
    gen_cols = [t[0] for t in gengt_zip]
    gt_cols = [t[1] for t in gengt_zip]
    
    for i,c in enumerate(gen_cols):
        #assume we have fixed the query strings manually for now,
        #since the space of errors can be wide...
        # fixed_c = fixed_sql(c)
        # fixed_c = fix_pgcaps(fixed_c, pg_details)
        fixed_c = c
        print("Fixed String: {}".format(fixed_c))
        gt_c = gt_cols[i]
        fixed_gt = fix_pgcaps(gt_c, pg_details)
        are_same = views_are_equal(fixed_c, fixed_gt, pg_details)
        if are_same:
            correct.append((fixed_c, fixed_gt))
        else:
            incorrect.append((fixed_c, fixed_gt))
    
    with open(outname + '_correct.txt', 'w+') as fh:
        print(correct, file=fh)
    
    with open(outname + '_incorrect.txt', 'w+') as fh:
        print(incorrect, file=fh)
        

if __name__=='__main__':
    pg_details = {'user' : 'postgres', 'password' : 'dbrocks33', 'host' : '127.0.0.1', 'port' : '5432', 'database' : 'tpch_db'}
    #NOTE : spider_dir should point to the spider/ directory unzipped from the spider.zip file 
    # containing the Spider NL2SQL benchmark downloaded from here: https://yale-lily.github.io/spider
    spider_dir = os.path.expanduser('~/Documents/spider')
    
    #Full Spider Part of Synthesizer Benchmark
    sp_single_projections = ['How many heads of the departments are older than 56 ?',
                          'How many acting statuses are there?',
                          'Give me the dates when the max temperature was higher than 85.',
                          'What are the names of stations that have latitude lower than 37.5?',
                          'What are the names of the heads who are born outside the California state?'
                          ]
    sp_mult_projections = ['List the name, born state and age of the heads of departments ordered by age.',
                        'List the creation year, name and budget of each department.',
                        'Give me the start station and end station for the trips with the three oldest id.',
                        'What is the name and capacity for the stadium with highest average attendance?'
                        'What is the average, maximum, and minimum budget for all movies before 2000.'
                        ]
    sp_single_agg = ['For each city, what is the highest latitude for its stations?',
                     'What is the publisher with most number of books?']
    sp_single_join = ['What are the distinct creation years of the departments managed by a secretary born in state \'Alabama\'?',
                      'Which department has more than 1 head at a time? List the id, name and the number of heads.',
                      'What are the id and name of the stations that have ever had more than 12 bikes available?',
                      'What is the average latitude and longitude of the starting points of all trips?',
                      "For each trip, return its ending station's installation date."]
    sp_multi_join = ["What are the distinct creation years of the departments managed by a secretary born in state 'Alabama'?"]
    sp_cte = ['What are the ids of stations that are located in San Francisco and have average bike availability above 10.',
              'Find the zip code in which the average mean visibility is lower than 10.',
              'Find the ids and names of stations from which at least 200 trips started.',
              'What are the ids of stations that have latitude above 37.4 and never had bike availability below 7?']
    sp_single_where = ['What are all company names that have a corresponding movie directed in the year 1999?',
                       'What is the average, maximum, and minimum budget for all movies before 2000.',
                       'Which bike traveled the most often in zip code 94002?',
                       'What are the dates in which the mean sea level pressure was between 30.3 and 31?',
                       'What is the title and director for the movie with highest worldwide gross in the year 2000 or before?']
    sp_multi_where = ['How many days had both mean humidity above 50 and mean visibility above 8?',
                      'On which day has it neither been foggy nor rained in the zip code of 94107?']
    
    #Full TPC-H Part of Synthesizer Benchmark
    tpch_single_projections = []
    tpch_mult_projections = [1]
    tpch_single_agg = [3]
    tpch_single_join = []
    tpch_multi_join = [10]
    tpch_nested = [4]
    tpch_cte = []
    tpch_single_where = []
    tpch_multi_where = [2]
    
    
    
    spider_nl = ['List the name, born state and age of the heads of departments ordered by age.',
                 'What are the distinct creation years of the departments managed by a secretary born in state \'Alabama\'?',
                 'What are the ids of stations that are located in San Francisco and have average bike availability above 10.',
                 'What are all company names that have a corresponding movie directed in the year 1999?']
    
    tpch_dir = 'tpch_queries'
    queries = [1, 2, 3, 4, 10]
    ''' TPC-H Test Generation '''
    # gen_type2tpch(tpch_dir, queries, pg_details)
    ''' TPC-H Test Run (with ChatGPT) '''
    # run_type2tpch('tpch_simplebench.json', pg_details, 'tpch_simple')
    ''' TPC-H Test Evaluation '''
    test_type2tpch('tpch_simple', pg_details, 'tpch_simplebench.json', 'tpch_simplestats')
    ''' Spider Database Generation '''
    # spider_to_postgres([os.path.join(spider_dir, 'database', 'department_management/department_management.sqlite'),
    #                     os.path.join(spider_dir, 'database', 'bike_1', 'bike_1.sqlite'),
    #                     os.path.join(spider_dir, 'database', 'culture_company', 'culture_company.sqlite')], pg_details)
    ''' Spider Test Generation '''
    # gen_type2spider(spider_dir, pg_details, spider_nl, 'spider')
    # run_type2spider('spider_simplebench.json', pg_details, 'spider_simple')
    test_type2spider('spider_simple', pg_details, 'spider_simplebench.json', 'spider_simplestats')
    
    
