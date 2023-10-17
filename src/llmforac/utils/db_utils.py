import psycopg2
import sqlite3
from sqlalchemy import create_engine
import pandas as pd
from abc import ABC, abstractmethod
import traceback

class DBAPI(ABC):
    @abstractmethod
    def __init__(self, con_details : dict):
        #we store the con_details because if an error happens, we may have to
        #reconnect, and storing them makes it more convenient.
        self.con_details = con_details
        self.con = self.connect()
    
    @abstractmethod
    def connect(self):
        raise Exception("Must be implemented")
    
    @abstractmethod
    def query(self, query_st : str):
        raise Exception("Must be implemented")
    
    @abstractmethod
    def execute(self, stmt_st : str):
        raise Exception("Must be implemented")
    
    @abstractmethod
    def tbl_to_df(self, tbl_name : str):
        raise Exception("Must be implemented")
    
    @abstractmethod
    def df_to_tbl(self, df_path : str, tbl_name : str):
        raise Exception("Must be implemented")
    
    @abstractmethod
    def memdf_to_tbl(self, df, tbl_name : str):
        raise Exception("Must be implemented")
    
    @abstractmethod
    def get_schema(self):
        raise Exception("Must be implemented")

class PostgresAPI(DBAPI):
    
    def __init__(self, con_details : dict):
        self.con_details = con_details
        self.con = self.connect()
    
    def connect(self):
        con = psycopg2.connect(user=self.con_details['user'], 
                                    password=self.con_details['password'], 
                                    host=self.con_details['host'], 
                                    port=self.con_details['port'], 
                                    database=self.con_details['database'])
        return con
    
    def query(self, query_st):
        cur = self.con.cursor()
        try:
            cur.execute(query_st)
            out_tups = cur.fetchall()
            return out_tups
        except:
            tb = traceback.format_exc()
            print(tb)
            #don't assume autocommit--reinitialize the connection
            cur.close()
            self.con.close()
            self.con = self.connect()
            return tb
    
    def execute(self, stmt_st):
        cur = self.con.cursor()
        try:
            cur.execute(stmt_st)
            self.con.commit()
            cur.close()
            self.con.close()
            self.con = self.connect()
            return ''
        except:
            tb = traceback.format_exc()
            print(tb)
            #don't assume autocommit--reinitialize the connection
            cur.close()
            self.con.close()
            self.con = self.connect()
            return tb
    
    def teardown(self):
        # cur = self.con.cursor()
        # get_myroles = "select rolname from pg_authid where rolname != 'postgres' and not rolname ilike 'pg' || '%';"
        # cur.execute(get_myroles)
        # cur_rolelst = [tup[0] for tup in cur.fetchall()]
        # drop_st = ''
        # for role in cur_rolelst:
        #     drop_st +=  'drop owned by ' + role + '; '
        #     drop_st += 'drop role ' + role + '; '
        
        # if drop_st != '':
        #     cur.execute(drop_st)
        #     self.con.commit()
        
        #also drop views
        view_drop = '''SELECT 'DROP VIEW ' || table_name || ';' FROM information_schema.views WHERE table_schema NOT IN ('pg_catalog', 'information_schema') AND table_name !~ '^pg_';'''
        drop_stmts = self.query(view_drop)
        for ds in drop_stmts:
            self.execute(ds[0])
        # cur.close()
        # self.con.close()
    
    def tbl_to_df(self, tbl_name):
        eng_st = 'postgresql://postgres:' + self.con_details['password'] + '@localhost:' + self.con_details['port'] + '/' + self.con_details['database']
        eng = create_engine(eng_st)
        tbl_query = 'select * from ' + tbl_name + ';'
        df = pd.read_sql_query(tbl_query, con=eng)
        return df
    
    def df_to_tbl(self, df_path, tbl_name):
        eng_st = 'postgresql://postgres:' + self.con_details['password'] + '@localhost:' + self.con_details['port'] + '/' + self.con_details['database']
        eng = create_engine(eng_st)
        df = pd.read_csv(df_path)
        df.to_sql(tbl_name, eng, if_exists='replace')
    
    def memdf_to_tbl(self, df, tbl_name):
        eng_st = 'postgresql://postgres:' + self.con_details['password'] + '@localhost:' + self.con_details['port'] + '/' + self.con_details['database']
        eng = create_engine(eng_st)
        df.to_sql(tbl_name, eng, if_exists='replace')
    
    def get_schema(self):
        #the below also gets views, which we don't want.
        # schema_details = self.query('SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = \'public\';')
        #instead, we only get base tables using the following query
        schema_details = self.query('SELECT INFORMATION_SCHEMA.COLUMNS.table_name, INFORMATION_SCHEMA.COLUMNS.column_name, INFORMATION_SCHEMA.COLUMNS.data_type FROM INFORMATION_SCHEMA.COLUMNS INNER JOIN INFORMATION_SCHEMA.TABLES ON INFORMATION_SCHEMA.COLUMNS.table_name = INFORMATION_SCHEMA.TABLES.table_name WHERE INFORMATION_SCHEMA.COLUMNS.table_schema = \'public\' and table_type = \'BASE TABLE\';')
        tabcols = {}
        for tup in schema_details:
            tab = tup[0]
            col = tup[1]
            if tab not in tabcols:
                tabcols[tab] = [col]
            else:
                tabcols[tab] += [col]
        
        return tabcols
        

def views_are_equal(view_def1, view_def2, db_details):
    #let's not go through the DB for the obvious...
    if 'CREATE VIEW' in view_def1 and 'CREATE VIEW' not in view_def2:
        return False
    elif 'CREATE VIEW' in view_def2 and 'CREATE VIEW' not in view_def1:
        return False
    elif 'CREATE VIEW' not in view_def1 and 'CREATE VIEW' not in view_def2:
        #mostly, this is a base table, so we can just check for string equality
        return (view_def1 == view_def2)
    elif view_def1 == view_def2:
        return True
    else:
        vname1 = view_def1.split(' ')[2]
        vname2 = view_def2.split(' ')[2]
        pgapi = PostgresAPI(db_details)
        
        pgapi.teardown()
        #first, create the views
        err_st1 = pgapi.execute(view_def1)
        if err_st1 != '':
            raise Exception("View_def1 failed: {}, {}".format(view_def1, err_st1))
        view_q1 = 'select * from ' + vname1 + ';'
        v1_recs = pgapi.query(view_q1)
        pgapi.teardown()
        
        
        err_st2 = pgapi.execute(view_def2)
        if err_st2 != '':
            raise Exception("View_def2 failed: {}, {}".format(view_def2, err_st2))
        view_q2 = 'select * from ' + vname2 + ';'
        v2_recs = pgapi.query(view_q2)
        pgapi.teardown()
        #now, get the records and check if they match        
        if type(v1_recs) == str:
            raise Exception("Query did not execute correctly: {}, {}".format(view_q1, v1_recs))
        if type(v2_recs) == str:
            raise Exception("Query did not execute correctly: {}, {}".format(view_q2, v2_recs))
        
        #if the sets of sets of attribute values are the same, we can assume
        #these are the same view.
        v1_recset = [frozenset(tup) for tup in v1_recs]
        v2_recset = [frozenset(tup) for tup in v2_recs]
        v1_set = set(v1_recset)
        v2_set = set(v2_recset)
        if v1_set == v2_set:
            return True
        else:
            return False

def unfuse_word(word, query_st):
    if word not in query_st:
        raise Exception("Very malformed query missing {}: {}".format(word, query_st))
    cur_pts = query_st.split(' ')
    new_pts = []
    if word not in cur_pts:
        for pt in cur_pts:
            if word in pt:
                sel_ind = pt.index(word)
                new_pt = pt[:sel_ind] + ' ' + word + ' ' + pt[sel_ind + len(word):]
                new_pts.append(new_pt)
            else:
                new_pts.append(pt)
    
    if new_pts == []:
        new_pts = cur_pts
    
    new_st = ' '.join(new_pts)
    return new_st

def fixed_sql(raw_st):
    if ' ' not in raw_st:
        #most likely this is a table/column/role name, so just return it.
        return raw_st
    new_st = raw_st
    #one mistake is that ChatGPT can repeat words.
    if raw_st.startswith('CREATE VIEW VIEW'):
        rest = raw_st[len('CREATE VIEW VIEW'):]
        new_st = 'CREATE VIEW' + rest
    
    #another is that words get fused
    new_st = unfuse_word('SELECT', new_st)
    new_st = unfuse_word('FROM', new_st)
    return new_st

#in postgres, capitalized column names have to be double-quoted
def fix_pgcaps(query_st : str, db_details : dict):
    pgapi = PostgresAPI(db_details)
    schema_query = 'SELECT table_schema, table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS where table_schema = \'public\';'
    schema_tups = pgapi.query(schema_query)
    col_names = [tup[2] for tup in schema_tups]
    print("Col_names: {}".format(col_names))
    
    q_pts = query_st.split(' ')
    new_pts = []
    #we need to examine equality among words likely to be column names
    #we do this to avoid column names that are substrings
    for pt in q_pts:
        if pt == '':
            continue
        clean_pt = pt.replace(' ', '')
        rel_pt = pt.replace(',', '')
        if rel_pt in col_names and rel_pt.isupper():
            new_pt = clean_pt[:clean_pt.index(rel_pt)] + '"' + rel_pt + '"' + clean_pt[clean_pt.index(rel_pt) + len(rel_pt):]
            new_pts.append(new_pt)
        else:
            new_pts.append(pt)
    
    new_st = ' '.join(new_pts)
    
    return new_st

#convert sqlite tables to postgres tables.
#this is needed if we want to run spider queries
def sqlite2postgres(sqlite_path : str, pg_details : dict):
    pgapi = PostgresAPI(pg_details)
    cnx = sqlite3.connect(sqlite_path)
    
    #first, get the list of tables
    cur = cnx.cursor()
    res = cur.execute("select name from sqlite_master where type='table';")
    tbls = [tup[0] for tup in res.fetchall()]
    for tbl in tbls:
        cur_df = pd.read_sql_query("SELECT * from " + tbl, cnx)
        pgapi.memdf_to_tbl(cur_df, tbl)
    
        
    
        
        
    
    
        

