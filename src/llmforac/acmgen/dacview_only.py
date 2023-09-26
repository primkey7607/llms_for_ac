import random
import sys
import math
import pandas as pd
from ast import literal_eval

tpch_schema = {'orders': ['ORDERKEY', 'CUSTKEY', 'ORDERSTATUS', 'TOTALPRICE', 'ORDERDATE', 'ORDERPRIORITY', 'CLERK', 'SHIPPRIORITY', 'COMMENT'], 'partsupp': ['PARTKEY', 'SUPPKEY', 'AVAILQTY', 'SUPPLYCOST', 'COMMENT'], 'part': ['PARTKEY', 'NAME', 'MFGR', 'BRAND', 'TYPE', 'SIZE', 'CONTAINER', 'RETAILPRICE', 'COMMENT'], 'supplier': ['SUPPKEY', 'NAME', 'ADDRESS', 'NATIONKEY', 'PHONE', 'ACCTBAL', 'COMMENT'], 'nation': ['NATIONKEY', 'NAME', 'REGIONKEY', 'COMMENT'], 'customer': ['CUSTKEY', 'NAME', 'ADDRESS', 'NATIONKEY', 'PHONE', 'ACCTBAL', 'MKTSEGMENT', 'COMMENT'], 'lineitem': ['ORDERKEY', 'PARTKEY', 'SUPPKEY', 'LINENUMBER', 'QUANTITY', 'EXTENDEDPRICE', 'DISCOUNT', 'TAX', 'RETURNFLAG', 'LINESTATUS', 'SHIPDATE', 'COMMITDATE', 'RECEIPTDATE', 'SHIPINSTRUCT', 'SHIPMODE', 'COMMENT'], 'region': ['REGIONKEY', 'NAME', 'COMMENT']}
administrators = list(set([
    "System Administrator",
    "Network Administrator",
    "Database Administrator",
    "Linux Administrator",
    "Windows Administrator",
    "Cloud Administrator",
    "Salesforce Administrator",
    "Exchange Administrator",
    "SharePoint Administrator",
    "Active Directory Administrator",
    "Security Administrator",
    "Database Security Administrator",
    "Application Administrator",
    "Web Administrator",
    "Storage Administrator",
    "Virtualization Administrator",
    "SAP Administrator",
    "HR Administrator",
    "Financial Administrator",
    "School Administrator",
    "Healthcare Administrator",
    "Hospital Administrator",
    "Office Administrator",
    "Project Administrator",
    "CRM Administrator",
    "Database Developer/Administrator",
    "Education Administrator",
    "Event Administrator",
    "System Integration Administrator"
]))

common_names = [
    "John",
    "Mary",
    "Michael",
    "Jennifer",
    "James",
    "Sarah",
    "David",
    "Jessica",
    "Robert",
    "Elizabeth",
    "William",
    "Emily",
    "Joseph",
    "Ashley",
    "Christopher",
    "Amanda",
    "Daniel",
    "Megan",
    "Matthew",
    "Rachel"
]

german_names = [
    "Friedrich", "Gisela", "Dieter", "Helga", "Klaus",
    "Ingrid", "Jürgen", "Ursula", "Rainer", "Brigitte",
    "Gunter", "Renate", "Horst", "Hannelore", "Manfred",
    "Gertrud", "Heinrich", "Lieselotte", "Wolfgang", "Elfriede",
    "Gerhard", "Traudl", "Eberhard", "Mechthild", "Siegfried",
    "Brunhilde", "Günther", "Irmgard", "Hartmut", "Adelheid"
]


interns = list(set([
    "Marketing Intern",
    "Software Development Intern",
    "Graphic Design Intern",
    "Data Analyst Intern",
    "Human Resources Intern",
    "Social Media Intern",
    "Business Development Intern",
    "Content Writing Intern",
    "Research Intern",
    "Finance Intern",
    "Product Management Intern",
    "IT Support Intern",
    "Video Production Intern",
    "Sales Intern",
    "UXUI Design Intern",
    "Public Relations Intern",
    "Environmental Science Intern",
    "Supply Chain Intern",
    "Event Planning Intern",
    "Engineering Intern",
    "Healthcare Administration Intern",
    "Legal Intern",
    "Nonprofit Management Intern",
    "Architecture Intern",
    "Cybersecurity Intern",
    "Art Curator Intern",
    "Journalism Intern",
    "Culinary Arts Intern",
    "Fashion Merchandising Intern",
    "Sports Management Intern"
]))
interns = [st.replace(' ', '_') for st in interns]
administrators = [st.replace(' ', '_') for st in administrators]

def rand_part(lst, level, n, chosen_seed):
    seed = chosen_seed * level + 1
    random.seed(seed)
    random.shuffle(lst)
    division = len(lst) / float(n) 
    return [ lst[int(round(division * i)): int(round(division * (i + 1)))] for i in range(n) ]

def privilege_to_policyblock_fmt(priv_st):
    out_lst = []
    if 'C' in priv_st:
        out_lst.append('CREATE')
    if 'R' in priv_st:
        out_lst.append('SELECT')
    if 'U' in priv_st:
        out_lst.append('UPDATE and INSERT')
    if 'D' in priv_st:
        out_lst.append('DELETE')
    
    out_st = ', '.join(out_lst)
    return out_st

def gen_dacviews_docs(roles, roles_per_level, levels, users, num_userpairs, subroles, num_sub, intern_users, num_internpairs, template_st, table_lst, outname, schema=tpch_schema, chosen_seed=3):
    total_roles = 1 + roles_per_level * (levels - 1) #Variable is unused
    highest = 'Administrator'
    rest = roles_per_level * (levels - 1)
    random.seed(chosen_seed)
    rest_roles = random.sample(roles, k=rest)
    chosen_users = random.sample(users, k=2 * num_userpairs)
    gt_schema = ['Role', 'Table', 'Privilege', 'isUser', 'Grantor', 'User Role', 'isView', 'Source Table']
    gtdct = {}
    print('gen_dacviews_docs gt_schema for loop')
    for gtl in gt_schema:
        #Cannot be infinite for loop, no modification on list and finite list
        gtdct[gtl] = []
    gtdct['Role'] += [highest] * len(table_lst)
    gtdct['Table'] += table_lst
    gtdct['Privilege'] += ['CRUD'] * len(table_lst)
    gtdct['isUser'] += [False] * len(table_lst)
    gtdct['Grantor'] += ['postgres'] * len(table_lst)
    gtdct['isView'] += [False] * len(table_lst)
    gtdct['Source Table'] += table_lst
    print("Initial Size of gt_dct: {}".format(sys.getsizeof(gtdct)))
    role_levels = {}
    #From what I can tell role is a string
    if levels > 0:
        role_levels = {role : math.floor(i / roles_per_level) for i,role in enumerate(rest_roles)}
        print(role_levels)
    print("Size of role_levels: {}".format(sys.getsizeof(role_levels)))
    print('gen_dacviews_docs STARTING ROLE LEVEL looping')
    for level in set(role_levels.values()):
        #Check here, could be modifying dictionary while looping
        role_lst = [role for role in role_levels if role_levels[role] == level]
        if level == 0:
            #full access
            finished = len(table_lst) * len(role_lst)
            cnt = 0
            for t in table_lst:
                for role in role_lst:
                    gtdct['Role'].append(role)
                    gtdct['Table'].append(t)
                    gtdct['Privilege'].append('RU')
                    gtdct['isUser'].append(False)
                    gtdct['Grantor'].append('postgres')
                    gtdct['isView'].append(False)
                    gtdct['Source Table'].append(t)
                    cnt += 1
                    if cnt == finished - 1:
                        break
            
        else:
            partition = rand_part(table_lst, level, roles_per_level, chosen_seed)
            for j,part in enumerate(partition):
                part_len = len(part)
                gtdct['Role'] += [role_lst[j]] * part_len
                gtdct['Table'] += part
                gtdct['Privilege'] += ['R'] * part_len
                gtdct['isUser'] += [False] * part_len
                gtdct['Grantor'] += ['postgres'] * part_len
                gtdct['isView'] += [False] * part_len
                gtdct['Source Table'] += part
        print("Size of gt_dct at level {}: {}".format(level, sys.getsizeof(gtdct)))
    
    cur_lstlen = len(gtdct['Role'])
    gtdct['User Role'] = [None] * cur_lstlen
    grantors = [u for i,u in enumerate(chosen_users) if i % 2 == 0 ]
    grantees = [u for i,u in enumerate(chosen_users) if i % 2 == 1 ]
    print('gen_dacviews_docs grantors looping')
    for i,grantor in enumerate(grantors):
        usr_role = random.choice(rest_roles)
        lst_pos = gtdct['Role'].index(usr_role)
        gtdct['Role'].append(grantor)
        gtdct['Table'].append(gtdct['Table'][lst_pos])
        gtdct['Privilege'].append(gtdct['Privilege'][lst_pos])
        gtdct['isUser'].append(True)
        gtdct['Grantor'].append('postgres')
        gtdct['User Role'].append(usr_role)
        gtdct['isView'].append(False)
        gtdct['Source Table'].append(gtdct['Table'][lst_pos])
        
        #now, fill in the grantee
        grantee = grantees[i]
        gtdct['Role'].append(grantee)
        gtdct['Table'].append(gtdct['Table'][lst_pos])
        gtdct['Privilege'].append(gtdct['Privilege'][lst_pos])
        gtdct['isUser'].append(True)
        gtdct['Grantor'].append(grantor)
        gtdct['User Role'].append(usr_role)
        gtdct['isView'].append(False)
        gtdct['Source Table'].append(gtdct['Table'][lst_pos])
    
    print("Size of gt_dct after table users: {}".format(sys.getsizeof(gtdct)))
    
    interns = random.sample(subroles, k=num_sub)
    view_tbls = random.choices(table_lst, k=len(interns))
    view_dct = {}
    print('gen_dacviews_docs view tables looping')
    print(schema)
    for i,v in enumerate(view_tbls):
        print(schema[v])
        colblock_lst = random.sample(schema[v], k=max(int(len(schema[v]) / 2), 1))
        view_dct[v] = colblock_lst
    print('gen_dacviews_docs interns looping')
    for i,intern in enumerate(interns):
        vname = 'internview' + str(i)
        gtdct['Role'].append(intern)
        gtdct['Table'].append(vname)
        gtdct['Source Table'].append(view_tbls[i])
        gtdct['Privilege'].append('R')
        gtdct['isView'].append(True)
        gtdct['User Role'].append(None)
        gtdct['isUser'].append(False)
        gtdct['Grantor'].append('postgres')
    
    print("Size of gt_dct after intern view granting: {}".format(sys.getsizeof(gtdct)))
    
    #now, define intern users who can grant , and intern grantees
    chosen_interns = random.sample(intern_users, k=2 * num_internpairs)
    intern_grantors = [u for i,u in enumerate(chosen_interns) if i % 2 == 0 ]
    intern_grantees = [u for i,u in enumerate(chosen_interns) if i % 2 == 1 ]
    print('gen_dacviews_docs intern_grantors looping')
    for i,intern_grantor in enumerate(intern_grantors):
        usr_role = random.choice(interns)
        lst_pos = gtdct['Role'].index(usr_role)
        gtdct['Role'].append(intern_grantor)
        gtdct['Table'].append(gtdct['Table'][lst_pos])
        gtdct['Privilege'].append(gtdct['Privilege'][lst_pos])
        gtdct['isUser'].append(True)
        gtdct['Grantor'].append('postgres')
        gtdct['User Role'].append(usr_role)
        gtdct['isView'].append(True)
        gtdct['Source Table'].append(gtdct['Source Table'][lst_pos])
        
        #now, fill in the grantee
        intern_grantee = intern_grantees[i]
        gtdct['Role'].append(intern_grantee)
        gtdct['Table'].append(gtdct['Table'][lst_pos])
        gtdct['Privilege'].append(gtdct['Privilege'][lst_pos])
        gtdct['isUser'].append(True)
        gtdct['Grantor'].append(intern_grantor)
        gtdct['User Role'].append(usr_role)
        gtdct['isView'].append(True)
        gtdct['Source Table'].append(gtdct['Source Table'][lst_pos])
    
    print("Size of gt_dct after intern user granting: {}".format(sys.getsizeof(gtdct)))
    
    gtdf = pd.DataFrame(gtdct)
    gtdf.to_csv(outname + '_dacviews_gt.csv', index=False)
    
    #now, translate the ground truth into access control sentences.
    #TODO: the below will have to become more modular to account for different
    #document templates.
    sent2sql = {}
    full_st = template_st
    full_st += 'Revoke all access to any table in the database for all roles. '
    sent2sql['Revoke all access to any table in the database for all roles.'] = 'revoke all on all tables in schema public from public;'
    full_st += 'The ' + highest + ' will have full access to all data. '
    viewdf = gtdf[(gtdf['isView'] == True) & (gtdf['isUser'] == False)]
    print('gen_dacviews_docs viewdf looping')
    for row in viewdf.to_dict(orient='records'):
        cur_role = row['Role']
        cur_priv = row['Privilege']
        cur_view = row['Table']
        cur_src = row['Source Table']
        
        view_create = 'Create a view of the ' + cur_src + ' table called ' + cur_view
        view_create += ' with all columns except ' + ', '.join(view_dct[cur_src]) + '. '
        
        vc_correctcols = [c for c in schema[cur_src] if c not in view_dct[cur_src]]
        vc_sql = 'CREATE VIEW ' + cur_view + ' AS SELECT ' + ', '.join(vc_correctcols)
        vc_sql += ' FROM ' + cur_src + ';'
        sent2sql[view_create] = vc_sql
        
        role_sentence = 'Grant role ' + cur_role + ' ' + privilege_to_policyblock_fmt(cur_priv) + ' access on the view ' + cur_view + ' with the option of passing on this privilege. '
        role_sql = 'GRANT ' + privilege_to_policyblock_fmt(cur_priv) + ' ON ' + cur_view + ' TO ' + cur_role + ' WITH GRANT OPTION;'
        sent2sql[role_sentence] = role_sql
        full_st += view_create
        full_st += role_sentence
        
        
    roledf = gtdf[(gtdf['isUser'] == False) & (gtdf['isView'] == False)]
    print('gen_dacviews_docs role looping')
    for row in roledf.groupby(['Role', 'Privilege']):
        cur_role = row[0][0]
        cur_priv = row[0][1]
        subdf = row[1]
        cur_tbls = subdf['Table'].unique().tolist()
        cur_tbl_st  = ', '.join(cur_tbls)
        
        sentence = 'Grant role ' + cur_role + ' ' + privilege_to_policyblock_fmt(cur_priv) + ' access to tables ' + cur_tbl_st + ' with the option of passing on this privilege. '
        sent_sql = 'GRANT ' + privilege_to_policyblock_fmt(cur_priv) + ' ON ' + cur_tbl_st + ' TO ' + cur_role + ' WITH GRANT OPTION;'
        sent2sql[sentence] = sent_sql
        full_st += sentence
    
    userdf = gtdf[gtdf['isUser'] == True]
    print('gen_dacviews_docs userdf looping')
    for row in userdf.to_dict(orient='records'):
        cur_role = row['User Role']
        cur_name = row['Role']
        cur_priv = row['Privilege']
        cur_tbl = row['Table']
        is_grantor = (row['Grantor'] == 'postgres')
        if is_grantor:
            sentence = 'Create user ' + cur_name + ' in the role ' + cur_role + '. '
            sent_sql = 'CREATE USER ' + cur_name + ' IN ROLE ' + cur_role + ';'
            sent2sql[sentence] = sent_sql
        else:
            sentence = 'Create user ' + cur_name + '. '
            sent_sql = 'CREATE USER ' + cur_name + ';'
            sent2sql[sentence] = sent_sql
        full_st += sentence
        
        if not is_grantor:
            sentence = 'Grant user ' + cur_name + ' ' + privilege_to_policyblock_fmt(cur_priv) + ' access on table ' + cur_tbl + '. '
            sent_sql = 'GRANT ' + privilege_to_policyblock_fmt(cur_priv) + ' ON ' + cur_tbl + ' TO ' + cur_name + ';'
            sent2sql[sentence] = sent_sql
            full_st += sentence
    
    
    with open(outname + '_sent2sql.json', 'w+') as fh:
        print(sent2sql, file=fh)
    
    with open(outname + '_dacviews_doc.txt', 'w+') as fh:
        print(full_st, file=fh)
    
    

def gen_alldacviewdocs(roles, 
                       roles_per_level, 
                       levels, 
                       users, 
                       num_userpairs, 
                       subroles, 
                       num_sub, 
                       intern_users, 
                       num_internpairs, 
                       template_st, 
                       table_lst, 
                       outname, 
                       num_docs):
    print('STARTING gen_all_dacviewdocs for loop')
    for i in range(1, num_docs + 1):
        #Cannot be the infinite for loop
        chosen_seed = 3 * i
        print(f'GOING TO gen_dacviews docs loop {i}')
        gen_dacviews_docs(roles, roles_per_level, levels, users, num_userpairs, subroles, num_sub, intern_users, num_internpairs, template_st, table_lst, outname + str(i), chosen_seed=chosen_seed)


def get_schema(db_details):
    #for now, let's just hardcode it
    return str(['supplier', 'customer', 'lineitem','region',
            'orders', 'partsupp', 'part', 'nation'])

if __name__=='__main__':
    schema_lst = literal_eval(get_schema({}))
    print('STARTING gen_alldacviewdocs call')
    # gen_alldacviewdocs(administrators, 3, 3, common_names, 5, interns, 5, german_names, 5, '', schema_lst, 'q6dacview', 100)
    gen_alldacviewdocs(administrators, 3, 3, common_names, 5, interns, 5, german_names, 5, '', schema_lst, 'dacview_wjson', 100)
