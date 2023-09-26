import random
import math
import pandas as pd
from roles_test import get_schema
from ast import literal_eval
import sys
#generated by chatgpt
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


tpch_schema = {'orders': ['ORDERKEY', 'CUSTKEY', 'ORDERSTATUS', 'TOTALPRICE', 'ORDERDATE', 'ORDERPRIORITY', 'CLERK', 'SHIPPRIORITY', 'COMMENT'], 'partsupp': ['PARTKEY', 'SUPPKEY', 'AVAILQTY', 'SUPPLYCOST', 'COMMENT'], 'part': ['PARTKEY', 'NAME', 'MFGR', 'BRAND', 'TYPE', 'SIZE', 'CONTAINER', 'RETAILPRICE', 'COMMENT'], 'supplier': ['SUPPKEY', 'NAME', 'ADDRESS', 'NATIONKEY', 'PHONE', 'ACCTBAL', 'COMMENT'], 'nation': ['NATIONKEY', 'NAME', 'REGIONKEY', 'COMMENT'], 'customer': ['CUSTKEY', 'NAME', 'ADDRESS', 'NATIONKEY', 'PHONE', 'ACCTBAL', 'MKTSEGMENT', 'COMMENT'], 'lineitem': ['ORDERKEY', 'PARTKEY', 'SUPPKEY', 'LINENUMBER', 'QUANTITY', 'EXTENDEDPRICE', 'DISCOUNT', 'TAX', 'RETURNFLAG', 'LINESTATUS', 'SHIPDATE', 'COMMITDATE', 'RECEIPTDATE', 'SHIPINSTRUCT', 'SHIPMODE', 'COMMENT'], 'region': ['REGIONKEY', 'NAME', 'COMMENT']}


def rand_part(lst, level, n, chosen_seed):
    seed = chosen_seed * level + 1
    random.seed(seed)
    random.shuffle(lst)
    division = len(lst) / float(n) 
    return [ lst[int(round(division * i)): int(round(division * (i + 1)))] for i in range(n) ]

def privilege_to_st(priv_st):
    out_lst = []
    if 'C' in priv_st:
        out_lst.append('create')
    if 'R' in priv_st:
        out_lst.append('read')
    if 'U' in priv_st:
        out_lst.append('update')
    if 'D' in priv_st:
        out_lst.append('delete')
    
    out_st = ', '.join(out_lst)
    return out_st

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

def gen_hierarchy_docs(roles, roles_per_level, levels, template_st, table_lst, outname, chosen_seed=3):
    total_roles = 1 + roles_per_level * (levels - 1)
    highest = 'Administrator'
    rest = roles_per_level * (levels - 1)
    random.seed(chosen_seed)
    rest_roles = random.sample(roles, k=rest)
    gt_schema = ['Role', 'Table', 'Privilege']
    gtdct = {}
    for gtl in gt_schema:
        gtdct[gtl] = []
    gtdct['Role'] += [highest] * len(table_lst)
    gtdct['Table'] += table_lst
    gtdct['Privilege'] += ['CRUD'] * len(table_lst)
    role_levels = {}
    if levels > 0:
        role_levels = {role : math.floor(i / roles_per_level) for i,role in enumerate(rest_roles)}
    for level in set(role_levels.values()):
        role_lst = [role for role in role_levels if role_levels[role] == level]
        if level == 0:
            #full access
            for t in table_lst:
                for role in role_lst:
                    gtdct['Role'].append(role)
                    gtdct['Table'].append(t)
                    gtdct['Privilege'].append('RU')
        else:
            partition = rand_part(table_lst, level, roles_per_level, chosen_seed)
            for j,part in enumerate(partition):
                part_len = len(part)
                gtdct['Role'] += [role_lst[j]] * part_len
                gtdct['Table'] += part
                gtdct['Privilege'] += ['R'] * part_len
    
    gtdf = pd.DataFrame(gtdct)
    gtdf.to_csv(outname + '_hier_gt.csv', index=False)
    
    #now, translate the ground truth into access control sentences.
    #TODO: the below will have to become more modular to account for different
    #document templates.
    full_st = template_st
    full_st += 'The ' + highest + ' will have access to all data.\nAll users’ access privileges of database is based on the Application Access Matrix (according to duties segregation), '
    full_st += 'owned by the ' + highest + ' and endorsed by the senior management.\nThe default authorization will be specified as follows: '
    for row in gtdf.groupby(['Role', 'Privilege']):
        cur_role = row[0][0]
        cur_priv = row[0][1]
        subdf = row[1]
        cur_tbls = subdf['Table'].unique().tolist()
        cur_tbl_st  = ', '.join(cur_tbls)
        
        sentence = cur_role + 's should have ' + privilege_to_st(cur_priv) + ' access to ' + cur_tbl_st + ' information.'
        full_st += sentence
    
    with open(outname + '_hier_doc.txt', 'w+') as fh:
        print(full_st, file=fh)

def gen_complexhier_docs(roles, roles_per_level, levels, template_st, table_lst, outname, chosen_seed=3):
    total_roles = 1 + roles_per_level * (levels - 1)
    highest = 'Administrator'
    rest = roles_per_level * (levels - 1)
    random.seed(chosen_seed)
    rest_roles = random.sample(roles, k=rest)
    gt_schema = ['Role', 'Table', 'Privilege']
    gtdct = {}
    for gtl in gt_schema:
        gtdct[gtl] = []
    gtdct['Role'] += [highest] * len(table_lst)
    gtdct['Table'] += table_lst
    gtdct['Privilege'] += ['CRUD'] * len(table_lst)
    role_levels = {}
    if levels > 0:
        role_levels = {role : math.floor(i / roles_per_level) for i,role in enumerate(rest_roles)}
    for level in set(role_levels.values()):
        role_lst = [role for role in role_levels if role_levels[role] == level]
        if level == 0:
            #full access
            for t in table_lst:
                for role in role_lst:
                    gtdct['Role'].append(role)
                    gtdct['Table'].append(t)
                    gtdct['Privilege'].append('RU')
        else:
            partition = rand_part(table_lst, level, roles_per_level, chosen_seed)
            for j,part in enumerate(partition):
                part_len = len(part)
                gtdct['Role'] += [role_lst[j]] * part_len
                gtdct['Table'] += part
                gtdct['Privilege'] += ['R'] * part_len
    
    gtdf = pd.DataFrame(gtdct)
    gtdf.to_csv(outname + '_complex_gt.csv', index=False)
    
    #now, translate the ground truth into access control sentences.
    #TODO: the below will have to become more modular to account for different
    #document templates.
    full_st = template_st
    full_st += 'The ' + highest + ' will have full access to all data.'
    for row in gtdf.groupby(['Role', 'Privilege']):
        cur_role = row[0][0]
        cur_priv = row[0][1]
        subdf = row[1]
        cur_tbls = subdf['Table'].unique().tolist()
        cur_tbl_st  = ', '.join(cur_tbls)
        
        sentence = 'The role ' + cur_role + ' should have ' + privilege_to_policyblock_fmt(cur_priv) + ' access to tables ' + cur_tbl_st + '.'
        full_st += sentence
    
    with open(outname + '_complex_doc.txt', 'w+') as fh:
        print(full_st, file=fh)

def gen_complexviews_docs(roles, roles_per_level, levels, subroles, num_sub, template_st, table_lst, outname, schema=tpch_schema, chosen_seed=3):
    total_roles = 1 + roles_per_level * (levels - 1)
    highest = 'Administrator'
    rest = roles_per_level * (levels - 1)
    random.seed(chosen_seed)
    rest_roles = random.sample(roles, k=rest)
    gt_schema = ['Role', 'Table', 'Privilege', 'isView', 'Source Table']
    gtdct = {}
    for gtl in gt_schema:
        gtdct[gtl] = []
    gtdct['Role'] += [highest] * len(table_lst)
    gtdct['Table'] += table_lst
    gtdct['Privilege'] += ['CRUD'] * len(table_lst)
    gtdct['isView'] += [False] * len(table_lst)
    gtdct['Source Table'] += table_lst
    role_levels = {}
    if levels > 0:
        role_levels = {role : math.floor(i / roles_per_level) for i,role in enumerate(rest_roles)}
    for level in set(role_levels.values()):
        role_lst = [role for role in role_levels if role_levels[role] == level]
        if level == 0:
            #full access
            for t in table_lst:
                for role in role_lst:
                    gtdct['Role'].append(role)
                    gtdct['Table'].append(t)
                    gtdct['Privilege'].append('RU')
                    gtdct['isView'].append(False)
                    gtdct['Source Table'].append(t)
        else:
            partition = rand_part(table_lst, level, roles_per_level, chosen_seed)
            for j,part in enumerate(partition):
                part_len = len(part)
                gtdct['Role'] += [role_lst[j]] * part_len
                gtdct['Table'] += part
                gtdct['Privilege'] += ['R'] * part_len
                gtdct['isView'] += [False] * part_len
                gtdct['Source Table'] += part
    
    interns = random.sample(subroles, k=num_sub)
    view_tbls = random.choices(table_lst, k=len(interns))
    view_dct = {}
    for i,v in enumerate(view_tbls):
        colblock_lst = random.sample(schema[v], k=max(int(len(schema[v]) / 2), 1))
        view_dct[v] = colblock_lst
    
    for i,intern in enumerate(interns):
        vname = 'internview' + str(i)
        gtdct['Role'].append(intern)
        gtdct['Table'].append(vname)
        gtdct['Source Table'].append(view_tbls[i])
        gtdct['Privilege'].append('R')
        gtdct['isView'].append(True)
    
    gtdf = pd.DataFrame(gtdct)
    gtdf.to_csv(outname + '_complexviews_gt.csv', index=False)
    
    #now, translate the ground truth into access control sentences.
    #TODO: the below will have to become more modular to account for different
    #document templates.
    full_st = template_st
    full_st += 'The ' + highest + ' will have full access to all data.'
    core_tbldf = gtdf[gtdf['Table'] == gtdf['Source Table']]
    for row in core_tbldf.groupby(['Role', 'Privilege']):
        cur_role = row[0][0]
        cur_priv = row[0][1]
        subdf = row[1]
        cur_tbls = subdf['Table'].unique().tolist()
        cur_tbl_st  = ', '.join(cur_tbls)
        
        sentence = 'The role ' + cur_role + ' should have ' + privilege_to_policyblock_fmt(cur_priv) + ' access to tables ' + cur_tbl_st + '.'
        full_st += sentence
    
    view_tbldf = gtdf[gtdf['Table'] != gtdf['Source Table']]
    for row in view_tbldf.to_dict(orient='records'):
        cur_intern = row['Role']
        cur_src = row['Source Table']
        cur_view = row['Table']
        cur_priv = row['Privilege']
        
        view_create = 'Create a view of the ' + cur_src + ' table called ' + cur_view
        view_create += ' with all columns except ' + ', '.join(view_dct[cur_src]) + '. '
        
        role_sentence = 'The role ' + cur_intern + ' should only have ' + privilege_to_policyblock_fmt(cur_priv) + ' access on the view ' + cur_view + '. '
        full_st += view_create
        full_st += role_sentence
    
    
    with open(outname + '_complexviews_doc.txt', 'w+') as fh:
        print(full_st, file=fh)

def gen_dac_docs(roles, roles_per_level, levels, users, num_userpairs, template_st, table_lst, outname, chosen_seed=3):
    total_roles = 1 + roles_per_level * (levels - 1)
    highest = 'Administrator'
    rest = roles_per_level * (levels - 1)
    random.seed(chosen_seed)
    rest_roles = random.sample(roles, k=rest)
    chosen_users = random.sample(users, k=2 * num_userpairs)
    gt_schema = ['Role', 'Table', 'Privilege', 'isUser', 'Grantor']
    gtdct = {}
    for gtl in gt_schema:
        gtdct[gtl] = []
    gtdct['Role'] += [highest] * len(table_lst)
    gtdct['Table'] += table_lst
    gtdct['Privilege'] += ['CRUD'] * len(table_lst)
    gtdct['isUser'] = [False]*len(table_lst)
    gtdct['Grantor'] = ['postgres'] * len(table_lst)
    role_levels = {}
    if levels > 0:
        role_levels = {role : math.floor(i / roles_per_level) for i,role in enumerate(rest_roles)}
        
    for level in set(role_levels.values()):
        role_lst = [role for role in role_levels if role_levels[role] == level]
        if level == 0:
            #full access
            for t in table_lst:
                for role in role_lst:
                    gtdct['Role'].append(role)
                    gtdct['Table'].append(t)
                    gtdct['Privilege'].append('RU')
                    gtdct['isUser'].append(False)
                    gtdct['Grantor'].append('postgres')
        else:
            partition = rand_part(table_lst, level, roles_per_level, chosen_seed)
            for j,part in enumerate(partition):
                part_len = len(part)
                gtdct['Role'] += [role_lst[j]] * part_len
                gtdct['Table'] += part
                gtdct['Privilege'] += ['R'] * part_len
                gtdct['isUser'] += [False] * part_len
                gtdct['Grantor'] += ['postgres'] * part_len
    
    cur_lstlen = len(gtdct['Role'])
    gtdct['User Role'] = [None] * cur_lstlen
    grantors = [u for i,u in enumerate(chosen_users) if i % 2 == 0 ]
    grantees = [u for i,u in enumerate(chosen_users) if i % 2 == 1 ]
    for i,grantor in enumerate(grantors):
        usr_role = random.choice(rest_roles)
        lst_pos = gtdct['Role'].index(usr_role)
        gtdct['Role'].append(grantor)
        gtdct['Table'].append(gtdct['Table'][lst_pos])
        gtdct['Privilege'].append(gtdct['Privilege'][lst_pos])
        gtdct['isUser'].append(True)
        gtdct['Grantor'].append('postgres')
        gtdct['User Role'].append(usr_role)
        
        #now, fill in the grantee
        grantee = grantees[i]
        gtdct['Role'].append(grantee)
        gtdct['Table'].append(gtdct['Table'][lst_pos])
        gtdct['Privilege'].append(gtdct['Privilege'][lst_pos])
        gtdct['isUser'].append(True)
        gtdct['Grantor'].append(grantor)
        gtdct['User Role'].append(usr_role)
    
    
    gtdf = pd.DataFrame(gtdct)
    gtdf.to_csv(outname + '_dac_gt.csv', index=False)
    
    #now, translate the ground truth into access control sentences.
    #TODO: the below will have to become more modular to account for different
    #document templates.
    full_st = template_st
    full_st += 'Revoke all access to any table in the database for all roles. '
    full_st += 'The ' + highest + ' will have full access to all data. '
    roledf = gtdf[gtdf['isUser'] == False]
    for row in roledf.groupby(['Role', 'Privilege']):
        cur_role = row[0][0]
        cur_priv = row[0][1]
        subdf = row[1]
        cur_tbls = subdf['Table'].unique().tolist()
        cur_tbl_st  = ', '.join(cur_tbls)
        
        sentence = 'Grant role ' + cur_role + ' ' + privilege_to_policyblock_fmt(cur_priv) + ' access to tables ' + cur_tbl_st + ' with the option of passing on this privilege. '
        full_st += sentence
    
    userdf = gtdf[gtdf['isUser'] == True]
    for row in userdf.to_dict(orient='records'):
        cur_role = row['User Role']
        cur_name = row['Role']
        cur_priv = row['Privilege']
        cur_tbl = row['Table']
        is_grantor = (row['Grantor'] == 'postgres')
        if is_grantor:
            sentence = 'Create user ' + cur_name + ' in the role ' + cur_role + '. '
        else:
            sentence = 'Create user ' + cur_name + '. '
        full_st += sentence
        
        if not is_grantor:
            sentence = 'Grant user ' + cur_name + ' ' + privilege_to_policyblock_fmt(cur_priv) + ' access on table ' + cur_tbl + '. '
            full_st += sentence
    
    with open(outname + '_dac_doc.txt', 'w+') as fh:
        print(full_st, file=fh)

def gen_readonly_docs(roles, template_st, table_lst, outname, roles_per_level, levels=1, chosen_seed=3):
    # highest = 'Administrator'
    rest = roles_per_level
    random.seed(chosen_seed)
    rest_roles = random.sample(roles, k=rest)
    gt_schema = ['Role', 'Table', 'Privilege']
    gtdct = {}
    for gtl in gt_schema:
        gtdct[gtl] = []
    role_levels = {}
    if levels > 0:
        role_levels = {role : math.floor(i / roles_per_level) for i,role in enumerate(rest_roles)}
    for level in set(role_levels.values()):
        role_lst = [role for role in role_levels if role_levels[role] == level]
        #everyone only gets SELECT access, but on various tables.
        #we select the tables randomly.
        partition = rand_part(table_lst, level, roles_per_level, chosen_seed)
        for j,part in enumerate(partition):
            part_len = len(part)
            gtdct['Role'] += [role_lst[j]] * part_len
            gtdct['Table'] += part
            gtdct['Privilege'] += ['R'] * part_len
    
    gtdf = pd.DataFrame(gtdct)
    gtdf.to_csv(outname + '_readonly_gt.csv', index=False)
    
    #now, translate the ground truth into access control sentences.
    #TODO: the below will have to become more modular to account for different
    #document templates.
    full_st = template_st
    # full_st += 'The ' + highest + ' will have access to all data.\nAll users’ access privileges of database is based on the Application Access Matrix (according to duties segregation), '
    # full_st += 'owned by the ' + highest + ' and endorsed by the senior management.\nThe default authorization will be specified as follows: '
    for row in gtdf.groupby(['Role', 'Privilege']):
        cur_role = row[0][0]
        cur_priv = row[0][1]
        subdf = row[1]
        cur_tbls = subdf['Table'].unique().tolist()
        cur_tbl_st  = ', '.join(cur_tbls)
        
        sentence = 'The role ' + cur_role + ' should have ' + privilege_to_policyblock_fmt(cur_priv) + ' access to tables ' + cur_tbl_st + '.'
        full_st += sentence
    
    with open(outname + '_readonly_doc.txt', 'w+') as fh:
        print(full_st, file=fh)

def gen_readwrite_docs(roles, template_st, table_lst, outname, roles_per_level, levels=1, chosen_seed=3):
    # highest = 'Administrator'
    rest = roles_per_level
    random.seed(chosen_seed)
    rest_roles = random.sample(roles, k=rest)
    gt_schema = ['Role', 'Table', 'Privilege']
    gtdct = {}
    for gtl in gt_schema:
        gtdct[gtl] = []
    role_levels = {}
    if levels > 0:
        role_levels = {role : math.floor(i / roles_per_level) for i,role in enumerate(rest_roles)}
    for level in set(role_levels.values()):
        role_lst = [role for role in role_levels if role_levels[role] == level]
        #everyone only gets SELECT access, but on various tables.
        #we select the tables randomly.
        partition = rand_part(table_lst, level, roles_per_level, chosen_seed)
        for j,part in enumerate(partition):
            part_len = len(part)
            gtdct['Role'] += [role_lst[j]] * part_len
            gtdct['Table'] += part
            priv = random.choice(['R', 'RU'])
            gtdct['Privilege'] += [priv] * part_len
    
    gtdf = pd.DataFrame(gtdct)
    gtdf.to_csv(outname + '_readwrite_gt.csv', index=False)
    
    #now, translate the ground truth into access control sentences.
    #TODO: the below will have to become more modular to account for different
    #document templates.
    full_st = template_st
    # full_st += 'The ' + highest + ' will have access to all data.\nAll users’ access privileges of database is based on the Application Access Matrix (according to duties segregation), '
    # full_st += 'owned by the ' + highest + ' and endorsed by the senior management.\nThe default authorization will be specified as follows: '
    for row in gtdf.groupby(['Role', 'Privilege']):
        cur_role = row[0][0]
        cur_priv = row[0][1]
        subdf = row[1]
        cur_tbls = subdf['Table'].unique().tolist()
        cur_tbl_st  = ', '.join(cur_tbls)
        
        sentence = 'The role ' + cur_role + ' should have ' + privilege_to_policyblock_fmt(cur_priv) + ' access to tables ' + cur_tbl_st + '.'
        full_st += sentence
    
    with open(outname + '_readwrite_doc.txt', 'w+') as fh:
        print(full_st, file=fh)

def gen_allhierarchydocs(roles, roles_per_level, levels, template_st, table_lst, outname, num_docs):
    for i in range(1, num_docs + 1):
        chosen_seed = 3 * i
        gen_hierarchy_docs(roles, roles_per_level, levels, template_st, table_lst, outname + str(i), chosen_seed=chosen_seed)

#there is one administrator (the person executing these queries),
#and they can only grant reads to other roles.        
def gen_allreadonly(roles, num_roles, template_st, table_lst, outname, num_docs):
    for i in range(1, num_docs + 1):
        chosen_seed = 3 * i
        gen_readonly_docs(roles, template_st, table_lst, outname + str(i), num_roles, chosen_seed=chosen_seed)

#there is one administrator (the person executing these queries),
#and they can only grant reads or writes to other roles.        
def gen_allreadwrite(roles, num_roles, template_st, table_lst, outname, num_docs):
    for i in range(1, num_docs + 1):
        chosen_seed = 3 * i
        gen_readwrite_docs(roles, template_st, table_lst, outname + str(i), num_roles, chosen_seed=chosen_seed)

#there is a complex hierarchy of roles, each with their own table accesses and privilege sets.
def gen_allcomplexdocs(roles, roles_per_level, levels, template_st, table_lst, outname, num_docs):
    for i in range(1, num_docs + 1):
        chosen_seed = 3 * i
        gen_complexhier_docs(roles, roles_per_level, levels, template_st, table_lst, outname + str(i), chosen_seed=chosen_seed)

def gen_alldacdocs(roles, roles_per_level, levels, users, num_userpairs, template_st, table_lst, outname, num_docs):
    for i in range(1, num_docs + 1):
        chosen_seed = 3 * i
        gen_dac_docs(roles, roles_per_level, levels, users, num_userpairs, template_st, table_lst, outname + str(i), chosen_seed=chosen_seed)

def gen_allcompviewdocs(roles, roles_per_level, levels, subroles, num_sub, template_st, table_lst, outname, num_docs):
    for i in range(1, num_docs + 1):
        chosen_seed = 3 * i
        gen_complexviews_docs(roles, roles_per_level, levels, subroles, num_sub, template_st, table_lst, outname + str(i), chosen_seed=chosen_seed)

if __name__=='__main__':
    schema_lst = literal_eval(get_schema({}))
    init_temp_st = 'SELECT privilege grants a user’s access on views and tables and should be limited to authorized personnel, based on their roles.'
    # gen_hierarchy_docs(administrators, 3, 3, init_temp_st, schema_lst, 'testgen1')
    # gen_allhierarchydocs(administrators, 3, 3, init_temp_st, schema_lst, 'testgen', 100)
    gen_allreadonly(administrators, 5, '', schema_lst, 'q1readonly', 100)
    gen_allreadwrite(administrators, 5, '', schema_lst, 'q2readwrite', 100)
    gen_allcomplexdocs(administrators, 3, 3, '', schema_lst, 'q3complex', 100)
    gen_alldacdocs(administrators, 3, 3, common_names, 5,'', schema_lst, 'q4dac', 100)
    gen_allcompviewdocs(administrators, 3, 3, interns, 5, '', schema_lst, 'q5complexview', 100)
    # gen_alldacviewdocs(administrators, 3, 3, common_names, 5, interns, 5, german_names, 5, '', schema_lst, 'q6dacview', 100)

        
        
        
    
    