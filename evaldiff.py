import pandas as pd
import os
from ast import literal_eval

'''
Purpose: evaluate ACM differencing
'''

#we specifically need to group the views that mean the same thing.
#spider has many of these, where the NL is different and the query is the same.
def construct_spidergt(acm_query, acm_base, gt_outname):
    if os.path.exists(gt_outname):
        with open(gt_outname, 'r') as fh:
            dct = literal_eval(fh.read())
        return dct
    
    qdf = pd.read_csv(acm_query)
    bdf = pd.read_csv(acm_base)
    
    query_dct = {}
    for i,c in enumerate(qdf.columns):
        if c == 'Role':
            continue
        
        if c.startswith('CREATE VIEW'):
            query_pt = c.split(' ')[4:]
            query_st = ' '.join(query_pt)
            if query_st not in query_dct:
                query_dct[query_st] = [(i, c)]
            else:
                query_dct[query_st].append((i, c))
    
    gt_dct = {}
    #the column indexes in the NL should match those of the SQL dataframe.
    for k in query_dct:
        same_inds = [t[0] for t in query_dct[k]]
        gt_dct[k] = []
        for ind in same_inds:
            gt_dct[k].append((ind, bdf.columns.tolist()[ind]))
    
    with open(gt_outname, 'w+') as fh:
        print(gt_dct, file=fh)
    
    return gt_dct

def first(tuplst):
    return [tup[0] for tup in tuplst]

def second(tuplst):
    return [tup[1] for tup in tuplst]

def views_gt_eq(view1 : str, view2 : str, acmdf1, acmdf2, gt_dct):
    
    ind_dct = {k : first(gt_dct[k]) for k in gt_dct}
    ind1 = acmdf1.columns.tolist().index(view1)
    rb1 = [k for k in ind_dct if ind1 in ind_dct[k]]
    ind2 = acmdf2.columns.tolist().index(view2)
    rb2 = [k for k in ind_dct if ind2 in ind_dct[k]]
    
    rb1set = set(rb1)
    rb2set = set(rb2)
    return (rb1set == rb2set)

def roles_gt_eq(role1 : str, role2: str, acmdf1, acmdf2):
    
    #there should be no tricks here. roles are only equivalent if they appear in the same index in both dataframes.
    roles1 = acmdf1['Role'].tolist()
    roles2 = acmdf2['Role'].tolist()
    
    r1ind = roles1.index(role1)
    r2ind = roles2.index(role2)
    
    
    
    return (r1ind == r2ind) or (role1 == role2)

def viewpair_gt_eq(raw_view1 : str, raw_view2 : str, ind, acmdf1, acmdf2, query_df):
    view1 = raw_view1.replace('\n', '')
    view2 = raw_view2.replace('\n', '')
    acm1_cols = [c.replace('\n', ' ') for c in acmdf1.columns]
    acm2_cols = [c.replace('\n', ' ') for c in acmdf2.columns]
    
    qcols = query_df.columns.tolist()
    #now, get the equivalent query indexes
    qdct = {}
    for i,c in enumerate(qcols):
        query_only = c.split(' ')[4:] #skip the CREATE VIEW x AS
        qo_st = ' '.join(query_only)
        if qo_st in qdct:
            qdct[qo_st] += [i]
        else:
            qdct[qo_st] = [i]
    
    v1_ind = acm1_cols.index(view1)
    v2_ind = acm2_cols.index(view2)
    
    v1_bucket = [k for k in qdct if v1_ind in qdct[k]]
    v2_bucket = [k for k in qdct if v2_ind in qdct[k]]
    ind_bt = [k for k in qdct if ind in qdct[k]]
    
    v1set = set(v1_bucket)
    v2set = set(v2_bucket)
    indset = set(ind_bt)
    
    
    if v1set == v2set and v1set == indset:
        return True
    return False

def tup_in_privs(tup : tuple, view_ind, privdf, acmdf1, acmdf2, query_df):
    
    #check each rule in privdf to see if the role-view mapping exists.
    for row in privdf.to_dict(orient='records'):
        priv_tup = (row['ACM 1 Role'], row['ACM 1 View'], row['ACM 2 Role'], row['ACM 2 View'])
        roles_captured = False
        views_captured = False
        if row['ACM 1 Role'] == tup[0] and row['ACM 2 Role'] == tup[2]:
            roles_captured = True
        elif row['ACM 1 Role'] == tup[0] or row['ACM 1 Role'] == tup[2] and row['ACM 1 Role'] == row['ACM 2 Role']:
            roles_captured = True
        elif row['ACM 2 Role'] == tup[0] or row['ACM 2 Role'] == tup[2] and row['ACM 1 Role'] == row['ACM 2 Role']:
            roles_captured = True
        
        views_captured = viewpair_gt_eq(row['ACM 1 View'], row['ACM 2 View'], view_ind, acmdf1, acmdf2, query_df)
        
        if roles_captured and views_captured:
            return True
        # elif not roles_captured and not views_captured:
        #     print("Tup: {}, Ind: {}, Neither captured".format(tup, view_ind))
        # elif not roles_captured:
        #     print("Tup: {}, Ind: {}, Roles not captured".format(tup, view_ind))
        # elif not views_captured:
        #     print("Tup: {}, Ind: {}, Views not captured".format(tup, view_ind))
    
    return False
    
    
        

def perteq_mainstats(acmpath1, acmpath2, querypath, role_res, view_res, priv_res, outdir, outpref, gt_details={}):
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    outname = os.path.join(outdir, outpref)
    out_dct = {}
    #there are two kinds of confusion matrices we want to fill in--
    #one with respect to whether we got role-view mappings right
    #and one with respect to whether we got privileges right, assuming we got role-view mappings right.
    out_schema = ['RV_TP', 'RV_TN', 'RV_FP', 'RV_FN', 'RV_Precision', 'RV_Recall',
                  'Priv_TP', 'Priv_TN', 'Priv_FP', 'Priv_FN', 'Priv_Precision', 'Priv_Recall']
    for o in out_schema:
        out_dct[o] = []
    
    acmdf1 = pd.read_csv(acmpath1)
    acmdf2 = pd.read_csv(acmpath2)
    query_df = pd.read_csv(querypath)
    roles1 = acmdf1['Role'].tolist()
    roles2 = acmdf2['Role'].tolist()
    views1 = [c for c in acmdf1.columns if c != 'Role']
    views2 = [c for c in acmdf2.columns if c != 'Role']
    
    privdf = pd.read_csv(priv_res)
    
    #first, compute the stats needed for role-view correctness
    #true positive--the role-view mapping that is in privdf is also in the ground truth.
    #false positive--the role-view mapping in privdf is not in the ground truth.
    #true negative--the role-view mapping is not in privdf or the ground truth.
    #false negative--the role-view mapping is not in privdf, but is in the ground truth.
    rv_tps = []
    rv_fps = []
    rv_tns = []
    rv_fns = []
    
    priv_tps = []
    priv_fps = []
    priv_tns = []
    priv_fns = []
    
    #first compute true and false positives.
    # if 'mode' in gt_details and gt_details['mode'] == 'spider':
    #     gt_outname = gt_details['outname']
    #     with open(gt_outname, 'r') as fh:
    #         gt_dct = literal_eval(fh.read())
            
    for row in privdf.to_dict(orient='records'):
        v1 = row['ACM 1 View']
        v2 = row['ACM 2 View']
        r1 = row['ACM 1 Role']
        r2 = row['ACM 2 Role']
        # v_are_same = views_gt_eq(v1, v2, acmdf1, acmdf2, gt_dct)
        v_are_same = check_view_gt(acmdf1, acmdf2, query_df, v1, v2)
        r_are_same = check_role_gt(acmdf1, acmdf2, r1, r2)
        # r_are_same = roles_gt_eq(r1, r2, acmdf1, acmdf2)
        
        if v_are_same and r_are_same:
            fulltup = (r1, v1, r2, v2)
            rv_tps.append(fulltup)
        elif not r_are_same and not v_are_same:
            fulltup = (r1, v1, r2, v2, 'Neither')
            rv_fps.append(fulltup)
        elif not r_are_same:
            fulltup = (r1, v1, r2, v2, 'Role')
            rv_fps.append(fulltup)
        elif not v_are_same:
            fulltup = (r1, v1, r2, v2, 'View')
            rv_fps.append(fulltup)

    #next, look for false negatives.
    #the procedure is--for each index of view pairs from acm1 and acm2,
    #for each rule in privdf,
    #check if that rule in privdf is equivalent to the ground truth views,
    #given the ground truth.
    for i,r1 in enumerate(acmdf1['Role']):
        r2 = roles2[i]
        for j,v1 in enumerate(views1):
            v2 = views2[j]
            fulltup = (r1, v1, r2, v2)
            cur_view_ind = j
            in_privs = tup_in_privs(fulltup, cur_view_ind, privdf, acmdf1, acmdf2, query_df)
            if not in_privs:
                rv_fns.append(fulltup)
            #and if in_privs, do nothing. we would already have captured this as a true positive.
    
    with open(outname + '_rvtps.txt', 'w+') as fh:
        print(rv_tps, file=fh)
    
    with open(outname + '_rvfps.txt', 'w+') as fh:
        print(rv_fps, file=fh)
    
    with open(outname + '_rvfns.txt', 'w+') as fh:
        print(rv_fns, file=fh)
    
    if len(rv_tps) + len(rv_fps) > 0:
        rv_precision = len(rv_tps) / (len(rv_tps) + len(rv_fps))
    else:
        rv_precision = 0.0
    
    if len(rv_tps) + len(rv_fns) > 0:
        rv_recall = len(rv_tps) / (len(rv_tps) + len(rv_fns))
    else:
        rv_recall = 0.0
    
    out_dct = {}
    out_dct['RV_TP'] = [len(rv_tps)]
    out_dct['RV_FP'] = [len(rv_fps)]
    out_dct['RV_FN'] = [len(rv_fns)]
    out_dct['RV_Precision'] = [rv_precision]
    out_dct['RV_Recall'] = [rv_recall]
    
    
    #next, compute the stats needed for privilege correctness given we got the role-view mapping correct
    #true positive--the mapping is in the ground truth, and "Violation" is false
    #false positive-- (does not appear) the mapping is in the ground truth and "Violation" is false, but should be true.
    #true negative--(should not appear) "Violation" is true, and should be true.
    #false negative--"Violation" is true, but should be false.
    #So we only have true positives and true negatives in this case, meaning recall is the only meaningful metric here.
    #next, look for false negatives.
    #the procedure is--for each index of view pairs from acm1 and acm2,
    #for each rule in privdf,
    #check if that rule in privdf is equivalent to the ground truth views,
    #given the ground truth.
    for row in privdf.to_dict(orient='records'):
        v1 = row['ACM 1 View']
        v2 = row['ACM 2 View']
        r1 = row['ACM 1 Role']
        r2 = row['ACM 2 Role']
        # v_are_same = views_gt_eq(v1, v2, acmdf1, acmdf2, gt_dct)
        v_are_same = check_view_gt(acmdf1, acmdf2, query_df, v1, v2)
        r_are_same = check_role_gt(acmdf1, acmdf2, r1, r2)
        
        if v_are_same and r_are_same:
            #now, check for a violation:
                if not row['Violation']:
                    priv_tps.append((r1, v1, r2, v2))
                else:
                    priv_fns.append((r1, v1, r2, v2))
    
    if len(priv_tps) + len(priv_fps) > 0:
        priv_precision = len(priv_tps) / (len(priv_tps) + len(priv_fps))
    else:
        priv_precision = 0.0
    
    if len(priv_tps) + len(priv_fns) > 0:
        priv_recall = len(priv_tps) / (len(priv_tps) + len(priv_fns))
    else:
        priv_recall = 0.0
    
    with open(outname + '_privtps.txt', 'w+') as fh:
        print(priv_tps, file=fh)
    
    with open(outname + '_privfps.txt', 'w+') as fh:
        print(priv_fps, file=fh)
    
    with open(outname + '_privfns.txt', 'w+') as fh:
        print(priv_fns, file=fh)
    
    out_dct['Priv_TP'] = [len(priv_tps)]
    out_dct['Priv_FP'] = [len(priv_fps)]
    out_dct['Priv_FN'] = [len(priv_fns)]
    out_dct['Priv_Precision'] = [priv_precision]
    out_dct['Priv_Recall'] = [priv_recall]
    
    out_df = pd.DataFrame(out_dct)
    out_df.to_csv(outname + '_stats.csv', index=False)
    return out_df
                
    # else:
    #     raise Exception("Only experimenting on spider for now. Change details accordingly: {}".format(gt_details))
    

def compare_to_gt_fp(acmpath1, acmpath2, priv_res, gt_info={}):
    #gt_info assumption: we assume that the gt_info was built with respect to acmpath1.
    if gt_info == {}:
        #then, naively assemble the ith role and jth view of one with the ith role and jth view of the other.
        acmdf1 = pd.read_csv(acmpath1)
        acmdf2 = pd.read_csv(acmpath2)
        gt_tups = []
        for i,row in enumerate(acmdf1.to_dict(orient='records')):
            row2 = acmdf2.loc[i]
            for j,c in enumerate(acmdf1.columns):
                if c == 'Role':
                    continue
                fulltup = (row['Role'], c, row2['Role'], acmdf2.columns[j])
                gt_tups.append(fulltup)
        
        fps = []
        privdf = pd.read_csv(priv_res)
        #so now, we've captured role-view mappings that are missing, or have been given incorrect answers.
        #next, we will account for incorrect role-view mappings.
        for row in privdf.to_dict(orient='records'):
            cur_tup = (row['ACM 1 Role'], row['ACM 1 View'], row['ACM 2 Role'], row['ACM 2 View'])
            if cur_tup not in gt_tups:
                fps.append(cur_tup)
            #and if cur_tup is in gt_tups, then we don't need to do anything, because
            #we would have already checked its correctness in the previous loop.
        
        return fps
    
    elif gt_info['mode'] == 'spider':
        gtpath = gt_info['outname']
        with open(gtpath, 'r') as fh:
            dct = literal_eval(fh.read())
        
        fps = []
        privdf = pd.read_csv(priv_res)
        acmdf1 = pd.read_csv(acmpath1)
        views1 = [v for v in acmdf1.columns if v != 'Role']
        acmdf2 = pd.read_csv(acmpath2)
        views2 = [v for v in acmdf2.columns if v != 'Role']
        
        for row in privdf.to_dict(orient='records'):
            cur_tup = (row['ACM 1 Role'], row['ACM 1 View'], row['ACM 2 Role'], row['ACM 2 View'])
            #now, find the equivalent views
            ind_dct = {k : first(dct[k]) for k in dct}
            val_dct = {k : second(dct[k]) for k in dct}
            
            #now, we want to check that the acmdf1 index for ACM 1 View and the acmdf2 index for ACM 2 View appear in the same bucket.
            ind1 = views1.index(row['ACM 1 View'])
            right_buckets1 = [k for k in ind_dct if ind1 in ind_dct[k]]
            
            ind2 = views2.index(row['ACM 2 View'])
            right_buckets2 = [k for k in ind_dct if ind2 in ind_dct[k]]
            
            rbset1 = set(right_buckets1)
            rbset2 = set(right_buckets2)
            if rbset1 != rbset2:
                fps.append(cur_tup)
        
        return fps

def check_view_gt(acmdf1, acmdf2, query_df, raw_view1, raw_view2):
    view1 = raw_view1.replace('\n', '')
    view2 = raw_view2.replace('\n', '')
    acm1_cols = [c.replace('\n', ' ') for c in acmdf1.columns]
    acm2_cols = [c.replace('\n', ' ') for c in acmdf2.columns]
    qcols = query_df.columns.tolist()
    #now, get the equivalent query indexes
    qdct = {}
    for i,c in enumerate(qcols):
        query_only = c.split(' ')[4:] #skip the CREATE VIEW x AS
        qo_st = ' '.join(query_only)
        if qo_st in qdct:
            qdct[qo_st] += [i]
        else:
            qdct[qo_st] = [i]
    
    v1_ind = acm1_cols.index(view1)
    v2_ind = acm2_cols.index(view2)
    
    v1_bucket = [k for k in qdct if v1_ind in qdct[k]]
    v2_bucket = [k for k in qdct if v2_ind in qdct[k]]
    
    v1set = set(v1_bucket)
    v2set = set(v2_bucket)
    if v1set.isdisjoint(v2set):
        return False
    else:
        return True

def check_role_gt(acmdf1, acmdf2, role1, role2, role_gt=None):
    #TODO: turns out, this is not straightforward at all.
    #the order in which records appear is somehow different between dataframes.
    #So when we perturb roles, we will have to remember to store the mappings.
    #And when we compare role queries to role names, we'll have to parse or use equality.
    # r1_ind = acmdf1['Role'].tolist().index(role1)
    # r2_ind = acmdf2['Role'].tolist().index(role2)
    
    if role1 == role2:
        return True
    elif role_gt != None:
        with open(role_gt, 'r') as fh:
            role_dct = literal_eval(fh.read())
        
        r1_ind = acmdf1['Role'].tolist().index(role1)
        r2_ind = acmdf2['Role'].tolist().index(role2)
        if role_dct[r1_ind] == r2_ind:
            return True
    
    return False

def dct_in_df(dct : dict, dfpath : str):
    df = pd.read_csv(dfpath)
    #TODO: fill in the rest. this will be a very useful utility to have.
    
    

def perteq_naivestats(acmpath1, acmpath2, role_res, view_res, priv_res, outdir, outpref, gt_details={}):
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    
    outname = os.path.join(outdir, outpref)
    #TODO: we need to compute precision and recall
    #specifically, a true positive is when the role mapping we found is in the ground truth.
    #a true negative is when we didn't find a role mapping that is not in the ground truth.
    # a false positive is when we found a role mapping that is not in the ground truth.
    # a false negative is when we did not find a role mapping that is in the ground truth.
    # the right metrics here are precision and recall, where we compute according to the above.
    acmdf1 = pd.read_csv(acmpath1)
    acmdf2 = pd.read_csv(acmpath2)
    privdf = pd.read_csv(priv_res)
    
    tps = []
    fps = []
    tns = []
    fns = []
    
    gt_tups = []
    
    for i,row in enumerate(acmdf1.to_dict(orient='records')):
        row2 = acmdf2.loc[i]
        for j,c in enumerate(acmdf1.columns):
            if c == 'Role':
                continue
            fulltup = (row['Role'], c, row2['Role'], acmdf2.columns[j])
            gt_tups.append(fulltup)
            #now, check if we even found this mapping among the privileges we found
            is_mapped = (privdf[['ACM 1 Role', 'ACM 1 View', 'ACM 2 Role', 'ACM 2 View']].values == fulltup).all(axis=1).any()
            if not is_mapped:
                #this is a rule we missed
                fns.append(fulltup)
            else:
                privmatches = privdf.query('@fulltup[0] == `ACM 1 Role` and @fulltup[1] == `ACM 1 View` and @fulltup[2] == `ACM 2 Role` and @fulltup[3] == `ACM 2 View`')
                #TODO: here, we use the fact that we're testing equality, so we know the ground truth should always be false--there was no violation.
                #but it would be nice not to have to hard-code this later.
                #also, I don't see why we'd have more than one row in privmatches, but let's AND the results
                correct = True
                for row3 in privmatches.to_dict(orient='records'):
                    correct = correct and not row3['Violation']
                
                if correct:
                    tps.append(fulltup)
                else:
                    fns.append(fulltup)
    
    fps = compare_to_gt_fp(acmpath1, acmpath2, priv_res, gt_info=gt_details)
    
    with open(outname + '_tps.txt', 'w+') as fh:
        print(tps, file=fh)
    
    with open(outname + '_fps.txt', 'w+') as fh:
        print(fps, file=fh)
    
    with open(outname + '_fns.txt', 'w+') as fh:
        print(fns, file=fh)
    
    precision = len(tps) / (len(tps) + len(fps))
    recall = len(tps) / (len(tps) + len(fns))
    
    out_dct = {}
    out_dct['TP'] = [len(tps)]
    out_dct['FP'] = [len(fps)]
    out_dct['FN'] = [len(fns)]
    out_dct['Precision'] = [precision]
    out_dct['Recall'] = [recall]
    out_df = pd.DataFrame(out_dct)
    out_df.to_csv(outname + '_stats.csv', index=False)

def pert_accbreakdown(acmpath1, acmpath2, role_res, view_res, priv_res, outname):
    out_schema = ['Role No. Correct', 'Role 1 No. Missing', 'Role 2 No. Missing', 'Role No. Wrong',
                  'View No. Correct', 'View 1 No. Missing', 'View 2 No. Missing', 'View No. Wrong',
                  'Priv No. Correct', 'Priv 1 No. Missing', 'Priv 2 No. Missing', 'Priv No. Wrong']
    out_dct = {}
    for o in out_schema:
        out_dct[o] = []
    #How accurate was LLM4AC at determining that dataframes are the same
    #under a specific perturbation?
    acmdf1 = pd.read_csv(acmpath1)
    acmdf2 = pd.read_csv(acmpath2)
    roledf = pd.read_csv(role_res)
    viewdf = pd.read_csv(view_res)
    privdf = pd.read_csv(priv_res)
    
    #first, evaluate role accuracy
    roles1 = acmdf1['Role'].tolist()
    roles2 = acmdf2['Role'].tolist()
    
    #we know that the ith role in acmdf1 should map to the ith role in acmdf2
    r1_correct = []
    r1_missing = []
    r1_wrong = []
    
    r2_correct = []
    r2_missing = []
    r2_wrong = []
    
    for i,r in enumerate(roles1):
        r2ith = roles2[i]
        if r not in roledf['ACM 1 Role'].values:
            r1_missing += [r]
        
        if r2ith not in roledf['ACM 2 Role'].values:
            r2_missing += [r2ith]
        else:
            newtup = [r, r2ith]
            is_mapped = (roledf[['ACM 1 Role','ACM 2 Role']].values == newtup).all(axis=1).any()
            if is_mapped:
                r1_correct.append(newtup)
                r2_correct.append(newtup)
            else:
                r1_wrong.append(r)
                r2_wrong.append(r2ith)
    
    with open(outname + '_roles1correct.txt', 'w+') as fh:
        print(r1_correct, file=fh)
    
    with open(outname + '_roles2correct.txt', 'w+') as fh:
        print(r2_correct, file=fh)
    
    with open(outname + '_roles1missing.txt', 'w+') as fh:
        print(r1_missing, file=fh)
    
    with open(outname + '_roles2missing.txt', 'w+') as fh:
        print(r2_missing, file=fh)
    
    with open(outname + 'roles1wrong.txt', 'w+') as fh:
        print(r1_wrong, file=fh)
    
    with open(outname + '_roles2wrong.txt', 'w+') as fh:
        print(r2_wrong, file=fh)
    
    out_dct['Role No. Correct'] = [len(r1_correct)]
    out_dct['Role 1 No. Missing'] = [len(r1_missing)]
    out_dct['Role 2 No. Missing'] = [len(r2_missing)]
    out_dct['Role No. Wrong'] = [len(r1_wrong)]
    
    #evaluate view accuracy
    views1 = [c for c in acmdf1.columns.tolist() if c != 'Role']
    views2 = [c for c in acmdf2.columns.tolist() if c != 'Role']
    
    #we know that the ith role in acmdf1 should map to the ith role in acmdf2
    v1_correct = []
    v1_missing = []
    v1_wrong = []
    
    v2_correct = []
    v2_missing = []
    v2_wrong = []
    
    for i,v in enumerate(views1):
        v2ith = views2[i]
        if v not in viewdf['ACM 1 Column'].values:
            v1_missing += [r]
        
        if v2ith not in roledf['ACM 2 Column'].values:
            v2_missing += [r2ith]
        else:
            newtup = [v, v2ith]
            is_mapped = (viewdf[['ACM 1 Column','ACM 2 Column']].values == newtup).all(axis=1).any()
            if is_mapped:
                v1_correct.append(newtup)
                v2_correct.append(newtup)
            else:
                v1_wrong.append(r)
                v2_wrong.append(r2ith)
    
    with open(outname + '_views1correct.txt', 'w+') as fh:
        print(v1_correct, file=fh)
    
    with open(outname + '_views2correct.txt', 'w+') as fh:
        print(v2_correct, file=fh)
    
    with open(outname + '_views1missing.txt', 'w+') as fh:
        print(v1_missing, file=fh)
    
    with open(outname + '_views2missing.txt', 'w+') as fh:
        print(v2_missing, file=fh)
    
    with open(outname + 'views1wrong.txt', 'w+') as fh:
        print(v1_wrong, file=fh)
    
    with open(outname + '_views2wrong.txt', 'w+') as fh:
        print(v2_wrong, file=fh)
    
    out_dct['View No. Correct'] = [len(v1_correct)]
    out_dct['View 1 No. Missing'] = [len(v1_missing)]
    out_dct['View 2 No. Missing'] = [len(v2_missing)]
    out_dct['View No. Wrong'] = [len(v1_wrong)]
    
    #and we'll do privilege accuracy separately

def analyze_tups(tuplst, priv_res):
    privdf = pd.read_csv(priv_res)
    tupdfs = []
    for tup in tuplst:
        tuprows = privdf.query('@tup[0] == `ACM 1 Role` and @tup[1] == `ACM 1 View` and @tup[2] == `ACM 2 Role` and @tup[3] == `ACM 2 View`')
        tupdfs.append(tuprows)
    
    if tupdfs == []:
        fulldf = pd.DataFrame()
    elif len(tupdfs) == 1:
        fulldf = tupdfs[0]
    else:
        fulldf = pd.concat(tupdfs)
    
    return fulldf

def analyze_tpfile(tupfile, priv_res):
    with open(tupfile, 'r') as fh:
        lst = literal_eval(fh.read())
    
    fulldf = analyze_tups(lst, priv_res)
    return fulldf

# def perteq_allstats(base_acm, acm_dir, acm_dir_pref, perts, \
#                     comp_dir_pref, comp_pref, \
#                     diff_dir_pref, diff_pref, \
#                     outdir_pref, outpref, gt_details={}):

def analyze_sqlview(acmpath, sqlpath, view_res, gt_outname):
    
    acmdf = pd.read_csv(acmpath)
    sqldf = pd.read_csv(sqlpath)
    viewdf = pd.read_csv(view_res)
    
    with open(gt_outname, 'r') as fh:
        gt_dct = literal_eval(fh.read())
    
    ind_dct = {k : first(gt_dct[k]) for k in gt_dct}
        
    
    good_rows = []
    bad_rows = []
    missing_queries = []
    no_match = []
    
    for row in viewdf.to_dict(orient='records'):
        matches = [el for el in acmdf.columns if el in row['Explanation']]
        if row['Explanation'] == 'None' and row['ACM 2 Column'] == 'None':
            bad_rows.append(row)
        elif matches == []:
            no_match.append(row)
            continue
        else:
            match_inds = [acmdf.columns.tolist().index(el) for el in matches]
            buckets = [k for k in gt_dct if len(set(gt_dct[k]).intersection(set(match_inds))) > 0]
            full_query = row['ACM 1 Column']
            if 'CREATE VIEW' in full_query:
                query_pts = full_query.split(' ')
                query = ' '.join(query_pts[4:]) # skip the CREATE VIEW x AS
            else:
                query = full_query
            
            if query in gt_dct and query in buckets:
                good_rows.append(row)
            elif query not in buckets and query in ind_dct:
                bad_rows.append(row)
            elif query not in gt_dct:
                missing_queries.append(row)
    
    #now, let's make dataframes for each of these.
    gooddf = pd.DataFrame.from_dict(good_rows)
    baddf = pd.DataFrame.from_dict(bad_rows)
    missingdf = pd.DataFrame.from_dict(missing_queries)
    nomatchdf = pd.DataFrame.from_dict(no_match)
    acm_name = acmpath[:-4]
    
    gooddf.to_csv(acm_name + '_nlvssql_good.csv', index=False)
    baddf.to_csv(acm_name + '_nlvssql_bad.csv', index=False)
    missingdf.to_csv(acm_name + '_nlvssql_missing.csv', index=False)
    nomatchdf.to_csv(acm_name + '_nlvssql_nomatch.csv', index=False)
                
    #print some statistics
    print("Total Shape: {}".format(acmdf.shape))
    print("Good shape: {}".format(gooddf.shape))
    print("Bad shape: {}".format(baddf.shape))
    print("Missing shape: {}".format(missingdf.shape))
    print("No Match shape: {}".format(nomatchdf.shape))

if __name__=='__main__':
    base_acm = 'spider_bike1base_nl.csv'
    pert_acm = 'drspiderperts/drspider_view_syn4.csv'
    orig_pert = 'drspider_view_syn4.csv'
    query_acm = 'spider_bike1base_queries.csv'
    gt_name = 'spider_bike1gt.txt'
    #first, construct the spider gt
    # construct_spidergt(query_acm, base_acm, gt_name)
    
    role_path = 'spidercompsdrspider_view_syn4/compdiffdrspider_view_syn4_rolecomplete.csv'
    view_path = 'spidercompsdrspider_view_syn4/compdiffdrspider_view_syn4_viewcomplete.csv'
    priv_path = 'spiderdiffdrspider_view_syn4/spiderdrspider_view_syn4_privcomplete.csv'
    outdir = 'spiderpertstats_view_syn4'
    outpref = 'pertstats'
    sp_info = {'mode' : 'spider', 'outname' : gt_name}
    #Compute the stats.
    # perteq_mainstats(base_acm, pert_acm, role_path, view_path, priv_path, outdir, outpref, gt_details=sp_info)
    
    #Now, compute stats for SQL vs NL.
    sql_role = 'spidersqlcompsdrspider_view_syn4/sqlcompdiffdrspider_view_syn4_rolecomplete.csv'
    sql_view = 'spidersqlcompsdrspider_view_syn4/sqlcompdiffdrspider_view_syn4_viewcomplete.csv'
    sql_priv = 'spidersqldiffdrspider_view_syn4/spidersqldrspider_view_syn4_privcomplete.csv'
    sql_outdir = 'spidersqlpertstats_view_syn4'
    # perteq_mainstats(query_acm, pert_acm, sql_role, sql_view, sql_priv, sql_outdir, outpref, gt_details=sp_info)
    #analyze parts of the result
    # tupfile = 'spiderpertstats_view_syn4/pertstats_fns.txt'
    # fndf = analyze_tpfile(tupfile, priv_path)
    # print(fndf.shape)
    
    # analyze_sqlview(orig_pert, query_acm, sql_view, gt_name)
    small_base = 'small_10_10_spider_bike1base_nl.csv'
    small_query = 'small_10_10_spider_bike1base_queries.csv'
    small_pert = 'smallbike1perts/drspider_view_syn4.csv'
    small_sqlrole = 'small_10_10_sqlcompsv2drspider_view_syn4/sqlcompdiffdrspider_view_syn4_rolecomplete.csv'
    small_sqlview = 'small_10_10_sqlcompsv2drspider_view_syn4/sqlcompdiffdrspider_view_syn4_viewcomplete.csv'
    small_sqlpriv = 'small_10_10_sqldiffv2drspider_view_syn4/sqldiffdrspider_view_syn4_privcomplete.csv'
    small_sqloutdir = 'small_10_10_sqlv2pertstats_view_syn4'
    perteq_mainstats(small_query, small_pert, small_query, small_sqlrole, small_sqlview, small_sqlpriv, small_sqloutdir, outpref)
    
    
    