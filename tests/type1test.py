from ast import literal_eval
import pandas as pd
import os

def type1acm_correctness(type0_acm, list_dir, prefix):
    df = pd.read_csv(type0_acm)
    partdf = df[[c for c in df.columns if c != 'Role']]
    non_nas = partdf.count().sum()
    lst_non_nas = 0
    for f in os.listdir(list_dir):
        if f.startswith(prefix):
            fullf = os.path.join(list_dir, f)
            with open(fullf, 'r') as fh:
                lst = literal_eval(fh.read())
                if len(lst) != 0:
                    lst_non_nas += 1
    
    print("Non-NA values in Type 0 ACM: {}".format(non_nas))
    print("Non-NA values in Type 1 ACM: {}".format(lst_non_nas))
    
    if non_nas != lst_non_nas:
        print("Failed!")

if __name__=='__main__':
    type1acm_correctness('dacview_test_type0acm.csv', '.', 'dacview_test_type1acm')
                
        
    
