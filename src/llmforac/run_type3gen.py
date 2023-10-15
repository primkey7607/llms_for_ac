from acm_datagen.gen_type3acm import type2totype3, construct_type3

def run_type3example():
    type2totype3('acmgen/dacview_test_type2acm.csv', ['desc_replace'], 'acmgen/dacview_type3_allsents', 'dacview_test_type3')
    construct_type3('acmgen/dacview_test_type2acm.csv', 0, 'acmgen/dacview_type3_allsents', 'dacview_test_type3', 'dacview_test_type3acm')
    
if __name__=='__main__':
    run_type3example()

