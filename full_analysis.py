import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np
import dataframe_image as dfi

'''
Purpose: analyze the full results files to rank results by method on nl vs nl, and nl vs sql.
Also include in-depth result exploration functions, like--how many mistakes were due to failure in role mapping vs view mapping?
Can we grab the LLM explanations, if there were any? The semantic scores?
'''

#ChatGPT-Generated
def plot_bar_graph(xs, y_vals : list, y_std : list, db_name, comp_type, metric):
    # Convert the x labels to numeric positions
    x_positions = np.arange(len(xs))
    
    # Extract y values and convert them to a NumPy array
    y_means = np.array(y_vals)
    
    # Create a bar graph with error bars
    plt.bar(x_positions, y_means, yerr=y_std, capsize=5, color='blue', alpha=0.7)

    # Add labels and title
    plt.xlabel('Role-View Mapping Method')
    plt.ylabel('Avg. ' + metric)
    plt.title(db_name + ', ' + comp_type + '-' + metric)

    # Add values on top of each bar
    for i, (x, y_mean) in enumerate(zip(x_positions, y_means)):
        plt.text(x, y_mean + 0.01, f'{y_mean:.3f}', ha='center')

    # Set x-axis ticks and labels
    plt.xticks(x_positions, xs)

    # Show the plot
    # plt.show()
    plt.savefig(db_name + '_' + comp_type + '_' + metric + '.png', bbox_inches='tight')
    plt.close()
    
def generate_bargraph(data_dict, 
                     comp_type, 
                     diff_step, 
                     title_suff=' F1 Across DB/Pert', 
                     xlab='Role-View Mapping Method',
                     ylab='F1',
                     fpref='all',
                     fig_sz = None,
                     lims=None):
    # Create lists to store data for boxplot
    labels = list(data_dict.keys())
    raw_data = list(data_dict.values())
    data = [sum(r) / len(r) for r in raw_data]
    
    plt.rcParams.update({'font.size': 22})
    # Create barchart
    plt.bar(labels, data)  # Set showfliers to True to display outliers
    if fig_sz != None:
        fig, ax = plt.subplots(figsize=fig_sz)
    else:
        fig, ax = plt.subplots()
    
    if lims != None:
        ax.set_ylim(lims)
    else:
        max_height = max(data)
        min_lim = 0
        max_lim = max_height + 0.1 * max_height
        ax.set_ylim([min_lim, max_lim])
    
    ax.bar(labels, data)

    # Add labels and title
    plt.xlabel(xlab)
    plt.ylabel(ylab)
    plt.title( diff_step + title_suff)
    
    # Add values on top of each bar
    max_height = max(data)
    val_height = max_height / 100
    for i, (x, y_mean) in enumerate(zip(labels, data)):
        plt.text(x, y_mean + val_height, f'{y_mean:.3f}', ha='center')

    # # Add quantile labels
    # quantiles = [25, 50, 75]
    # for i, label in enumerate(labels):
    #     plt.text(i + 1, max(data[i]), f'Q1: {round(data[i][0], 2)}\nQ2: {round(data[i][1], 2)}\nQ3: {round(data[i][2], 2)}',
    #              verticalalignment='top', horizontalalignment='left', color='blue', fontweight='bold')

    # Show the plot
    # plt.show()
    plt.savefig(fpref + 'f1s_' + diff_step + '_' + comp_type + '.png', bbox_inches='tight')
    plt.close()

# Example usage:
# xs = ['A', 'B', 'C', 'D']
# y_dct = {'A': 5.0, 'B': 7.2, 'C': 3.8, 'D': 9.5}
# y_std = {'A': 0.3, 'B': 0.5, 'C': 0.2, 'D': 0.8}
# plot_bar_graph(xs, y_dct, y_std)

def compute_f1(prec, rec):
    if prec * rec == 0 or prec + rec == 0:
        return 0
    f1 = prec * rec
    f1 = f1 / (prec + rec)
    f1 = 2 * f1
    return f1

def compute_negf1(prec, rec):
    if prec * rec == 0 or prec + rec == 0:
        return 0
    f1 = prec * rec
    f1 = f1 / (prec + rec)
    f1 = 2 * f1
    neg_f1 = 1 - f1
    return neg_f1
    

def rank_methods(db_name, comp_type, pref='nlpert_10_10'):
    all_methods = ['', 'sem', 'word', 'textsim', 'tsllm']
    m_xs = ['Plain LLM', 'Sentence', 'Word', 'Jaccard', 'LLM4AC']
    rvp_ys = []
    rvr_ys = []
    privp_ys = []
    privr_ys = []
    
    rvp_ye = []
    rvr_ye = []
    privp_ye = []
    privr_ye = []
    
    for m in all_methods:
        suff = db_name + m + '_' + comp_type + '_fullstats.csv'
        m_name = m if m != '' else 'llm'
        final_stats = [f for f in os.listdir() if f.startswith(pref) and f.endswith(suff)]
        if len(final_stats) == 0:
            print("No stats for method: {}".format(m_name))
        else:
            stats_f = final_stats[0]
            stats_df = pd.read_csv(stats_f)
            rvp_mean = stats_df['RV_Precision'].mean()
            rvr_mean = stats_df['RV_Recall'].mean()
            privp_mean = stats_df['Priv_Precision'].mean()
            privr_mean = stats_df['Priv_Recall'].mean()
            
            rvp_std = stats_df['RV_Precision'].std()
            rvr_std = stats_df['RV_Recall'].std()
            privp_std = stats_df['Priv_Precision'].std()
            privr_std = stats_df['Priv_Recall'].std()
            
            rvp_ys.append(rvp_mean)
            rvr_ys.append(rvr_mean)
            privp_ys.append(privp_mean)
            privr_ys.append(privr_mean)
            rvp_ye.append(rvp_std)
            rvr_ye.append(rvr_std)
            privp_ye.append(privp_std)
            privr_ye.append(privr_std)
            print("{} Mean RV Precision: {}".format(m_name, rvp_mean))
            print("{} Mean RV Recall: {}".format(m_name, rvr_mean))
            print("{} Mean Priv Precision: {}".format(m_name, privp_mean))
            print("{} Mean Priv Recall: {}".format(m_name, privr_mean))
    
    plot_bar_graph(m_xs, rvp_ys, rvp_ye, db_name, comp_type, 'RV Precision')
    plot_bar_graph(m_xs, rvr_ys, rvr_ye, db_name, comp_type, 'RV Recall')
    plot_bar_graph(m_xs, privp_ys, privp_ye, db_name, comp_type, 'Priv Precision')
    plot_bar_graph(m_xs, privr_ys, privr_ye, db_name, comp_type, 'Priv Recall')

def generate_boxplot(data_dict, 
                     comp_type, 
                     diff_step, 
                     title_suff=' F1 Across DB/Pert', 
                     xlab='Role-View Mapping Method',
                     fpref='all',
                     fig_sz = None,
                     lims=None):
    # Create lists to store data for boxplot
    labels = list(data_dict.keys())
    data = list(data_dict.values())
    
    plt.rcParams.update({'font.size': 14})

    # Create boxplot
    plt.boxplot(data, labels=labels, showfliers=False)  # Set showfliers to True to display outliers
    if fig_sz !=None:
        fig, ax = plt.subplots(figsize=fig_sz)
    else:
        fig, ax = plt.subplots()
    
    if lims != None:
        ax.set_ylim(lims)
    
    ax.boxplot(data, labels=labels, showfliers=False)

    # Add labels and title
    plt.xlabel(xlab)
    plt.ylabel('1-F1')
    # plt.title( diff_step + title_suff)

    # # Add quantile labels
    # quantiles = [25, 50, 75]
    # for i, label in enumerate(labels):
    #     plt.text(i + 1, max(data[i]), f'Q1: {round(data[i][0], 2)}\nQ2: {round(data[i][1], 2)}\nQ3: {round(data[i][2], 2)}',
    #              verticalalignment='top', horizontalalignment='left', color='blue', fontweight='bold')

    # Show the plot
    # plt.show()
    plt.savefig(fpref + 'f1s_' + diff_step + '_' + comp_type + '.png', bbox_inches='tight')
    plt.close()

# # Example dictionary
# data_dictionary = {
#     'Label1': [3, 6, 8],
#     'Label2': [5, 7, 10],
#     'Label3': [2, 4, 6],
#     'Label4': [8, 12, 15]
# }

# # Generate and display the boxplot
# generate_boxplot(data_dictionary)

    

def complete_results(comp_type, pref='nlpert_10_10'):
    all_methods = ['', 'sem', 'word', 'textsim', 'tsllm']
    m_xs = ['Plain LLM', 'Sentence', 'Word', 'Jaccard', 'LLM4AC']
    db_names = ['orchestra', 'dog_kennels', 'car_1', 'employee_hire_evaluation', 'student_transcripts_tracking']
    
    rvplot_dict = {}
    for m in m_xs:
        rvplot_dict[m] = []
    
    privplot_dict = {}
    for m in m_xs:
        privplot_dict[m] = []
    
    
    for m in all_methods:
        rvf1s = []
        privf1s = []
        m_name = m_xs[all_methods.index(m)]
        for db_name in db_names:
            suff = db_name + m + '_' + comp_type + '_fullstats.csv'
            final_stats = [f for f in os.listdir() if f.startswith(pref) and f.endswith(suff)]
            if len(final_stats) == 0:
                print("No stats for method: {}".format(m_name))
            else:
                stats_f = final_stats[0]
                stats_df = pd.read_csv(stats_f)
                
                for row in stats_df.to_dict(orient='records'):
                    rvp = row['RV_Precision']
                    rvr = row['RV_Recall']
                    privp = row['Priv_Precision']
                    privr = row['Priv_Recall']
                    
                    rvf1 = compute_negf1(rvp, rvr)
                    privf1 = compute_negf1(privp, privr)
                    
                    rvf1s.append(rvf1)
                    privf1s.append(privf1)
                
        rvplot_dict[m_name] += rvf1s
        privplot_dict[m_name] += privf1s
    
    # generate_boxplot(rvplot_dict, comp_type, 'RV')
    # generate_boxplot(privplot_dict, comp_type, 'Priv')
    
    generate_bargraph(rvplot_dict, comp_type, 'RV', title_suff=' 1-F1 Across DB/Pert', ylab='Avg 1-F1', fig_sz=(15,6))
    generate_bargraph(privplot_dict, comp_type, 'Priv', title_suff=' 1-F1 Across DB/Pert', ylab='Avg 1-F1', fig_sz=(15,6))

def roll_bypert(comp_type, pref='nlpert_10_10'):
    all_methods = ['', 'sem', 'word', 'textsim', 'tsllm']
    m_xs = ['Plain LLM', 'Sentence', 'Word', 'Jaccard', 'LLM4AC']
    db_names = ['orchestra', 'dog_kennels', 'car_1', 'employee_hire_evaluation', 'student_transcripts_tracking']
    pert_names = ['multitype', 'column_attribute', 'others',
                  'value_synonym', 'keyword_carrier', 'column_value',
                  'column_synonym', 'keyword_synonym', 'column_carrier',
                  'role_syn', 'roledesc_replace', 'priv_syn', 'priv_inf']
    
    rvplot_dict = {}
    for m in m_xs:
        rvplot_dict[m] = []
    
    privplot_dict = {}
    for m in m_xs:
        privplot_dict[m] = []
    
    all_dfs = {}
    for m in all_methods:
        m_name = m_xs[all_methods.index(m)]
        m_dfs = []
        for db_name in db_names:
            suff = db_name + m + '_' + comp_type + '_fullstats.csv'
            final_stats = [f for f in os.listdir() if f.startswith(pref) and f.endswith(suff)]
            if len(final_stats) == 0:
                print("No stats for method: {}".format(m_name))
            else:
                stats_f = final_stats[0]
                stats_df = pd.read_csv(stats_f)
                m_dfs.append(stats_df)
        
        all_dfs[m_name] = m_dfs
    
    for m in all_methods:
        m_name = m_xs[all_methods.index(m)]
        for p in pert_names:
            rvtps = 0
            rvfps = 0
            rvfns = 0
            privtps = 0
            privfps = 0
            privfns = 0
            
            for df in all_dfs[m_name]:
                pertrows = df[df['Perturbation'] == p]
                rvtps += pertrows['RV_TP'].tolist()[0]
                rvfps += pertrows['RV_FP'].tolist()[0]
                rvfns += pertrows['RV_FN'].tolist()[0]
                privtps += pertrows['Priv_TP'].tolist()[0]
                privfps += pertrows['Priv_FP'].tolist()[0]
                privfns += pertrows['Priv_FN'].tolist()[0]
            
            if rvtps == 0 and rvfps == 0:
                rvprec = 0
            else:
                rvprec = rvtps / (rvtps + rvfps)
            
            if rvtps == 0 and rvfns == 0:
                rvrec = 0
            else:
                rvrec = rvtps / (rvtps + rvfns)
            
            rvf1 = compute_f1(rvprec, rvrec)
            
            if privtps == 0 and privfps == 0:
                privprec = 0
            else:
                privprec = privtps / (privtps + privfps)
            
            if privtps == 0 and privfns == 0:
                privrec = 0
            else:
                privrec = privtps / (privtps + privfns)
            
            privf1 = compute_f1(privprec, privrec)
            
            rvplot_dict[m_name] += [rvf1]
            privplot_dict[m_name] += [privf1]
    
    rv_df = pd.DataFrame(rvplot_dict)
    priv_df = pd.DataFrame(privplot_dict)
    
    rv_df.index = pert_names
    priv_df.index = pert_names
    
    rv_df.to_csv('rvf1sbypert_' + comp_type + '_raw.csv')
    priv_df.to_csv('privf1sbypert_' + comp_type + '_raw.csv')
    
    rv_df = rv_df.round(3)
    priv_df = priv_df.round(3)
    
    # rv_styled = rv_df.style.highlight_max(color='green', axis=1)
    # priv_styled = priv_df.style.highlight_max(color='green', axis=1)
    rv_styled = rv_df.style.format('{:.3f}').background_gradient(axis=1)
    priv_styled = priv_df.style.format('{:.3f}').background_gradient(axis=1)
    dfi.export(rv_styled, 'rvf1sbypert_' + comp_type + '.png', table_conversion="matplotlib")
    dfi.export(priv_styled, 'privf1sbypert_' + comp_type + '.png', table_conversion="matplotlib")

def roll_bydb(comp_type, pref='nlpert_10_10'):
    all_methods = ['', 'sem', 'word', 'textsim', 'tsllm']
    m_xs = ['Plain LLM', 'Sentence', 'Word', 'Jaccard', 'LLM4AC']
    db_names = ['orchestra', 'dog_kennels', 'car_1', 'employee_hire_evaluation', 'student_transcripts_tracking']
    pert_names = ['multitype', 'column_attribute', 'others',
                  'value_synonym', 'keyword_carrier', 'column_value',
                  'column_synonym', 'keyword_synonym', 'column_carrier',
                  'role_syn', 'roledesc_replace', 'priv_syn', 'priv_inf']
    
    rvplot_dict = {}
    for m in m_xs:
        rvplot_dict[m] = []
    
    privplot_dict = {}
    for m in m_xs:
        privplot_dict[m] = []
    
    all_dfs = {}
    for m in all_methods:
        m_name = m_xs[all_methods.index(m)]
        
        for db_name in db_names:
            mdb_dfs = []
            suff = db_name + m + '_' + comp_type + '_fullstats.csv'
            final_stats = [f for f in os.listdir() if f.startswith(pref) and f.endswith(suff)]
            if len(final_stats) == 0:
                print("No stats for method: {}".format(m_name))
            else:
                stats_f = final_stats[0]
                stats_df = pd.read_csv(stats_f)
                mdb_dfs.append(stats_df)
                all_dfs[(m_name, db_name)] = mdb_dfs
    
    for m in all_methods:
        m_name = m_xs[all_methods.index(m)]
        for db_name in db_names:
            rvtps = 0
            rvfps = 0
            rvfns = 0
            privtps = 0
            privfps = 0
            privfns = 0
            
            for df in all_dfs[(m_name, db_name)]:
                rvtps += df['RV_TP'].sum()
                rvfps += df['RV_FP'].sum()
                rvfns += df['RV_FN'].sum()
                privtps += df['Priv_TP'].sum()
                privfps += df['Priv_FP'].sum()
                privfns += df['Priv_FN'].sum()
            
            if rvtps == 0 and rvfps == 0:
                rvprec = 0
            else:
                rvprec = rvtps / (rvtps + rvfps)
            
            if rvtps == 0 and rvfns == 0:
                rvrec = 0
            else:
                rvrec = rvtps / (rvtps + rvfns)
            
            rvf1 = compute_f1(rvprec, rvrec)
            
            if privtps == 0 and privfps == 0:
                privprec = 0
            else:
                privprec = privtps / (privtps + privfps)
            
            if privtps == 0 and privfns == 0:
                privrec = 0
            else:
                privrec = privtps / (privtps + privfns)
            
            privf1 = compute_f1(privprec, privrec)
            
            rvplot_dict[m_name] += [rvf1]
            privplot_dict[m_name] += [privf1]
    
    rv_df = pd.DataFrame(rvplot_dict)
    priv_df = pd.DataFrame(privplot_dict)
    
    rv_df.index = db_names
    priv_df.index = db_names
    
    rv_df.to_csv('rvf1sbydb_' + comp_type + '_raw.csv')
    priv_df.to_csv('privf1sbydb_' + comp_type + '_raw.csv')
    
    
    # rv_styled = rv_df.style.highlight_max(color='green', axis=1)
    # priv_styled = priv_df.style.highlight_max(color='green', axis=1)
    rv_styled = rv_df.style.format('{:.3f}').background_gradient(axis=1)
    priv_styled = priv_df.style.format('{:.3f}').background_gradient(axis=1)
    dfi.export(rv_styled, 'rvf1sbydb_' + comp_type + '.png', table_conversion="matplotlib")
    dfi.export(priv_styled, 'privf1sbydb_' + comp_type + '.png', table_conversion="matplotlib")

######################################MicroBench: LLM4AC Only##############################
def sys_bydb(comp_type, pref='nlpert_10_10'):
    all_methods = ['tsllm']
    m_xs = ['LLM4AC']
    db_names = ['orchestra', 'dog_kennels', 'car_1', 'employee_hire_evaluation', 'student_transcripts_tracking']
    pretty_dbs = ['orchestra', 'dog_kennels', 'car_1', 'employee', 'student']
    
    rvplot_dict = {}
    for db in pretty_dbs:
        rvplot_dict[db] = []
    
    privplot_dict = {}
    for db in pretty_dbs:
        privplot_dict[db] = []
    
    
    for m in all_methods:
        m_name = m_xs[all_methods.index(m)]
        for db_name in db_names:
            pretty_db = pretty_dbs[db_names.index(db_name)]
            rvf1s = []
            privf1s = []
            suff = db_name + m + '_' + comp_type + '_fullstats.csv'
            final_stats = [f for f in os.listdir() if f.startswith(pref) and f.endswith(suff)]
            if len(final_stats) == 0:
                print("No stats for method: {}".format(m_name))
            else:
                stats_f = final_stats[0]
                stats_df = pd.read_csv(stats_f)
                
                for row in stats_df.to_dict(orient='records'):
                    rvp = row['RV_Precision']
                    rvr = row['RV_Recall']
                    privp = row['Priv_Precision']
                    privr = row['Priv_Recall']
                    
                    rvf1 = compute_negf1(rvp, rvr)
                    privf1 = compute_negf1(privp, privr)
                    
                    rvf1s.append(rvf1)
                    privf1s.append(privf1)
                
            rvplot_dict[pretty_db] += rvf1s
            privplot_dict[pretty_db] += privf1s
    
    generate_boxplot(rvplot_dict, comp_type, 'RV', title_suff=' LLM4AC F1 Across DB', xlab='Database', fpref='llm4acdb', fig_sz=(15, 4), lims=[0,0.2])
    generate_boxplot(privplot_dict, comp_type, 'Priv', title_suff=' LLM4AC F1 Across DB', xlab='Database', fpref='llm4acdb', fig_sz=(15, 4), lims=[0,0.2])

def sys_bypert(comp_type, pref='nlpert_10_10'):
    all_methods = ['tsllm']
    m_xs = ['LLM4AC']
    pert_names = ['multitype', 'column_attribute', 'others',
                  'value_synonym', 'keyword_carrier', 'column_value',
                  'column_synonym', 'keyword_synonym', 'column_carrier',
                  'role_syn', 'roledesc_replace', 'priv_syn', 'priv_inf']
    pretty_perts = ['multi', 'col_attr', 'other', 'val_syn',
                    'kw_\ncarrier', 'col_val', 'col_syn', 'kw_syn',
                    'col_\ncarrier', 'role_syn', 'role_desc', 'priv_syn', 'priv_inf']
    db_names = ['orchestra', 'dog_kennels', 'car_1', 'employee_hire_evaluation', 'student_transcripts_tracking']
    
    rvplot_dict = {}
    for pert in pretty_perts:
        rvplot_dict[pert] = []
    
    privplot_dict = {}
    for pert in pretty_perts:
        privplot_dict[pert] = []
    
    
    for m in all_methods:
        m_name = m_xs[all_methods.index(m)]
        for pert_name in pert_names:
            pretty_pert = pretty_perts[pert_names.index(pert_name)]
            rvf1s = []
            privf1s = []
            for db_name in db_names:
                suff = db_name + m + '_' + comp_type + '_fullstats.csv'
                final_stats = [f for f in os.listdir() if f.startswith(pref) and f.endswith(suff)]
                if len(final_stats) == 0:
                    print("No stats for method: {}".format(m_name))
                else:
                    stats_f = final_stats[0]
                    stats_df = pd.read_csv(stats_f)
                    statp_df = stats_df[stats_df['Perturbation'] == pert_name]
                    
                    for row in statp_df.to_dict(orient='records'):
                        rvp = row['RV_Precision']
                        rvr = row['RV_Recall']
                        privp = row['Priv_Precision']
                        privr = row['Priv_Recall']
                        
                        rvf1 = compute_negf1(rvp, rvr)
                        privf1 = compute_negf1(privp, privr)
                        
                        rvf1s.append(rvf1)
                        privf1s.append(privf1)
                    
                rvplot_dict[pretty_pert] += rvf1s
                privplot_dict[pretty_pert] += privf1s
    
    generate_boxplot(rvplot_dict, comp_type, 'RV', title_suff=' LLM4AC F1 Across Perturbation', xlab='Perturbation', fpref='llm4acpert', fig_sz=(15, 4), lims=[0,0.3])
    generate_boxplot(privplot_dict, comp_type, 'Priv', title_suff=' LLM4AC F1 Across Perturbation', xlab='Perturbation', fpref='llm4acpert', fig_sz=(15, 4), lims=[0,0.3])

def sociome_vs_rest(comp_type, sociome_resdir, spider_resdir, pref='nlpert_10_10'):
    out_schema = ['Perturbation', 'Dr. Spider\nAvg. RV F1', 'Sociome\nRV F1',
                  'Dr. Spider\nAvg. Priv F1', 'Sociome\nPriv F1']
    out_dct = {}
    for o in out_schema:
        out_dct[o] = []
    
    sp_perfdct = {}
    
    all_methods = ['tsllm']
    m_xs = ['LLM4AC']
    db_names = ['orchestra', 'dog_kennels', 'car_1', 'employee_hire_evaluation', 'student_transcripts_tracking']
    
    rvplot_dict = {}
    for m in m_xs:
        rvplot_dict[m] = []
    
    privplot_dict = {}
    for m in m_xs:
        privplot_dict[m] = []
    
    
    for m in all_methods:
        # rvf1s = []
        # privf1s = []
        m_name = m_xs[all_methods.index(m)]
        for db_name in db_names:
            suff = db_name + m + '_' + comp_type + '_fullstats.csv'
            final_stats = [os.path.join(spider_resdir, f) for f in os.listdir(spider_resdir) if f.startswith(pref) and f.endswith(suff)]
            if len(final_stats) == 0:
                print("No stats for method: {}".format(m_name))
            else:
                stats_f = final_stats[0]
                stats_df = pd.read_csv(stats_f)
                
                for row in stats_df.to_dict(orient='records'):
                    rvp = row['RV_Precision']
                    rvr = row['RV_Recall']
                    privp = row['Priv_Precision']
                    privr = row['Priv_Recall']
                    
                    rvf1 = compute_f1(rvp, rvr)
                    privf1 = compute_f1(privp, privr)
                    if row['Perturbation'] not in sp_perfdct:
                        sp_perfdct[row['Perturbation']] = []
                    
                    sp_perfdct[row['Perturbation']] += [(rvf1, privf1)]
    
    socpath = os.path.join(sociome_resdir, 'sociome_basetsllm_' + comp_type + '_fullstats.csv')
    socdf = pd.read_csv(socpath)
    
    for pert in sp_perfdct:
        if pert not in socdf['Perturbation'].tolist():
            continue
        
        rvf1s = [tup[0] for tup in sp_perfdct[pert]]
        privf1s = [tup[1] for tup in sp_perfdct[pert]]
        avg_rvf1 = sum(rvf1s) / len(rvf1s)
        avg_privf1 = sum(privf1s) / len(privf1s)
        
        pertdf = socdf[socdf['Perturbation'] == pert]
        srvf1s = []
        sprivf1s = []
        for row in pertdf.to_dict(orient='records'):
            srvf1 = compute_f1(row['RV_Precision'], row['RV_Recall'])
            sprivf1 = compute_f1(row['Priv_Precision'], row['Priv_Recall'])
            srvf1s.append(srvf1)
            sprivf1s.append(sprivf1)
        
        out_dct['Perturbation'] += [pert]
        out_dct['Dr. Spider\nAvg. RV F1'] += [avg_rvf1]
        out_dct['Sociome\nRV F1'] += [srvf1s[0]]
        # 'Dr. Spider\nAvg. Priv 1-F1', 'Sociome\nPriv 1-F1'
        out_dct['Dr. Spider\nAvg. Priv F1'] += [avg_privf1]
        out_dct['Sociome\nPriv F1'] += [sprivf1s[0]]
    
    clean_dct = {}
    for o in out_dct:
        if o != 'Perturbation':
            clean_dct[o] = out_dct[o]
    
    clean_perts = out_dct['Perturbation']
    out_df = pd.DataFrame(clean_dct)
    out_df.index = clean_perts
    out_df.index.name = 'Perturbation'
    out_df.to_csv('sociomevsspider_' + comp_type + '.csv')
    
    out_styled = out_df.style.format('{:.3f}').background_gradient(axis=1)
    dfi.export(out_styled, 'sociomevsspider_' + comp_type + '.png', table_conversion="matplotlib")
    
        
    

if __name__=='__main__':
    # db_name = 'orchestra'
    # comp_type = 'nlvsnl'
    # rank_methods(db_name, comp_type)
    # complete_results('nlvsnl', pref='synth_10_10')
    # complete_results('nlvssql', pref='synth_10_10')
    roll_bypert('nlvsnl', pref='synth_10_10')
    roll_bypert('nlvssql', pref='synth_10_10')
    roll_bydb('nlvsnl', pref='synth_10_10')
    roll_bydb('nlvssql', pref='synth_10_10')
    # sys_bydb('nlvsnl', pref='synth_10_10')
    # sys_bydb('nlvssql', pref='synth_10_10')
    # sys_bypert('nlvsnl', pref='synth_10_10')
    # sys_bypert('nlvssql', pref='synth_10_10')
    # sociome_vs_rest('nlvsnl', '../sociome_basetsllm_allresults', '../llm4acdiffres/synth_res/synth_results',
    #                 pref='synth_10_10')
    # sociome_vs_rest('nlvssql', '../sociome_basetsllm_allresults', '../llm4acdiffres/synth_res/synth_results',
    #                 pref='synth_10_10')