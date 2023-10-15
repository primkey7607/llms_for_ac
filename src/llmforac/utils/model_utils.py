from transformers import AutoTokenizer, T5ForConditionalGeneration, TrainingArguments, Trainer
import torch
import pandas as pd

def model_from_disk(path : str):
    raise Exception("Not yet implemented")

def init_hfmodel(model_type='niansong1996/lever-spider-codex'):
    model_name = model_type
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = T5ForConditionalGeneration.from_pretrained(model_name)
    return tokenizer, model

def create_input(example : dict):
    context = example['context']
    code = example['code']
    execution_result_str = example['exec_result']
    input_text = context + code + "\n" + \
             "-- exec result:\n" + "/*\n" + execution_result_str + "\n*/"
    
    return input_text

def infer_hfmodel(model, tokenizer, examples : list):
    prob_corrects = []
    for ex in examples:
        input_text = create_input(ex)
        input_ids = tokenizer(input_text, return_tensors = 'pt').input_ids
        model_result = model.generate(input_ids = input_ids, do_sample = False, max_new_tokens = 2, return_dict_in_generate = True, output_scores = True, num_beams = 1)
        correct_token_idx = 4273 #Index for expected token response --> essentially says that this is correct
        incorrect_token_idx = 150 #Index for wrong token response --> essentially says that this is not correct SQL
        logits = [model_result.scores[0][:, correct_token_idx].item(), model_result.scores[0][:, incorrect_token_idx].item()]
        prob_tens = torch.nn.functional.softmax(torch.tensor(logits))
        prob_lst = prob_tens.tolist()
        prob_corrects.append(max(prob_lst))
    
    return prob_corrects

def execute_example():
    context = "How many heads of the departments are older than 56?"
    code = "SELECT count(*) FROM head WHERE age  >  56"
    execution_result_str = "" #Format here
    model_name = "niansong1996/lever-spider-codex"
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = T5ForConditionalGeneration.from_pretrained(model_name)
    input_text = context + code + "\n" + \
             "-- exec result:\n" + "/*\n" + execution_result_str + "\n*/"
    
    input_ids = tokenizer(input_text, return_tensors = 'pt').input_ids
    model_result = model.generate(input_ids = input_ids, do_sample = False, max_new_tokens = 2, return_dict_in_generate = True, output_scores = True, num_beams = 1)
    correct_token_idx = 4273 #Index for expected token response --> essentially says that this is correct
    incorrect_token_idx = 150 #Index for wrong token response --> essentially says that this is not correct SQL
    logits = [model_result.scores[0][:, correct_token_idx].item(), model_result.scores[0][:, incorrect_token_idx].item()]
    prob_tens = torch.nn.functional.softmax(torch.tensor(logits))
    print(prob_tens)

def execute_one(context, code, execution_result_str):
    model_name = "niansong1996/lever-spider-codex"
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = T5ForConditionalGeneration.from_pretrained(model_name)
    input_text = context + code + "\n" + \
             "-- exec result:\n" + "/*\n" + execution_result_str + "\n*/"
    
    input_ids = tokenizer(input_text, return_tensors = 'pt').input_ids
    model_result = model.generate(input_ids = input_ids, do_sample = False, max_new_tokens = 2, return_dict_in_generate = True, output_scores = True, num_beams = 1)
    correct_token_idx = 4273 #Index for expected token response --> essentially says that this is correct
    incorrect_token_idx = 150 #Index for wrong token response --> essentially says that this is not correct SQL
    logits = [model_result.scores[0][:, correct_token_idx].item(), model_result.scores[0][:, incorrect_token_idx].item()]
    prob_tens = torch.nn.functional.softmax(torch.tensor(logits))
    return prob_tens

def pd_df_from_dict(dt: dict) -> pd.DataFrame:
    return pd.DataFrame.from_dict(dt, orient='tight')

def execute_val(fname):
    df = pd.read_json('spider_codex_verification_dev.jsonl', lines=True)
    #columns Index(['metadata', 'gold_program', 'generated_program', 'generated_programs'], dtype='object')
    all_tens = []
    for row in df.to_dict(orient='records'):
        if row['generated_program']['exec_match'] != 0.0:
            continue
        context = row['metadata']['question']
        code = row['generated_program']['code']
        # exec_result = pd_df_from_dict(row['generated_program']['exec_result']).to_string(index=False)
        exec_result = ''
        prob_tens = execute_one(context, code, exec_result)
        print("Current probability tensor: {}".format(prob_tens))
        print("Is this a match? {}".format(row['generated_program']['exec_match']))
        all_tens.append(prob_tens)
    
    return all_tens

def finetune_t5(pos_example_file, neg_example_file):
    model_name = 't5-base'
    model = T5ForConditionalGeneration.from_pretrained(model_name)
    

if __name__=='__main__':
    # execute_example()
    all_tensors = execute_val('spider_codex_verification_dev.jsonl')
    print(all_tensors)
    with open('spider_devtensors.txt', 'w+') as fh:
        print(all_tensors, file=fh)
