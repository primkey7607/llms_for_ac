# LLM4AC: Intent-based Access Control for Databases: Using LLMs to Intelligently Manage Access Control
This is the official project page for experiments that uses LLMs to generate access control code. The goal of the project is to support operations to help determine whether database SQL complies with access control matrices specified in natural language by non-expert users.

# Setup
1. This code uses OpenAI's API to prompt ChatGPT. It assumes you have set the ```OPENAI_API_KEY``` with your API key.
2. Our current experiments use the TPC-H database and the following spider databases: ```department_manager```, ```culture_company```, ```bike_1```, ```car_1```, ```student_transcripts_tracking```, ```dog_kennels```, ```employee_hire_evaluation```, and ```orchestra``` databases. Download the Spider benchmark from here: https://yale-lily.github.io/spider and put the databases in postgres.
3. Download the Dr. Spider benchmark from here: https://github.com/awslabs/diagnostic-robustness-text-to-sql .
4. Run ```pip install -r requirements.txt```.

# Differencing Experiments
We plan to provide the NLACMs in our benchmark. But if you want to generate them yourself, run ```drspider_to_acms.py``` and ```drspider_rolesprivs.py``` to generate the natural language access control matrices (NLACMs) from the Dr. Spider benchmark. We have used seeds in these scripts to ensure that the resulting NLACMs will be identical to those used in our experiment.
For each of these scripts, change the ```db_name``` variable and run for each of databases: ```car_1```, ```student_transcripts_tracking```, ```dog_kennels```, ```employee_hire_evaluation```, and ```orchestra```.

We have 5 scripts to run experiments, one for each method that the role-view mapping method of LLM4AC can use These are in the ```test_scripts``` directory:

1. ```test_drspider.py```: uses the LLM only (also called "Plain LLM")
2. ```test_tsllmdrspider.py```: uses DB literal pruning and the LLM (this is LLM4AC's default).
3. ```test_semdrspider.py```: uses SentenceBERT to embed role strings and view strings, and then find the best match by comparing them using cosine similarity of their embeddings.
4. ```test_worddrspider.py```: uses BERT to embed tokens in role strings and view strings, averages them, and then uses cosine similarity to compare embeddings. 
5. ```test_textsimdrspider.py```: compares role/view strings using Jaccard similarity.

Running any of these scripts will reproduce our experiments on one database. To decide the database, set the variable ```db_name```. Then, you need only run ```python test_X.py```.

# Synthesizer Experiment
Running ```python synthesizer_exp.py``` will construct the 10 x 33 NLACM used in our experiment, and automatically determine its performance in terms of execution accuracy of the resulting queries.



