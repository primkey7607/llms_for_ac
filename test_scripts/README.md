# ACM Differencing Test Scripts
This is where all the test scripts live. To run any of them, move them into the top directory and then simply do "python script_x.py". The scripts are divided by baseline:

1. test_drspider.py: LLM only, naive prompts. Currently only NL perturbations on views.
2. test_semdrspider.py: Semantic similarity. Currently only NL perturbations on views.

DBs tested:
1. bike_1
2. orchestra

(to test others, do CREATE DATABASE db_name, and replace the parameter db_name in the scripts)
