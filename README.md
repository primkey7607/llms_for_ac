# LLM4AC: Intent-based Access Control for Databases: Using LLMs to Intelligently Manage Access Control
This is the official project page for experiments that uses LLMs to generate access control code. The goal of the project is to support operations to help determine whether database SQL complies with access control matrices specified in natural language by non-expert users.

# Setup
1. This code uses OpenAI's API to prompt ChatGPT. It assumes you have set the ```OPENAI_API_KEY``` with your API key.
2. Our current experiments use the TPC-H database and the following spider databases: ```department_manager```, ```culture_company```, ```bike_1```, ```car_1```, ```student_transcripts_tracking```, ```dog_kennels```, ```employee_hire_evaluation```, and ```orchestra``` databases. Download the Spider benchmark from here: https://yale-lily.github.io/spider and put the databases in postgres.
3. Run ```pip install -r requirements.txt```.

# Differencing Experiments

# Synthesizer Experiment



