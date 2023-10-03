# LLMs for Access Control
This is the official project page for the system that uses LLMs to generate access control code. The goal of the project is to support operations to help determine whether database SQL complies with access control matrices specified in natural language by non-expert users.

We support the following operations
1. Access Control SQL Generation (done): given an access control matrix that can have varying levels of natural language, convert it to SQL code that can be implemented on the database.
2. Access Control Rule Verification (WIP): given an access control matrix and a database with access control rules already implemented, determine how the implemented rules differ from those specified in the access control matrix.
3. Access Control Matrix Comparison (WIP): given two access control matrices expressing rules using potentially different levels of natural language, can we concisely describe which rules are shared by/exclusive to both?
The input is a Type X access control matrix (ACM), and the output is SQL code.

# Setup
1. This code uses OpenAI's API to prompt ChatGPT. It assumes you have set the ```OPENAI_API_KEY``` with your API key.
2. Our current experiments assume the TPC-H dataset is the default database. To simplify the process of running experiments on different machines, we use the TPC-H benchmark as a collection of csvs. To generate this for yourself: (a) Clone this repo, and follow the instructions at this repo to generate the ```.tbl``` files: https://github.com/gregrahn/tpch-kit . (b) Convert the ```.tbl``` files to ```.csv``` files. (c) Put the csvs in directory, ```~/tpch-kit/scale1data/tpchcsvs```. (TODO: add support for directly querying postgres for the role and schema information--we already know how to do this, but we've ).

The main APIs are ```tp{N+1}totpN``` and ```reconstruct_typeN```, and ```gen_tpNsql```. We give them for each type of ACM. For example, if your input ACM is a Type 2 ACM, then you will import ```from type2tosql import tp2totp1, reconstruct_type1, gen_tp1sql```, and call each of these functions consecutively.
TODO (describe code, LLM instructions, and postgres setup)

# Requirements
TODO (will be described in requirements.txt)

