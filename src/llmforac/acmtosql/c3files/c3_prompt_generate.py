import json
import argparse

def parse_option():
    parser = argparse.ArgumentParser("command line arguments for generate prompt")
    parser.add_argument("--input_dataset_path", type=str)
    parser.add_argument("--output_dataset_path", type=str)

    opt = parser.parse_args()

    return opt


if __name__ == "__main__":
    opt = parse_option()
    print(opt)
    with open(opt.input_dataset_path) as f:
        data_all = json.load(f)
    temp = []
    for id, data in enumerate(data_all):
        data['input_sequence'] = "### Complete postgres SQL statement only and with no explanation, and do not grant privileges on tables, roles, and users that are not explicitly requested in the statement. " \
                        "\n ### Postgres SQL tables, with their properties: \n#\n"
        schema = ""
        for tab, cols in data['schema'].items():
            schema += '# ' + tab + ' ( '
            for i, col in enumerate(cols):
                schema += col
                if data['db_contents'][tab][str(i)]:
                    schema += '("'
                    for value in data['db_contents'][tab][str(i)]:
                        schema += value + '", "'
                    schema = schema[:-4] + '")'
                schema += ', '
            schema = schema[:-2] + ' )\n'
        data['input_sequence'] += schema[:-1]
        for fk in data['fk']:
            data['input_sequence'] += '\n# ' + fk
        
        qpref = '\nGRANT'
        if data['question'].startswith('Create'):
            qpref = '\nCREATE'
        
        data['input_sequence'] += '\n#\n### ' + data['question'] + qpref
    with open(opt.output_dataset_path, 'w') as f:
        json.dump(data_all, f, indent=2)

