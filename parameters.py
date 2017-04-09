import yaml

def load_parameters(parameter):
    with open('parameters.yaml', 'r') as yml:
        par_values = yaml.safe_load(yml)
    return par_values[parameter]