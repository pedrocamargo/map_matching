

import yaml
import os


def load_parameters(parameter):
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'parameters.yaml'), 'r') as yml:
        par_values = yaml.safe_load(yml)
    return par_values[parameter]