import json
from .schemas import *

def load_base_endpoints(router,base_endpoint_function,endpoint_settings_path:str)->None:    
    with open(endpoint_settings_path, 'r') as file: 
        data = json.load(file)
    for module_name,class_settings in data.items():
        class_name=class_settings['class']
        module=globals()[class_name]
        router.add_api_route(f'/{module_name}',base_endpoint_function(module,class_name) , methods=["GET"])
        print(f'registered endpoint:{module_name}')