import sys
import os
import csv

from datetime import datetime
#

import ftrack_api
from ftrack_action_handler.action import BaseAction

def get_delimiter(file_path, bytes = 4096):
    sniffer = csv.Sniffer()
    data = open(file_path, "r").read(bytes)
    delimiter = sniffer.sniff(data).delimiter
    return delimiter

def load_dependencies_path():
    _cwd = os.path.dirname(__file__)
    _sources_path = os.path.abspath(os.path.join(_cwd, '..', 'dependencies'))

    if _sources_path not in sys.path:
        sys.path.append(_sources_path)

def execute_action():
    import pandas as pd

    csv_file = r"C:\Users\T-Gamer\AppData\Local\ftrack\ftrack-connect-plugins\import-project\hook\data_to_read\excel.csv"
    txt_file = r"C:\Users\T-Gamer\AppData\Local\ftrack\ftrack-connect-plugins\import-project\hook\teste.txt"

    sheet = pd.read_csv(csv_file, sep = get_delimiter(csv_file))

    with open(txt_file, "w") as text_file:
        for item in sheet['ID']:
            text_file.write(item)   
            text_file.write("\n")     
    
def write_log(message):
    
    action_log = os.path.realpath(os.path.join(os.path.join(os.path.dirname(__file__),"..","logs", "action_log.log")))
    
    message = str(message)
    now = datetime.now()
    
    with open(action_log, "a") as log_file:      
        log_file.write(now.strftime("%d/%m/%Y %H:%M:%S") + " " + message)
        log_file.write("\n")
    
# Loading Dependencies

load_dependencies_path()

class MyCustomAction(BaseAction):
    '''Import Project Data Using CSV File'''

    identifier = 'import.csv.project'
    label = 'Import Project'
    variant = None
    description = 'This is an example action'
    
    def discover(self, session, entities, event):
                
        if len(entities) != 1:
            return

        entity_type, entity_id = entities[0]
        if entity_type != 'Project':
            return

        return True

    def launch(self, session, entities, event):
            
        try:    
            entity_type, entity_id = entities[0] 
            project = session.query('Project where id is "{0}"'.format(entity_id)).one()
            
            return {
                'success': True,
                'message': project["name"],
            }
            
        except Exception as error:
            write_log(error)
            
            return {
                'success': False,
                'message': "Something Went Wrong, Please Check Log File",
            }

def register(session, **kw):

    if not isinstance(session, ftrack_api.session.Session):
        return

    action_handler = MyCustomAction(session)
    action_handler.register()


if __name__ == '__main__':
                            
    session = ftrack_api.Session()
    register(session)

    # Wait for events
    session.event_hub.wait()

        