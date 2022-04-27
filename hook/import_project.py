from distutils.fancy_getopt import wrap_text
import sys
import os
import csv

from datetime import datetime

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
            
def write_log(message = None, reset = False):
    
    if reset:
        with open(action_log, "w") as log_file:      
            log_file.write("\n")
    
    action_log = os.path.realpath(os.path.join(os.path.join(os.path.dirname(__file__),"..","logs", "action_log.log")))
    
    message = str(message)
    now = datetime.now()
        
    with open(action_log, "a") as log_file:      
        log_file.write(now.strftime("%d/%m/%Y %H:%M:%S") + " " + message)
        log_file.write("\n")
    
# Loading Dependencies

load_dependencies_path()

class CreateProjectStructure:
    
    project = None
    
    def __init__(self, session, project):
        
        self.project = project
        
        self._clear_project_structure(session)
        self.execute_action(session)
        
        session.commit()

    def execute_action(self,session):
        
        write_log("Executing Action")
        
        csv_folder = r"C:\Users\T-Gamer\AppData\Local\ftrack\ftrack-connect-plugins\import-project\hook\data_to_read"
        
        if os.path.exists(csv_folder):
            for csv_file in os.listdir(csv_folder):
                
                csv_file = os.path.join(csv_folder, csv_file)
                self._generate_project_structure_from_csv(session, csv_file)

    def _generate_project_structure_from_csv(self, session, csv_file):
        
        import pandas as pd
        
        sheet = pd.read_csv(csv_file, sep = get_delimiter(csv_file))

        pastas = []
        for item in sheet['Pasta']:
            if not item in pastas:
                pastas.append(item)
        
        for pasta in pastas:       
            folder = self._create_folder(pasta,session)
            folder_task_dataframe = sheet.loc[sheet['Pasta'] == pasta]
            
            for task_row_data in folder_task_dataframe.itertuples():              
                self._create_task(task_row_data, folder, session)
                session.commit() 
                                          
    def _create_task(self, task_row_data, parent, session):
        
        task_dict = task_row_data.__dict__
                
        write_log("Creating Task: " + task_dict["Nome"])
                
        task = session.create('Task', {
                'name': task_dict["Nome"],
                'parent' : parent,
            })
        
        write_log(str(task_dict["Incoming"]))
        
        if not str(task_dict["Incoming"]) == "nan": 
            
            write_log("Creating Task Link: " + task_dict["Incoming"])
            outcoming_task = session.query('Task where name is ' + task_dict["Incoming"]).one()
     
            if outcoming_task:
                self._create_task_link(session, outcoming_task, task)
            
        return task   

    def _create_task_link(self, session, link_from, link_to):
        
        task_link = session.create('TypedContextLink', {
                'from': link_to,
                'to' : link_from
            })
    
        return task_link
    
    def _create_folder(self, name, session):
        
        write_log("Creating Service Folder: " + name)
        
        folder = session.create('Folder', {
                'name': name,
                'parent' : self.project
            })
        
        return folder
                    
    def _clear_project_structure(self, session):
        
        write_log("Clearing Project Structure")
        
        project_folders = session.query('Folder where parent.id is ' + str(self.project['id']))
        
        if project_folders:
            for folder in project_folders:                                  
                session.delete(folder)
            
            session.commit()        

class MyCustomAction(BaseAction):
    '''Import Project Data Using CSV File'''

    identifier = 'import.csv.project'
    label = 'Import Project safes'
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
            
            if project["id"] == "1993a0ce-c562-11ec-97b1-02a0d5fc5f47": # For Safety reasons, this action will execute only in this project
                
                CreateProjectStructure(session, project)
                
                return {
                'success': True,
                'message': project["name"],
                }
            
            else:
                return {
                    'success': False,
                    'message': "Invalid Project, current project-id: " + str(entity_id),
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
    
    write_log(reset = True)
                            
    session = ftrack_api.Session()
    register(session)

    # Wait for events
    session.event_hub.wait()

        