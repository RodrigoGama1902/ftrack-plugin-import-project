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

def write_csv_log(csv_path, message, reset = False):
    
    csv_log = None
    
    if os.path.exists(csv_path):
        csv_path = os.path.dirname(csv_path)
        csv_log = os.path.join(csv_path, "csv_log.log")
    else:
        return   
    
    if reset:
        with open(csv_log, "w") as log_file:      
            log_file.write("\n")
        
    message = str(message)
    now = datetime.now()
        
    with open(csv_log, "a") as log_file:      
        log_file.write(now.strftime("%d/%m/%Y %H:%M:%S") + " " + message)
        log_file.write("\n")
    
# Loading Dependencies

load_dependencies_path()

class CreateTask:
    '''Create a new task inside a parent'''
        
    def __init__(self, task_dict, parent, session):
                                
        if session.query('Task where name is "{}"'.format(task_dict["Nome"])).first():
            write_log("Task already exists")
            return
        
        write_log("Creating Task: " + task_dict["Nome"])
                
        task = session.create('Task', {
            
                'parent' : parent,
                
                'name': self._get_correct_task_attribute(task_dict["Nome"],"Name"),
                'description': self._get_correct_task_attribute(task_dict["Description"],""),
                'type': self._get_entity_by_name(session, "Type", task_dict["Type"], "Production"),
                'priority': self._get_entity_by_name(session, "Priority", task_dict["Priority"], "P1"),
                'scopes': [self._get_entity_by_name(session, "Scope", task_dict["Scope"], "None"),],
                'status': self._get_entity_by_name(session, "Status", task_dict["Status"], "Not Started"),
                'custom_attributes': {
                    'SKU': self._get_correct_task_attribute(task_dict["SKU"],""),
                    'Height': self._get_correct_task_attribute(task_dict["Height"],""),
                    'Width': self._get_correct_task_attribute(task_dict["Width"],""),
                    'Depth': self._get_correct_task_attribute(task_dict["Depth"],""),
                    
                },
            })
        
        self._task_assigment(session, task_dict, task)
        self._create_task_link(session, task_dict, task)   
               
        self.task = task
        
        session.commit()
        
    def _create_task_link(self, session, task_dict, task):
        
        incoming_task_name = str(task_dict["Incoming"])
        
        if not incoming_task_name == "nan":
            
            write_log("Creating Task Link: " + incoming_task_name)
            incoming_task = session.query('Task where name is ' + incoming_task_name)
            
            if incoming_task:
                task_link = session.create('TypedContextLink', {
                        'from': incoming_task.one(),
                        'to' : task
                    })
    
    @staticmethod
    def _get_entity_by_name(session, entity_type, name, default_name):
        
        entity = session.query(entity_type + " where name is '" + name + "'")
        
        if entity:
            return entity.one()
        else:
            return session.query(entity_type + " where name is '" + default_name + "'").one()
        
    @staticmethod
    def _task_assigment(session, task_dict, task):
        
        assignee = str(task_dict["Assignee"])
        
        if assignee == "nan":
            return
        
        user = session.query('User where username is "' + assignee + '"')
        
        if user:
            session.create('Appointment', {
                'context': task,
                'resource': user.one(),
                'type': 'assignment'
            })
 
    @staticmethod
    def _get_correct_task_attribute(value, default):
        
        if str(value) == "nan":
            return default
        else:
            return value
        
    
    def get_task(self):
        return self.task


class CreateFolder:
    '''Create a new folder inside a project root'''
    
    folder = None
    
    def __init__(self, name, project, session):
        
        if session.query('Folder where name is "{}"'.format(name)).first():
            write_log("Folder already exists")
            return
        
        write_log("Creating Service Folder: " + name)

        self.folder = session.create('Folder', {
                'name': name,
                'parent' : project
            })
        
    def get_folder(self):
        return self.folder


class CreateProjectStructure:
    '''Create Project Structure from CSV'''
    
    project = None
    values = None
    
    def __init__(self, session, project, values):
        
        self.project = project
        self.values = values
        
        if values["clear_project_structure"]:  
            self._clear_current_project_structure(session, project)
            
        self._create_project_structure(session)
        
        session.commit()
        
    def _create_project_structure(self, session):
        
        #csv_files = self._load_csv_files(r"C:\Users\T-Gamer\AppData\Local\ftrack\ftrack-connect-plugins\import-project\hook\data_to_read")
        
        #for file in csv_files:
        #    self._generate_project_structure_from_csv(session, self.project, file)
            
         
        csv_files = self.values["csv_paths"].split(",")
        csv_files = [path.strip() for path in csv_files]
        
        for file in csv_files:
            if not os.path.exists(file):
                continue
            
            if not file.endswith(".csv"):
                continue
            
            write_csv_log(file, "Creating Project Structure from CSV", reset = False)
                        
            self._generate_project_structure_from_csv(session, self.project, file)
        
            
    @staticmethod
    def _load_csv_files(csv_folder_path):
        
        csv_files = []
                
        if os.path.exists(csv_folder_path):
            for csv_file in os.listdir(csv_folder_path):      
                
                csv_file = os.path.join(csv_folder_path, csv_file)
                csv_files.append(csv_file)
        
        return csv_files

    @staticmethod
    def _generate_project_structure_from_csv(session, project, csv_file):
        
        import pandas as pd
        
        sheet = pd.read_csv(csv_file, sep = get_delimiter(csv_file))

        pastas = []
        for item in sheet['Pasta']:
            if not item in pastas:
                pastas.append(item)
        
        for pasta in pastas: 
            write_log(pasta)  
            
            if str(pasta) == "nan":  
                continue
              
            folder = CreateFolder(pasta, project, session).get_folder()
            
            if not folder:
                continue
            
            folder_task_dataframe = sheet.loc[sheet['Pasta'] == pasta] # Generating DataFrame with tasks inside the current folder only
            
            for row in folder_task_dataframe.itertuples():  
                
                task_dict = row.__dict__   
                
                if str(task_dict["Nome"]) == "nan":
                    continue
                 
                CreateTask(task_dict, folder, session)          
    
    @staticmethod                           
    def _clear_current_project_structure(session, project):
        
        project_folders = session.query('Folder where parent.id is ' + str(project['id']))
        
        if project_folders:
            for folder in project_folders:                                  
                session.delete(folder)
        
        write_log("Project Structure Cleared")
            
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
        
        if 'values' in event['data']:
            values = event['data']['values']
            
            try:    
    
                entity_type, entity_id = entities[0] 
                project = session.query('Project where id is "{0}"'.format(entity_id)).one()
                
                if project["id"] == "1993a0ce-c562-11ec-97b1-02a0d5fc5f47": # For Safety reasons, this action will execute only in this project
                    
                    CreateProjectStructure(session, project, values)
                    
                    return {
                    'success': True,
                    'message': 'Finished',
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
    
    def interface(self, session, entities, event):
        
        values = event['data'].get('values', {})
        
        if (not values or not (values.get('csv_paths'))):     
            return [
                {
                'type': 'textarea',
                'label': 'CSV Paths (Separate by ",")',
                'name': 'csv_paths',
                },
                {
                'type': 'boolean',
                'label': 'Clear Current Project Structure',
                'name': 'clear_project_structure',
                }
            ]

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

        