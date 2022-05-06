from distutils.fancy_getopt import wrap_text
import sys
import os
import csv

import arrow
from datetime import datetime
from dateutil import tz

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

                
# Loading Dependencies

load_dependencies_path()

# Script Start

username_not_found = []

class WriteLog:
    '''This class will create both global logs and csv logs'''
        
    def __init__(self, message = "Starting Log"):
        
        self.action_log = os.path.realpath(os.path.join(os.path.join(os.path.dirname(__file__),"..","logs", "action_log.log")))
            
    def write(self, message = None):
            
        message = str(message)
        now = datetime.now()
            
        with open(self.action_log, "a") as log_file:      
            log_file.write(now.strftime("%d/%m/%Y %H:%M:%S") + " " + message)
            log_file.write("\n")
    
    def add_header(self, message = None):
            
        message = str(message)
        now = datetime.now()
            
        with open(self.action_log, "a") as log_file:    
            log_file.write("")  
            log_file.write(now.strftime("%d/%m/%Y %H:%M:%S") + " " + "############################ " + message.upper() + " ############################")
            log_file.write("")
            log_file.write("\n")
    
    def get_path(self):
        return self.action_log
    
log = WriteLog()    

class CreateTask:
    '''Create a new task inside a parent'''
        
    def __init__(self, task_dict, parent, session):
                                
        if session.query('select name from Task where name is "{}"'.format(task_dict["Nome"])).first():
            log.write("Task already exists")
            return
        
        log.write("Creating Task: " + task_dict["Nome"])
                
        task = session.create('Task', {
            
                'parent' : parent,
                
                'name': self._get_correct_task_attribute(task_dict["Nome"],"Name"),
                'description': self._get_correct_task_attribute(task_dict["Description"],""),
                'type': self._get_entity_by_name(session, "Type", task_dict["Type"], "Production"),
                'priority': self._get_entity_by_name(session, "Priority", task_dict["Priority"], "P1"),
                'scopes': [self._get_entity_by_name(session, "Scope", task_dict["Scope"], "None"),],
                'status': self._get_entity_by_name(session, "Status", task_dict["Status"], "Not Started"),
                
                # Bid Days
                'bid': self._get_bid_days(task_dict["Bid"]),                       
                
                # Arrow Time Objects
                'start_date': self._get_arrow_time(task_dict["_5"]), # For some reason, the start date is begin read as _5
                'end_date': self._get_arrow_time(task_dict["_6"]),
                
                # Custom Attributes
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
        
        incoming_task = str(task_dict["Incoming"])
        
        if incoming_task == "nan":
            return
        
        incoming_task_list = incoming_task.split(";")
        
        for incoming_task_name in incoming_task_list:  
            incoming_task_name = incoming_task_name.strip()
            
            log.write("Creating Task Link: " + incoming_task_name)
            
            incoming_task = session.query('select name from Task where name is ' + incoming_task_name)
            
            if incoming_task:
                task_link = session.create('TypedContextLink', {
                        'from': incoming_task.one(),
                        'to' : task
                    })
    
    @staticmethod
    def _get_arrow_time(string_data):
        
        if str(string_data) == "nan":
            return ""
        
        if not string_data:
            return ""
        
        try:
            datetime_obj = datetime.strptime(string_data, "%m/%d/%Y")
            return arrow.get(datetime_obj)
        
        except Exception as e:
            
            log.write("Error converting date: " + string_data)
            log.write(e)
            
            return ""

    @staticmethod
    def _get_bid_days(bid):
        
        if str(bid) == "nan":
            return 0

        if not bid:
            return 0
        
        return bid * 86400
        
    @staticmethod
    def _get_entity_by_name(session, entity_type, name, default_name):
        
        entity = session.query(entity_type + " where name is '" + name + "'")
        
        if entity:
            return entity.one()
        else:
            return session.query(entity_type + " where name is '" + default_name + "'").one()
        
    @staticmethod
    def _task_assigment(session, task_dict, task):
        
        global username_not_found
               
        assignee = str(task_dict["Assignee"])
                        
        if assignee == "nan":
            return
        
        assignee_list = assignee.split(";")
        
        for assignee_item in assignee_list:  
            
            if assignee_item in username_not_found:
                return
                    
            assignee_item = assignee_item.strip()
                        
            user = session.query('select username from User where username is "' + assignee_item + '"')
            
            if user:
                session.create('Appointment', {
                    'context': task,
                    'resource': user.one(),
                    'type': 'assignment'
                })
                
            else:
                username_not_found.append(assignee_item)
                log.write("User not found {}".format(assignee_item))
 
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
        
        if session.query('select name, project_id from Folder where name is "{folder_name}" and project_id is "{project_id}"'.format(folder_name = name, project_id = project["id"])).first():
            log.write("Folder already exists")
            return
        
        log.write("Creating Service Folder: " + name)

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
            
            #write_csv_log(file, "Creating Project Structure from CSV", reset = False)
                        
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
            
            if str(pasta) == "nan":  
                continue
              
            folder = CreateFolder(pasta, project, session).get_folder()
            
            if not folder:
                continue
            
            folder_task_dataframe = sheet.loc[sheet['Pasta'] == pasta] # Generating DataFrame with tasks inside the current folder only
            
            # Development Only
            
            max_rows = 0 # Prevent that creates more than the necessary tasks when in developer mode, set 0 to disable
            
            for idx, row in enumerate(folder_task_dataframe.itertuples()):  
                
                if max_rows:
                    if idx == max_rows:
                        log.write("Task Creation Was Limited for {} Tasks".format(max_rows))
                        break
                
                task_dict = row.__dict__   
                
                if str(task_dict["Nome"]) == "nan":
                    continue
                                     
                CreateTask(task_dict, folder, session)          
    
    @staticmethod                           
    def _clear_current_project_structure(session, project):
        
        project_folders = session.query('select parent.id from Folder where parent.id is ' + str(project['id']))
        
        if project_folders:
            for folder in project_folders:                                  
                session.delete(folder)
        
        log.write("Project Structure Cleared")
            
        session.commit()        


class CreateProjectStructureAction(BaseAction):
    '''Import Project Data Using CSV File'''

    identifier = 'import.csv.project'
    label = 'Create Project Structure'
    variant = None
    description = 'This action will create a project structure from CSV files.'
    icon = 'https://cdn-icons-png.flaticon.com/512/180/180855.png'
    
    test_projects = ["a0467cac-cb4a-11ec-940f-02a0d5fc5f47",
                     "1993a0ce-c562-11ec-97b1-02a0d5fc5f47"
                     ]
    
    def discover(self, session, entities, event):
                
        if len(entities) != 1:
            return

        entity_type, entity_id = entities[0]
        if entity_type != 'Project':
            return

        return True


    def launch(self, session, entities, event):
        
        log.add_header("Creating New Project Structure")
        
        if 'values' in event['data']:
            values = event['data']['values']
            
            try:   
                
                global username_not_found
                username_not_found = []
    
                entity_type, entity_id = entities[0] 
                project = session.query('Project where id is "{0}"'.format(entity_id)).one()
                
                if project["id"] in self.test_projects: # For Safety reasons, this action will execute only in this project
                    
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
                
                log.write(error)
                
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
                },
                {
                'label': 'Log Path',
                'type': 'text',
                'value': log.get_path(),
                'name': 'log_path'
                },
            ]    


    def register(self):
              
        '''Register action.'''
        self.session.event_hub.subscribe(
            'topic=ftrack.action.discover and source.user.username={0}'.format(
                self.session.api_user
            ),
            self._discover
        )

        self.session.event_hub.subscribe(
            'topic=ftrack.action.launch and data.actionIdentifier={0} and '
            'source.user.username={1}'.format(
                self.identifier,
                self.session.api_user
            ),
            self._launch
        )
    
    
def register(session, **kw):

    if not isinstance(session, ftrack_api.session.Session):
        return

    action_handler = CreateProjectStructureAction(session)
    action_handler.register()

if __name__ == '__main__':
                                
    session = ftrack_api.Session()
    register(session)

    # Wait for events
    session.event_hub.wait()

        