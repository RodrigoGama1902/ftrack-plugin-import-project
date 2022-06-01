
import sys
import os
import logging

import arrow
from datetime import datetime

import ftrack_api
from ftrack_action_handler.action import BaseAction

# Loading Dependencies
_cwd = os.path.dirname(__file__)
_sources_path = os.path.abspath(os.path.join(_cwd, '...', 'dependencies'))

if _sources_path not in sys.path:
    sys.path.append(_sources_path)
    
# Constants

LOG_RELATIVE_PATH = os.path.realpath(os.path.join(os.path.join(os.path.dirname(__file__),"..","logs", "import-project.log")))
                 
# Script Start

username_not_found = []  

class CreateTask:
    '''Create a new task inside a parent'''
        
    def __init__(self, task_dict, parent, session):
        
        self.logger = logging.getLogger("import-project")  
                                
        if session.query('select name from Task where name is "{}"'.format(task_dict["Nome"])).first():
            self.logger.warning("Task already exists")
            return
        
        self.logger.info("Creating Task: " + task_dict["Nome"])
                        
        task = session.create('Task', {
            
                'parent' : parent,
                
                'name': self._get_correct_task_attribute(task_dict["Nome"],"Name"),
                'description': self._get_correct_task_attribute(task_dict["Description"],""),
                'type': self._get_entity_type_by_name(session, "Type", task_dict["Type"], "MLTR"),
                'priority': self._get_entity_type_by_name(session, "Priority", task_dict["Priority"], "P1"),
                'scopes': [self._get_entity_type_by_name(session, "Scope", task_dict["Scope"], "None"),],
                'status': self._get_entity_type_by_name(session, "Status", task_dict["Status"], "Not Started"),
                
                # Bid Days
                'bid': self._get_bid_days(task_dict["Bid"]),                       
                
                # Arrow Time Objects
                'start_date': self._get_arrow_time(task_dict["Start Date"]), 
                'end_date': self._get_arrow_time(task_dict["Due Date"]),
                
                # Custom Attributes
                'custom_attributes': {
                    'SKU': self._get_correct_task_attribute(task_dict["SKU"],""),
                    'Height': self._get_correct_task_attribute(task_dict["Height"],""),
                    'Width': self._get_correct_task_attribute(task_dict["Width"],""),
                    'Depth': self._get_correct_task_attribute(task_dict["Depth"],""),
                    
                },
            })
        
        self._create_task_assignment(session, task_dict, task)
        self._create_task_link(session, task_dict, task)   
               
        self.task = task
        
        session.commit()
        
        
    def _create_task_link(self, session, task_dict, task):
        '''Create task incoming links'''
        
        incoming_task = str(task_dict["Incoming"])
        
        if incoming_task == "":
            return
        
        incoming_task_list = incoming_task.split(";")
        
        for incoming_task_name in incoming_task_list:  
            incoming_task_name = incoming_task_name.strip()
            
            self.logger.info("Creating Task Link: " + incoming_task_name)
            
            incoming_task = session.query('select name from Task where name is ' + incoming_task_name)
            
            if incoming_task:
                task_link = session.create('TypedContextLink', {
                        'from': incoming_task.one(),
                        'to' : task
                    })
    
    
    def get_task(self):
        '''Return task object'''
        return self.task
    
  
    def _get_arrow_time(self, string_data):
        '''Get arrow time to be used in the task due and end dates'''
        
        if str(string_data) == "":
            return ""
        
        if not string_data:
            return ""
        
        try:
            datetime_obj = datetime.strptime(string_data, "%m/%d/%Y")
            return arrow.get(datetime_obj)
        
        except Exception as e:        
            self.logger.error("Error", exc_info = True)   
                     
            return ""

    @staticmethod
    def _get_bid_days(bid):
        '''Get correct bid days'''
        
        if str(bid) == "":
            return 0

        if not bid:
            return 0
        
        return bid * 86400
        
    @staticmethod
    def _get_entity_type_by_name(session, entity_type, name, default_name):
        '''Get entity type by name, return a default one if not found'''
        
        entity = session.query(entity_type + " where name is '" + name + "'")
        
        if entity:
            return entity.one()
        else:
            return session.query(entity_type + " where name is '" + default_name + "'").one()
        
        
    def _create_task_assignment(self, session, task_dict, task):
        '''Create task assignments by user email'''
        
        global username_not_found
               
        assignee = str(task_dict["Assignee"])
                        
        if assignee == "":
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
                self.logger.warning("User not found {}".format(assignee_item))
 
    @staticmethod
    def _get_correct_task_attribute(value, default):
        '''Used to return a default value if the value is empty'''
        
        if str(value) == "":
            return default
        else:
            return value
        

class CreateFolder:
    '''Create a new folder inside a project root'''
    
    folder = None
    
    def __init__(self, name, project, session):
        
        self.logger = logging.getLogger("import-project")  
        
        if session.query('select name, project_id from Folder where name is "{folder_name}" and project_id is "{project_id}"'.format(folder_name = name, project_id = project["id"])).first():
            self.logger.warning("Folder already exists")
            return
        
        self.logger.info("Creating Service Folder: " + name)

        self.folder = session.create('Folder', {
                'name': name,
                'parent' : project
            })
        
    def get_folder(self):
        '''Return folder object'''
        return self.folder


class CreateProjectStructure:
    '''Create Project Structure from CSV'''
    
    project = None
    values = None
    
    def __init__(self, session, project, values):
        
        self.logger = logging.getLogger("import-project")   
        self.project = project
        self.values = values
        
        #if values["clear_project_structure"]:  # Not used for now
        #    self._clear_current_project_structure(session, project)
            
        self._load_csv_input(session)
        
        session.commit()
        
    def _load_csv_input(self, session):
        '''Read all csv paths and create the project structure based on the csv'''
                    
        csv_files = self.values["csv_paths"].split(",")
        csv_files = [path.strip() for path in csv_files]               
        
        for file in csv_files:
            if not os.path.exists(file):
                continue
            
            if not file.endswith(".csv"):
                continue
                                    
            self._generate_project_structure_from_csv(session, self.project, file)


    @staticmethod
    def _generate_project_structure_from_csv(session, project, csv_file):
        '''Read the current CSV, and create the project structure based on it'''
        
        from csv_helper.csv_to_dict import CSVToDict # Importing custom CSV Library
                        
        csv_data = CSVToDict(csv_file)  
        possible_values = csv_data.possible_values("Pasta")
        
        for pasta in possible_values:
            
            if str(pasta) == "":  
                continue
              
            folder = CreateFolder(pasta, project, session).get_folder()
            
            if not folder:
                continue
            
            folder_tasks = csv_data.loc("Pasta", pasta)
                        
            for row in folder_tasks:
                
                if str(row["Nome"]) == "":
                    continue
                
                CreateTask(row, folder, session) 
                                             
    def _clear_current_project_structure(self, session, project):
        '''Clear current project structure'''
        
        project_folders = session.query('select parent.id from Folder where parent.id is ' + str(project['id']))
        
        if project_folders:
            for folder in project_folders:                                  
                session.delete(folder)
        
        self.logger.info("Project Structure Cleared")
            
        session.commit()        


class CreateProjectStructureAction(BaseAction):
    '''Import Project Data Using CSV File'''

    identifier = 'import.csv.project'
    label = 'Create Project Structure'
    variant = None
    description = 'This action will create a project structure from CSV files.'
    icon = 'https://cdn-icons-png.flaticon.com/512/180/180855.png'
    
    # For safety reason, this action will run in these project IDs only
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
        
        logger = logging.getLogger("import-project")
        handler = logging.FileHandler(LOG_RELATIVE_PATH)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
            
        logger.info("############################## Creating New Project Structure ##############################")
        
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
                        'message': "Invalid Project, please add this project ID to the projects list, current project-id:  " + str(entity_id),
                    }
                
            except:
                
                logger.error("Error", exc_info=True)

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
                #{     # Not used for now
                #'type': 'boolean',
                #'label': 'Clear Current Project Structure',
                #'name': 'clear_project_structure',
                #},
                {
                'label': 'Log Path',
                'type': 'text',
                'name': 'log_path', 
                'value': LOG_RELATIVE_PATH,
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

        