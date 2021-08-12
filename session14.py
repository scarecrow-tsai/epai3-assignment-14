import csv
from datetime import datetime
from collections import namedtuple

personal_info = namedtuple(
    "personal_info", ["ssn", "first_name", "last_name", "gender", "language"]
)
vehicle_info = namedtuple(
    "vehicle_info", ["ssn", "vehicle_make", "vehicle_model", "model_year"]
)
employment_info = namedtuple("employment_info", ["employer", "department", "employee_id", "ssn"])
update_info = namedtuple("update_info", ["ssn", "last_updated", "created"])



def read_file(file_name: str):
    """
    Read csv files.
    """
    with open(file_name) as f:
        rows = csv.reader(f, delimiter=',', quotechar='"')
        yield from rows
        
def create_generator_personal_info(file_name:str):
    """
    Create generator for namedtuple.
    """
    data = read_file(file_name)
    next(data)
    for row in data:
        yield personal_info(*row)    
        
def create_generator_vehicle_info(file_name: str):
    """
    Create generator for namedtuple.
    """
    data = read_file(file_name)
    next(data)
    for row in data:
        yield vehicle_info(row[0], row[1], row[2], int(row[3]))
        
def create_generator_employee_info(file_name:str):
    """
    Create generator for namedtuple.
    """
    data = read_file(file_name)
    next(data)
    for row in data:
        yield employment_info(*row)
        
def create_generator_update_info(file_name:str):
    """
    Create generator for namedtuple.
    """
    data = read_file(file_name)
    next(data)
    for row in data:
        yield update_info(
            row[0],
            datetime.strptime(
                datetime.fromisoformat(row[1].replace("Z", "+00:00")).strftime("%d/%m/%Y"),
                "%d/%m/%Y",
            ),
            created = datetime.strptime(
            datetime.fromisoformat(row[2].replace("Z", "+00:00")).strftime("%d/%m/%Y"),
            "%d/%m/%Y",
        )
        
        )
        
        

def merge_nt(*ntuples):
    """
    Merge namedtuples using dict.
    """
    combined_dict = {}
    for nt in ntuples:
            combined_dict.update(nt._asdict())
    yield namedtuple("combined_info", combined_dict.keys())(*combined_dict.values())
    
def create_generator_combined_info(generator_tuple):
    """
    Create a generator for a combined named tuple.
    """
    for nts in zip(*generator_tuple):
        yield from merge_nt(*nts)
        
        
        
        
def get_popular_car(gen):
    """
    Return most occuring car in the input generator.
    """
    count_dict = {}

    for c in gen:
        if c not in count_dict:
            count_dict[c] = 0
        else:
            count_dict[c] += 1
            
    return max(count_dict.items(), key = lambda k : k[1])