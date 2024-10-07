from automation_functions_final import *

date_1 = '2023-07-10'
date_2 = '2023-07-17'
root = '/Users/sroger12/Library/CloudStorage/OneDrive-UniversityofVermont/criminal_justice_data/Weekly_Data'
target = '/Users/sroger12/Library/CloudStorage/OneDrive-UniversityofVermont/criminal_justice_data/Validations'

make_subdirectory(root,date_1,date_2)
run_program(date_1,date_2,root,target)