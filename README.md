# CarCrash_CaseStudy

This Project is to analysis the vehicle crash data

## Directory Structure

- `analysis/`: this folder consits the python scripts which analysis vehicle crash data. Each analysis_\<n>.py script performs a specific type of analysis.
- `config.json`: this is the configuration file storing settings and parameters used by publish.py.
- `data/`: this folder contains the input data and input reader script.
- `docs/`: contain all the documents related to this project, including a data dictionary and case study questions.
- `publish/`: contains scripts for publishing the analysis results.
- `README.md`: this file provides the overview of the project and instructions.
- `requirements.txt`: this file conatins python package dependencies.
- `spark-submit.sh`: shell script for submitting Spark jobs
  
## Input Data

The input data is located in data/input_data/ and includes the following files:

- Charges_use.csv
- Damages_use.csv
- Endorse_use.csv
- Primary_Person_use.csv
- Restrict_use.csv
- Units_use.csv


## Documentation

For detailed information on the dataset and analysis questions, refer data_dict or case_study from docs/ directory.


## Publishing Results

To publish the results of your analysis, run the publish.py script located in the publish/ directory.
