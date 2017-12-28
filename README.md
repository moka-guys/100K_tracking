# 100K_tracking
This repo holds two scripts used to read the API and import samples into Moka to be tracked.
These scripts are designed to be run periodically

## import_API_to_moka.py
This script is called by Moka.

1. Performs a query to extract all the probands currently in the database
2. Calls a script (return_all_API_cases.py) which runs on the server, passing the list of probands in moka as a argument
3. This script returns a list of values which can be used to create insert commands which are executed, importing samples into the tracking database, denoting any negative negative samples

## return_all_API_cases.py
This script runs on the server. 
1. The script reads the API, identifies any samples which:
    - Are not already in Moka 
    - Have been processed by Omicia and returned to the GMC
2. These samples are assessed to see if any tier1,2 or CIP candidate variants are present.
3. The program is determined by the site assigned to the sample (GSTT = pilot)
4. A string used to insert to Moka is returned for each sample containing:
    - Proband ID
    - Interpretation request ID (eg 123-1)
    - Database key denoting Pilot/Main Program
    - Database key denoting the default status (awaiting analysis)
    - A timestamp for the date added
    - A boolean flag denoting if the case is a negative negative (true denotes it is a negative negative, false if any variants are present)
    
