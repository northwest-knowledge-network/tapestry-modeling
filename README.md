# Economic Modeling: Greg Alward & Phil Watson

This program fetches an impedance matrix and supply/demand data from a postgres database and runs a gravity model using these inputs. The program is setup to handle different impedance variables that are found in the db, and allows users to input the commodity id of interest.

## To run the program:
- Clone the repository
- Create a python virtual environment (`python -m venv venv/`)
- Activate the virtual environment (`source venv/bin/activate`)
- Install the requirements.txt file (`pip install -r requirements.txt`)
- Create a .env file in the root directory with the following variables:
    - DB_HOST
    - DB_NAME
    - DB_USER
    - DB_PASSWORD
    - DB_PORT
- From the command line, run python run_model.py