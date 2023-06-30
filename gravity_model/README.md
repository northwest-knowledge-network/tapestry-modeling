# Gravity Model

This calculates trade between regions for a single given commodity
using a fully constrained gravity model
 
Commodity supply and demand for each region as well as a distances between
regions and impedence factor (weights on distance) are the required inputs
 
Gravity model Based on Wilson, A.G. (1967), A Statistical Theory of
Spatial Distribution Models. Transportation Research 1 (3), pp. 252-270.

Below are the steps for running the gravity model:

1. The *run_model.py* script is run from the command line, accepting two arguments. These include the impedance variable of interest, and optionally a trade commodity id. To see a list of the arguments and the values that are accepted, run the following command from the command line:

```python run_conversion.py -h```

2. Once you have decided upon the arguments, run the following command from the command line:

```python run_model.py -impedance <impedance_variable> -commodity <commodity_id>```

**Currently, the program writes the output to a csv file. In order to run properly, the user must manually create a folder called "output" in the same directory as the script. The program will write the output to this folder.** 