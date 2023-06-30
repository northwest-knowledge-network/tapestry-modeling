# NAICS to BEA conversion

By default, QCEW data is reported using 6 digit NAICS codes. However, our program relies on wage and employment data reported according to BEA sectors. Therefore, the program relies on carefully constructed bridge tables that translate the NAICS data into BEA sectors.

The data conversion program relies on two scripts: *conversion.py* and *run_conversion.py*. The first script contains the logic for performing the data conversion, and the first script fetches the QCEW data, performs multiple data formatting steps, and finally runs the conversion.

Below are the steps for running the conversion:

1. The run program script is run from the command line, accepting three arguments. These include the year, the commodity ownership code, and the employer wage range. To see a list of the arguments and the values that are accepted, run the following command from the command line:

```python run_conversion.py -h```

2. Once you have decided upon the arguments, run the following command from the command line:

```python run_conversion.py -year <year> -ownership <commodity_ownership_code> -employer_wage <wage_range>```