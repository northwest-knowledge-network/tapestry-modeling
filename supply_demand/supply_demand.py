import pandas as pd
import numpy as np 
import os

file_path = './supp_dem_data/'

# read in the test data
input_df = pd.read_csv(file_path + 'InputData.csv', header=1)
input_df.set_index('Num', inplace=True)
input_df["Region1"] = [float(str(i).replace(",", "")) for i in input_df["Region1"]]
emp_df = pd.read_csv(file_path + 'NationalEmp.csv')
emp_df.set_index('Num', inplace=True)
supp_fun_df = pd.read_csv(file_path + 'SupplyFunctions.csv')
supp_fun_df = supp_fun_df[~supp_fun_df['Industry Description'].isin([np.nan, 'SUM'])].reset_index(drop=True)
prod_fun_df = pd.read_csv(file_path + 'ProductionFunctions.csv')
make_fun_df = pd.read_csv(file_path + 'MakeFunctions.csv', header=1)

# calculate regional calcs for the yellow region
regional_calc_df = pd.merge(input_df, emp_df[emp_df['Color']=='yellow'], left_index=True, right_index=True)
regional_calc_df['Output'] = regional_calc_df['Region1'].astype(float) * regional_calc_df['Out/Emp'].astype(float)

# copy the original dataframe
supp_fun_mat = supp_fun_df.copy()

# industry description needs to be removed to convert df to matrix
supp_fun_mat.drop(columns=['Industry Description'])

# remove the column names from the matrix 
supp_mat = supp_fun_mat.values[:, 1:]

input_list = input_df['Region1'].tolist()
emp_list2 = np.array(emp_df[emp_df['Color']=='green']['Out/Emp'].tolist())

mat_prod = np.matmul(supp_mat, input_list)

regional_calcs_green = np.multiply(mat_prod, emp_list2)