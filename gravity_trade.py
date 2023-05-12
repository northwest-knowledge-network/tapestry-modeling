# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 15:07:27 2023
 
@author: Philip Watson: pwatson@uidaho.edu; code re-factored by Tanner Varrelman tvarrelman@uidaho.edu

This calculates trade between regions for a single given commodity
using a fully constrained gravity model
 
Commodity supply and demand for each region as well as a distances between
regions and impedence factor (weights on distance) are the required inputs
 
Gravity model Based on Wilson, A.G. (1967), A Statistical Theory of
Spatial Distribution Models. Transportation Research 1 (3), pp. 252-270
"""
 
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy import integrate


class GravityModel:
    def __init__(self, dist, comm_sup_dem):
        self.dist = dist
        self.comm_sup_dem = comm_sup_dem

    def format_data(self):
        """Reads the input data from the specified files."""
        self.sup = self.comm_sup_dem['supply_amount']
        self.dem = self.comm_sup_dem['demand_amount']

        self.tot_sup = sum(self.sup)
        self.tot_dem = sum(self.dem)

    def calculate_cost_matrix(self):
        """Calculates the cost matrix."""
        self.alpha = 1
        self.beta = -1.1
        self.gamma = 0

        self.cost_mat = self.alpha * np.power(self.dist, self.beta) * np.exp(self.gamma * self.dist)

    def calculate_attraction_matrices(self):
        """Calculates the attraction matrices."""
        self.s_d = np.diag(self.sup)
        self.d_d = np.diag(self.dem)

        self.att_org = np.matmul(self.s_d, self.cost_mat)
        self.att_des = np.matmul(self.cost_mat, self.d_d)

    def calculate_A_and_B(self):
        """Calculates A and B."""
        def calculate_MS_MD_A_B(MS, MD, A, B, iteration):
            if iteration == 100:
                return MS, MD, A, B,
            else:
                MS_new = self.att_org * A[:, np.newaxis]
                MD_new = self.att_des * B
                A_new = np.reciprocal(np.sum(MD, axis=1), where=np.sum(MD, axis=1) != 0)
                B_new = np.reciprocal(np.sum(MS, axis=0), where=np.sum(MS, axis=0) != 0)
                return calculate_MS_MD_A_B(MS_new, MD_new, A_new, B_new, iteration + 1)

        self.initial_MS = self.att_org
        self.initial_B = 1 / (np.sum(self.att_org, axis=0))
        self.initial_MD = self.initial_B * self.att_des

        self.initial_A = 1
        self.initial_A = np.array([1.0])

        self.final_MS, self.final_MD, self.final_A, self.final_B = calculate_MS_MD_A_B(
            self.initial_MS,
            self.initial_MS,
            self.initial_A,
            self.initial_B,
            0,
        )

        self.A = self.final_A
        self.B = self.final_B

    def calculate_prob_matrix(self):
        """Calculates the probability matrix."""
        self.A_diag = np.diag(self.A)
        self.B_diag = np.diag(self.B)

        self.int_prob = np.matmul(self.A_diag, self.att_des)
        self.prob = np.matmul(self.int_prob, self.B_diag)

    def calculate_shipping_matrix(self):
        """Calculates the shipping matrix."""
        self.S = np.matmul(self.s_d, self.prob)
        self.S_rowsum = np.sum(self.S, axis=1)
        self.S_colsum = np.sum(self.S, axis=0)

        self.total_shipped_supply = sum(self.S_rowsum)
        self.total_shipped_demand = sum(self.S_colsum)

        self.S_orig = self.S

        # Set any values less than this value to zero
        #self
