import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from models import QCEWComp, NAICStoBEA

class QCEWtoBEA:
    def __init__(self, qcew_data, ownership_code):
        self.qcew_data = qcew_data
        self.ownership_code = ownership_code

    def bea_conversion(self):
        bridge_cols = [
            NAICStoBEA.r_seq, 
            NAICStoBEA.c_seq, 
            NAICStoBEA.r_naics6_code, 
            NAICStoBEA.c_bea_code_2_lvl_6, 
            NAICStoBEA.naics_to_io_coeffs,
            NAICStoBEA.bls_own_code
        ]
        if self.ownership_code == 0:
            out_data = pd.DataFrame()
            bridge_data = db.session.query(*bridge_cols).all()
            bridge_df = pd.DataFrame.from_records(bridge_data, columns=['r_seq', 'c_seq', 'r_naics6_code', 'c_bea_code_2_lvl_6', 'naics_to_io_coeffs', 'own_code'])
            for code in bridge_df.own_code.unique():
                bridge_sub = bridge_df[bridge_df.own_code == code]
                csv_data_sub = csv_data[csv_data.own_code == code]
                csv_out_sub = bea_bridge(req_data, wage_info, emp_info, bridge_sub, csv_data_sub)
                out_data = pd.concat([out_data, csv_out_sub])
            out_data = out_data.drop(['own_code'], axis=1)
            out_data.insert(2, "own_code", 0, allow_duplicates=True)
            out_data = out_data.groupby(['bea_num', 'bea_name', 'bea_code', 'own_code', 'year'])[["tap_estabs_count_bea", wage_info[1] + '_bea', emp_info[1] + '_bea']].sum().reset_index()
            csv_data = out_data.copy()
        else:
            bridge_data = db.session.query(*bridge_cols).filter(NAICStoBEA.bls_own_code == own_code).all()
            bridge_df = pd.DataFrame.from_records(bridge_data, columns=['r_seq', 'c_seq', 'r_naics6_code', 'c_bea_code_2_lvl_6', 'naics_to_io_coeffs', 'oen_code'])
            csv_data = bea_bridge(req_data, wage_info, emp_info, bridge_df, csv_data)
        self.csv_data = csv_data

    def bea_bridge(req_data, wage_info, emp_info, bridge_df, csv_data):
        bea = pd.read_csv('./app/static/files/bea_cols.csv', dtype=str)
        naics = pd.read_csv('./app/static/files/naics_rows.csv', dtype=str)
        if naics['naics >>'].isnull().values.any():
            naics = naics.dropna(subset=['naics >>'])
        naics['naics >>'] = naics['naics >>'].astype(int)
        if bea['column_index'].isnull().values.any():
            bea = bea.dropna(subset=['column_index'])
        bea['column_index'] = bea['column_index'].astype(int)
        sectors = pd.read_csv('./app/static/files/BEA_sectors.csv')

        csv_data = csv_data.merge(naics, left_on='naics_code', right_on='naics6_code')
        csv_data = csv_data.merge(bridge_df, left_on='naics >>', right_on='r_seq', suffixes=('', '_y'))
        csv_data = csv_data.merge(bea, left_on='c_seq', right_on='column_index')
        csv_data['own_code'] = csv_data['own_code'].astype(int)
        csv_data['tap_estabs_count'] = csv_data['tap_estabs_count'].astype(int)
        csv_data[wage_info[1]] = csv_data[wage_info[1]].astype(int)
        csv_data[emp_info[1]] = csv_data[emp_info[1]].astype(int)
        csv_data['naics_to_io_coeffs'] = csv_data['naics_to_io_coeffs'].astype(float)

        csv_data[wage_info[1] + '_bea'] = np.array(csv_data[wage_info[1]].tolist()) * np.array(csv_data['naics_to_io_coeffs'].tolist())
        csv_data[emp_info[1] + '_bea'] = np.array(csv_data[emp_info[1]].tolist()) * np.array(csv_data['naics_to_io_coeffs'].tolist())
        csv_data['tap_estabs_count_bea'] = np.array(csv_data['tap_estabs_count'].tolist()) * np.array(csv_data['naics_to_io_coeffs'].tolist())
        csv_data = csv_data.drop(['naics >>', 'column_index', 'r_seq', 'c_seq', 'naics_to_io_coeffs', wage_info[1], emp_info[1], 'tap_estabs_count', 'naics_code', 'naics6_code'], axis=1)
        if req_data.get('fips_calc') == 'fipsAgg':
            csv_data = csv_data.groupby(['bea_code', 'own_code', 'year'])[["tap_estabs_count_bea", wage_info[1] + '_bea', emp_info[1] + '_bea']].sum().reset_index()
        else:
            csv_data = csv_data.groupby(['area_fips', 'bea_code', 'own_code', 'year'])[["tap_estabs_count_bea", wage_info[1] + '_bea', emp_info[1] + '_bea']].sum().reset_index()
        csv_data['bea_num'] = [x.split('_')[1] for x in csv_data['bea_code'].tolist()]
        csv_data = csv_data.merge(sectors, left_on="bea_num", right_on="Num")
        csv_data.pop('Num')
        csv_data.insert(0, 'bea_num', csv_data.pop('bea_num'))
        csv_data.insert(1, 'bea_name', csv_data.pop('Name'))
        csv_data.insert(2, 'bea_code', csv_data.pop('bea_code'))
        return csv_data
