import numpy as np
import pandas as pd
from nyisotoolkit import NYISOData
import pickle
import logging
from os import path

_logger = logging.getLogger(__name__)

CURVES_FILEPATH = path.join(path.dirname(__file__), 'database/co2_curves{year}.pickle')
DEFAULT_YEAR = 2019


def calc_grid_co2_avg(dataframe: pd.DataFrame, year=DEFAULT_YEAR):
    """
    :param dataframe: A DataFrame containing fuel mix data.
    :param year: The basis year. There must be a corresponding curve package - see README / Generating CO2 curves.
    :return: A DataFrame with CO2 intensity in kg/MWh.
    """
    df = dataframe.copy().rename_axis('', axis='columns')
    with open(CURVES_FILEPATH.format(year=year), 'rb') as infile:
        data = infile.read()
    if not data:
        _logger.error('CO2 curves package {file} not found'.format(file=CURVES_FILEPATH))
    else:
        co2_curves = pickle.loads(data)
        df['total_mwh'] = 0.0
        df['co2_rate'] = 0.0
        for c in df.columns:
            if c != 'co2_rate' and c != 'total_mwh':
                df['total_mwh'] += df[c].apply(lambda x: x / 12)
                df['co2_rate'] += df[c].apply(lambda x: x / 12 * np.interp(x,
                                                                           co2_curves[c]['cum_cap'],
                                                                           co2_curves[c]['co2_rate']))
        df['co2_rate'] = df['co2_rate'] / df['total_mwh']
        return df[['co2_rate']]


def generate_curves(year: int, egrid_file: str):
    """
    Reads the eGRID data published by the EPA in Excel format and NYISO historical fuel mix data for the specified year
    and generates for each fuel type an approximate curve of carbon intensity vs. cumulative capacity dispatched.

        - year: The basis year for the calculation. Should correspond to the eGRID reporting year.

        - egrid_file: Filepath to the EPA eGRID data file (metric) (https://www.epa.gov/egrid/download-data).
    """

    egrid_cols = [  # First item will be set as DataFrame index
        'ORISPL',  # Plant code
        'CAPFAC',  # Capacity Factor
        'NAMEPCAP',  # Nameplate capacity (MW)
        'PLFUELCT',  # Primary fuel code
        'PLC2ERTA'  # Equivalent CO2 emissions (kg/MWh)
    ]

    fuel_epa_nyiso_map = {  # Map of eGRID primary fuel codes to the NYISO reported fuel categories
        'BIOMASS': 'Other Renewables',
        'COAL': 'Other Fossil Fuels',
        'GAS': 'Natural Gas',
        'HYDRO': 'Hydro',
        'NUCLEAR': 'Nuclear',
        'OIL': 'Other Fossil Fuels',
        'OTHF': 'Other Fossil Fuels',
        'SOLAR': 'Other Renewables',
        'WIND': 'Wind',
        'OFSL': 'Other Fossil Fuels'
    }

    fmix_hist_df = NYISOData(dataset='fuel_mix_5m', year=str(year)).df
    # Dual fuel plant runs almost exclusively on gas so is rolled up with Natural Gas.
    fmix_hist_df['Natural Gas'] = fmix_hist_df['Natural Gas'] + fmix_hist_df['Dual Fuel']
    fmix_hist_df = fmix_hist_df.drop(['Dual Fuel'], axis=1)

    # Create a dictionary with fuel type as key and the max. MW generation for that fuel in the year
    # This will be used later to normalize cumulative capacity
    nyiso_fmix_max = {}
    for v in fmix_hist_df.columns.values:
        nyiso_fmix_max[v] = fmix_hist_df[v].max()

    # Open EPA eGRID metric data file, import desired columns from the plant data tab, filter to NYISO only, set Plant
    # Code as index and map NYISO fuel categories to the EPA fuel code.
    egrid_plnt_df = pd.read_excel(egrid_file.format(year=str(year)),
                                  sheet_name='PLNT{year}'.format(year=str(year)[-2:]),
                                  skiprows=[0])
    egrid_plnt_df = egrid_plnt_df[egrid_plnt_df['BACODE'] == 'NYIS'][egrid_cols]
    egrid_plnt_df = egrid_plnt_df.set_index(egrid_cols[0])
    egrid_plnt_df = egrid_plnt_df.replace({'PLFUELCT': fuel_epa_nyiso_map})

    # Construct a set of curves for each fuel type describing the relationship with total capacity dispatched in that
    # category and average equivalent CO2 emissions intensity.

    co2_curves = dict()
    for v in nyiso_fmix_max.keys():
        co2_curves[v] = egrid_plnt_df[egrid_plnt_df['PLFUELCT'] == v]
        co2_curves[v] = co2_curves[v].sort_values('CAPFAC', axis=0, ascending=False)
        co2_curves[v]['cum_cap'] = co2_curves[v]['NAMEPCAP'].cumsum(axis=0, skipna=True)
        co2_curves[v] = co2_curves[v].fillna(0)
        co2_curves[v].drop(co2_curves[v][co2_curves[v]['CAPFAC'] == 0].index, inplace=True)
        co2_hr_rate = co2_curves[v]['PLC2ERTA'] * co2_curves[v]['NAMEPCAP']
        co2_curves[v]['cum_co2'] = co2_hr_rate.cumsum(axis=0, skipna=True)
        co2_curves[v]['co2_rate'] = co2_curves[v]['cum_co2'] / co2_curves[v]['cum_cap']
        co2_curves[v]['cum_cap'] = co2_curves[v]['cum_cap'] * nyiso_fmix_max[v] / co2_curves[v]['cum_cap'].max()
        co2_curves[v] = co2_curves[v][['cum_cap', 'co2_rate']]

    with open(CURVES_FILEPATH.format(year=str(year)), 'wb') as outfile:
        pickle.dump(co2_curves, outfile)

    # Check calibration against EPA published data for NYISO
    egrid_st_df = pd.read_excel(egrid_file.format(year=str(year)),
                                sheet_name='BA{year}'.format(year=str(year)[-2:]),
                                skiprows=[0])
    accuracy = co2_historical_total(year) / egrid_st_df[egrid_st_df['BACODE'] == 'NYIS']['BAC2ERTA']
    print('CO2 curves written for {yr}. Average CO2 intensity estimated at {acc:.1f}% of EPA reported value.'
          .format(yr=year, acc=float(accuracy)*100))
    return float(accuracy)


def co2_historical_total(year):
    df_mix = NYISOData(dataset='fuel_mix_5m', year=str(year)).df
    df_mix['Natural Gas'] = df_mix['Dual Fuel'] + df_mix['Natural Gas']  # Dual fuel plant almost exclusively burns gas
    df_mix = df_mix.drop(['Dual Fuel'], axis=1)
    df_co2 = calc_grid_co2_avg(df_mix, year=year)
    df_mix['total_mwh'] = df_mix.loc[:].sum(axis=1)
    df_co2 = pd.concat([df_co2, df_mix[['total_mwh']]], axis=1)
    total = np.sum(df_co2['total_mwh'] * df_co2['co2_rate']) / np.sum(df_co2['total_mwh'])
    return total
