import unittest
import os

import pandas as pd

from redistrict import redistrict, redistrict_grouped, BASE_DIR


class RedistrictTest(unittest.TestCase):
    def test_redistrict(self):
        path = os.path.join(BASE_DIR, 'data', 'de', 'kreise_test.csv')
        df = pd.read_csv(path)
        df.AGS = df.AGS.apply(lambda x: '{:0>5d}'.format(x))
        df = df.set_index('AGS')
        df = df[['value1', 'value2']]
        self.assertEqual(df.ix['15082'].sum(), 27)
        new_df = redistrict(df[['value1', 'value2']], 'de/kreise',
                            drop=True, splits=True)
        self.assertEqual(new_df.ix['15082'].sum(), 74.25)

    def test_redistrict_grouped(self):
        path = os.path.join(BASE_DIR, 'data', 'de', 'kreise_test.csv')
        df = pd.read_csv(path)
        df.AGS = df.AGS.apply(lambda x: '{:0>5d}'.format(x))
        df = df.drop(['name'], axis=1)
        print df
        new_df = redistrict_grouped(df, 'de/kreise', ['year'],
                                    district_col='AGS',
                                    value_cols=['value1', 'value2'],
                                    drop=True)
        print new_df
        kreis = new_df.AGS == '15082'
        self.assertEqual(round(new_df[kreis & (new_df.year == 2005)].value1.sum(), 2), 22.44)
        self.assertEqual(new_df[kreis & (new_df.year == 2005)].value2.sum(), 24.81)
        self.assertEqual(new_df[kreis & (new_df.year == 2013)].value1.sum(), 13)
        self.assertEqual(new_df[kreis & (new_df.year == 2013)].value2.sum(), 14)

    def test_redistrict_population(self):
        path = os.path.join(BASE_DIR, 'data', 'de', '12411-0014.csv')
        population_orig = pd.read_csv(path, skiprows=5, skipfooter=5,
                                      delimiter=';', encoding='latin-1',
                                      engine='python')
        population_orig.rename(columns={
            'Unnamed: 0': 'AGS',
            'Unnamed: 1': 'Kreisname'
        }, inplace=True)
        years = range(1996, 2015)
        year_renames = {'31.12.%d' % (year - 1): year for year in years}
        years = [2011, 2012, 2013]
        population_orig.rename(columns=year_renames, inplace=True)

        format_ags = lambda x: '{:0>5d}'.format(x)
        population_orig['AGS'] = population_orig['AGS'].apply(format_ags)
        population_orig[years] = population_orig[years].convert_objects(convert_numeric=True)
        population_orig = population_orig.set_index('AGS')
        population = redistrict(population_orig[years], 'de/kreise', drop=True)
        population = pd.merge(population, population_orig[['Kreisname']],
                              how='left', right_index=True, left_index=True)
        manual_calc = population_orig.ix[["13001", "13059", "13062"]][2011].sum() + population_orig.ix[['13052']][2011].sum() * 0.18
        result = population.ix['13075'][2011]
        self.assertEqual(manual_calc, result)
