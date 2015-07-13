import unittest
import os

import pandas as pd

from redistrict import redistrict, BASE_DIR


class RedistrictTest(unittest.TestCase):
    def test(self):
        path = os.path.join(BASE_DIR, 'data', 'de', 'kreise_test.csv')
        df = pd.read_csv(path)
        df.AGS = df.AGS.apply(lambda x: '{:0>5d}'.format(x))
        df = df.set_index('AGS')
        df = df[['value1', 'value2']]
        self.assertEqual(df.ix['15082'].sum(), 27)
        print(df)
        new_df = redistrict(df[['value1', 'value2']], 'de/kreise',
                           drop=True,
                           splits=True)
        print(new_df)
        self.assertEqual(new_df.ix['15082'].sum(), 74.25)
