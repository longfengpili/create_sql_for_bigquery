import unittest
import pandas as pd
from parse_table import create_for_bigquery
import re

class TestDict(unittest.TestCase):

    def setUp(self):
        print('setUp...')

    def tearDown(self):
        print('tearDown...')

    def test_table_column(self):
        c = create_for_bigquery()
        df = pd.DataFrame({'event_name':['t']*5,'key':['playType','play_type','adUnitId','ad_unit_id','total'],
        'value_type':['string_value','int_value','int_value','int_value','int_value']}).set_index('event_name')
        print(df)
        table_column,event_name = c.table_column(df)

        print(table_column)
        return table_column

    # @unittest.skip('skip')
    # def test_times(self):
    #     while True:
    #         result = self.test_send_mail()
    #         resultlist = ['success','fail']
    #         self.assertIn(result,resultlist)


if __name__ == '__main__':
    
    unittest.main()
    