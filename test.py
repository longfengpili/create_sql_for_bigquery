import unittest
import pandas as pd
from parse_table import create_for_bigquery
import re

class TestDict(unittest.TestCase):

    def setUp(self):
        print('setUp...')
        self.c = create_for_bigquery()
        self.df = pd.DataFrame({'event_name':['t']*5,'key':['playType','play_type','adUnitId','ad_unit_id','total'],
        'value_type':['string_value','int_value','int_value','int_value','int_value']}).set_index('event_name')
        # print(self.df)
        

    def tearDown(self):
        print('tearDown...')

    @unittest.skip('skip')
    def test_table_column(self):
        table_column,event_name = self.c.table_column(self.df)

        print(table_column)
        return table_column

    def test_report_create_table(self):
        columns_name = self.c.report_create_table(self.df)
        print(columns_name)

    def test_pop_prefix(self):
        columns = 'app_info.id,app_info.version,geo.city,device.mobile_brand_name,device.mobile_model_name,device.language'
        columns = self.c.pop_prefix(columns)
        print(columns)


if __name__ == '__main__':
    
    # unittest.main()

    suite = unittest.TestSuite()
    suite.addTest(TestDict('test_pop_prefix'))
    runner = unittest.TextTestRunner()
    runner.run(suite)