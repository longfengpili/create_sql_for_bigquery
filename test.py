import unittest
import pandas as pd
from parse_table import create_for_bigquery
import re

class TestDict(unittest.TestCase):

    def setUp(self):
        print('setUp...')
        self.c = create_for_bigquery()
        self.df = pd.DataFrame({'event_name':['ad_show']*5,'key':['playType','play_type','adUnitId','ad_unit_id','group_id'],
        'value_type':['string_value','int_value','int_value','int_value','int_value']}).set_index('event_name')
        # print(self.df)
        

    def tearDown(self):
        print('tearDown...')

    @unittest.skip('skip')
    def test_table_column(self):
        table_column,event_name = self.c.table_column(self.df)

        print(table_column)
        return table_column

    @unittest.skip('skip')
    def test_pop_prefix(self):
        columns = 'app_info.id,app_info.version,geo.city,device.mobile_brand_name,device.mobile_model_name,device.language'
        columns = self.c._pop_prefix(columns)
        print(columns)

    def test_report_create_table(self):
        columns_name = self.c.report_create_table(self.df)

    def test_first_value_sql(self):
        sql_for_report = self.c.first_value_sql(self.c.first_value_list)
        print(sql_for_report)

    def test_agg_func(self):
        agg_fun = self.c.agg_func(dict)
        print(agg_fun)

    def test_report_insert_table(self):
        sql_for_report = self.c.report_insert_table(self.df)
        print(sql_for_report)

if __name__ == '__main__':
    
    # unittest.main()

    suite = unittest.TestSuite()
    suite.addTest(TestDict('test_report_insert_table'))
    runner = unittest.TextTestRunner()
    runner.run(suite)
