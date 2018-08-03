import pandas as pd
import os,sys
import re

sys.path.append('..')
import config_for_bigquery as config


class create_for_bigquery(object):

    def __init__(self):
        self.project = config.project_name
        self.table = config.table_name
        self.event_column_sort = config.event_column_sort
        self.change_column_type = config.change_column_type
        self.fliter_fields = config.fliter_fields
        self.fliter_event_name = config.fliter_event_name
        self.base_fields_first = config.base_fields_first
        self.base_fields_second = config.base_fields_second
        self.filepath = config.filepath
        self.createpath = config.createpath
    

    def _modify_column(self,column_name):
        pattern="[A-Z]"
        column_name=re.sub(pattern,lambda x:"_"+x.group(0).lower(),column_name)
        return column_name
    
    def sort_column(self,df):
        table_column_info = df.values
        event_name = df.index.unique().values[0]

        columns_sort = {}
        for k,v in enumerate(self.event_column_sort):
            columns_sort[v] = k

        table_column_sort = {}
        global non_in_column
        for i in table_column_info:
            column = i[0]
            column_type = i[1]
            if column in self.event_column_sort:
                table_column_sort[columns_sort[column]] = [column,column_type]
            elif column in self.fliter_fields:
                pass
            else:
                non_in_column.append(column)
        table_column_sorted = [table_column_sort[key] for key in sorted(table_column_sort.keys())]
        # print(table_column_sorted)
        return table_column_sorted,event_name
    
    def key_value(self,key,value_type,target_type='string'):
        if key in self.change_column_type.keys():
            target_type = self.change_column_type[key]
        # else:
        #     change_target_type = target_type

        value = "(select cast(value.{1} as {3}) from unnest(event_params) where key = '{0}') as {2}".format(
                key,value_type,self._modify_column(key),target_type)
        return value

    def table_column(self,df):
        table_column_info,event_name = self.sort_column(df)
        select_list = []
        for i in table_column_info:
            # print(i)
            select_list.append(self.key_value(i[0],i[1]))
        table_column = ",\n".join(select_list)
        return table_column,event_name

    def create_table(self,df):
        table_column,event_name = self.table_column(df)
        print(event_name)
        print(table_column)
        if event_name in self.fliter_event_name:
            sql_for_create = ''
        else:
            if table_column != '':            
                sql_for_create = '''
                --{0}.{5}
                create table raw_data_{0}.{1} as
                select {2},
                {3},
                {4}
                from {6} 
                where event_name = '{5}'
                ;
                '''.format(self.project,self.table,self.base_fields_first,table_column,
                            self.base_fields_second,event_name,self.table)
            else:
                sql_for_create = '''
                --{0}.{5}
                create table raw_data_{0}.{1} as
                select {2},
                {4}
                from {6} 
                where event_name = '{5}'
                ;
                '''.format(self.project,self.table,self.base_fields_first,table_column,self.base_fields_second,event_name,self.table)
        return re.sub('    ','',sql_for_create)

if __name__ == '__main__':
    
    non_in_column = []

    c = create_for_bigquery()
    filepath = c.filepath
    createpath = c.createpath
    f = open(filepath,'r')
    data = pd.read_csv(f,index_col=['event_name'])
    f.close()

    # data = data.loc['ad_close',:]
    # print(c.sort_column(data))
    
    for i in data.index.unique().values:
        result = data.loc[[i]].copy()
        # print(result)
        with open(createpath,'a',encoding='utf-8') as f:
            f.write(c.create_table(result))

    with open(createpath,'a',encoding='utf-8') as f:
            f.write('\n出现额外的列名{}'.format(non_in_column))
