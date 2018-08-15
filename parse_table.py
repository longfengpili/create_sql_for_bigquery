import pandas as pd
import os
import sys
import re

sys.path.append('..')
import config_for_bigquery as config

non_in_column = []


class create_for_bigquery(object):

    def __init__(self):
        self.project_name = config.project_name
        self.table_name = config.table_name
        self.schema_name = config.schema_name
        self.column_fixed = config.column_fixed
        self.event_column_sort = config.event_column_sort
        self.change_column_type = config.change_column_type
        self.fliter_fields = config.fliter_fields
        self.fliter_event_name = config.fliter_event_name
        self.base_fields_first = config.base_fields_first
        self.base_fields_first_no_function = config.base_fields_first_no_function
        self.base_fields_second = config.base_fields_second
        self.report_columns_fixed = config.report_columns_fixed
        self.report_first_value_list = config.report_first_value_list
        self.report_columns_pop = config.report_columns_pop
        self.report_agg_columns_pop = config.report_agg_columns_pop
        self.report_events = config.report_events

        self.filepath = config.filepath
        self.raw_createpath = config.raw_createpath
        self.raw_insertpath = config.raw_insertpath
        self.report_createtpath = config.report_createtpath
        self.report_insertpath = config.report_insertpath

    def _modify_column(self, column_name):
        pattern = "[A-Z]"
        column_name = re.sub(pattern, lambda x: "_" +
                             x.group(0).lower(), column_name)
        return column_name

    def _pop_prefix(self, columns):
        columns = re.sub('\w*?\.', lambda x: '', columns)
        return columns

    def sort_column(self, df):
        table_column_info = df.values
        event_name = df.index.unique().values[0]

        columns_sort = {}
        for k, v in enumerate(self.event_column_sort):
            columns_sort[v] = k

        table_column_sort = {}
        global non_in_column
        for i in table_column_info:
            column = i[0]
            column_type = i[1]
            if column in self.event_column_sort:
                table_column_sort[columns_sort[column]] = [column, column_type]
            elif column in self.fliter_fields:
                pass
            elif column not in non_in_column:
                non_in_column.append(column)
            else:
                pass
        table_column_sorted = [table_column_sort[key]
                               for key in sorted(table_column_sort.keys())]
        # print(table_column_sorted)
        for i in self.column_fixed:
            if i in table_column_sorted:
                table_column_sorted.pop(table_column_sorted.index(i))

            table_column_sorted.insert(0, i)

        # print('{}-{}'.format(event_name,table_column_sorted))

        return table_column_sorted,event_name
    
    def first_value_sql(self,columns):
        '''columns is list'''
        first_value = []
        for i in columns:
            first_value_i = "first_value({0} ignore nulls) over (partition by user_pseudo_id,id,platform,event_date order by ts " \
            "rows between unbounded preceding and unbounded following) as {0}".format(i)
            first_value.append(first_value_i)
        first_value = ',\n'.join(first_value)
        return first_value

    def key_from_unnest(self,key,value_type,target_type='string'):
        if key in self.change_column_type.keys():
            target_type = self.change_column_type[key]

        value = "(select cast(value.{1} as {3}) from unnest(event_params) where key = '{0}') as {2}".format(
                key, value_type, self._modify_column(key), target_type)
        return value

    def merge_on(self,agg_columns,comp='=',join_str='and'):
        merge_on = []
        for i in agg_columns:
            agg_column_on = 'T.{0} {1} S.{0}'.format(i,comp)
            merge_on.append(agg_column_on)
        
        merge_on = ' {} '.format(join_str).join(merge_on)
        return merge_on            

    def columns_from_unnest(self,df):
        table_column_sorted,event_name = self.sort_column(df)
        select_list = []
        for i in table_column_sorted:
            # print(i)
            select_list.append(self.key_from_unnest(i[0],i[1]))
        columns_from_unnest = ",\n".join(select_list)
        
        column_l = re.findall(r'\) as (\w*)',columns_from_unnest)
        for i in column_l:
            if column_l.count(i) > 1:
                for j in range(column_l.count(i)):
                    columns_from_unnest = re.sub('\) as {}(?![0-9_])'.format(i),lambda i:i.group(0)+'_{}'.format(j),columns_from_unnest,1)
                    
        return columns_from_unnest,event_name

    def agg_func(self,dict):
        column = dict.get('column')
        agg_column = dict.get('agg_column')
        distinct = dict.get('distinct')
        aggfunc = dict.get('aggfunc')
        dtype = dict.get('dtype')

        if not aggfunc:
            aggfunc = 'sum'
        
        if not agg_column:
            agg_column = column

        if dtype:
            column = 'cast({} as {})'.format(column,dtype)
        
        if distinct:
            agg_func = '{0}(distinct {1}) as {2}'.format(aggfunc,column,agg_column)
        else:
            agg_func = '{0}({1}) as {2}'.format(aggfunc,column,agg_column)

            

        return agg_func


    def raw_data_create_table(self,df):
        columns_from_unnest,event_name = self.columns_from_unnest(df)
        # print(event_name)
        # print(columns_from_unnest)
        if event_name in self.fliter_event_name:
            sql_for_create = ''
        else:
            if columns_from_unnest != '':            
                sql_for_create = '''
                --{6}.{5}
                create table `{0}.raw_data_{6}.{5}`
                partition by event_date
                cluster by user_pseudo_id,id,platform,country
                as
                select {2},
                {3},
                {4}
                from {1} 
                where event_name = '{5}'
                ;
                '''.format(self.project_name,self.table_name,self.base_fields_first,columns_from_unnest,
                            self.base_fields_second,event_name,self.schema_name)
            else:
                sql_for_create = '''
                --{6}.{5}
                create `{0}.raw_data_{6}.{5}`
                partition by event_date
                cluster by user_pseudo_id,id,platform,country
                as
                select {2},
                {4}
                from {1} 
                where event_name = '{5}'
                ;
                '''.format(self.project_name,self.table_name,self.base_fields_first,columns_from_unnest,self.base_fields_second,event_name,self.schema_name)
        return re.sub('    ','',sql_for_create)

    def raw_data_insert_table(self,df):
        columns_from_unnest,event_name = self.columns_from_unnest(df)
        # print(columns_from_unnest)
        columns_name = ','.join(re.findall('\) as (\w*)',columns_from_unnest))
        base_fields_second_pop = self._pop_prefix(self.base_fields_second)
        # print(columns_name)
        if event_name in self.fliter_event_name:
            sql_for_insert = ''
        else:
            if columns_from_unnest != '':            
                sql_for_insert = '''
                --{10}.{5}
                merge `{0}.raw_data_{10}.{5}` T
                using
                (select {2},
                {3},
                {4}
                from {6} 
                where event_name = '{5}') S
                on T.user_pseudo_id = S.user_pseudo_id and T.id = S.id and T.platform = S.platform
                and T.event_name = S.event_name and T.event_timestamp = S.event_timestamp
                when not matched then
                insert
                ({8},
                {7},
                {9})
                values
                ({8},
                {7},
                {9})
                ;
                '''.format(self.project_name,self.table_name,self.base_fields_first,columns_from_unnest,
                            self.base_fields_second,event_name,self.table_name,columns_name,self.base_fields_first_no_function,base_fields_second_pop,self.schema_name)
            else:
                sql_for_insert = '''
                --{10}.{5}
                merge `{0}.raw_data_{10}.{5}` T
                using
                (select {2},
                {4}
                from {6} 
                where event_name = '{5}') S
                on T.user_pseudo_id = S.user_pseudo_id and T.id = S.id and T.platform = S.platform
                and T.event_name = S.event_name and T.event_timestamp = S.event_timestamp
                when not matched then
                insert
                ({8},
                {9})
                values
                ({8},
                {9})
                ;
                '''.format(self.project_name,self.table_name,self.base_fields_first,columns_from_unnest,
                            self.base_fields_second,event_name,self.table_name,columns_name,self.base_fields_first_no_function,base_fields_second_pop,self.schema_name)
        return re.sub('    ','',sql_for_insert)

    def report_create_table(self,df):
        
        sql_for_report = ''

        columns_from_unnest,event_name = self.columns_from_unnest(df)
        columns_name = re.findall('\) as (\w*)',columns_from_unnest)

        # print(event_name)
        # print(columns_name)

        report_first_value_list = []
        for c in self.report_first_value_list:
            if c not in report_first_value_list:
                report_first_value_list.append(c)
            for m in columns_name:
                if m.startswith(c):
                    report_first_value_list.append(m)
                    columns_name.pop(columns_name.index(m))
                    try:
                        report_first_value_list.pop(report_first_value_list.index(c))
                    except:
                        pass
        # print(columns_name)                
        columns_name_str = ','.join(columns_name)
        columns_name_str = self._pop_prefix(columns_name_str)
        # print(columns_name_str)

        for i in self.report_columns_pop:
            i = self._modify_column(i)
            try:
              columns_name.pop(columns_name.index(i))
            except:
                pass
        if event_name in self.report_events.keys():
            if self.report_events[event_name].get('pop'):
                for i in self.report_events[event_name].get('pop'):
                    i = self._modify_column(i)
                    try:
                        columns_name.pop(columns_name.index(i))
                    except:
                        pass
        # print(columns_name)

        columns_all = self.report_columns_fixed + columns_name + report_first_value_list
        agg_columns_l = [i for i in columns_all if i not in self.report_agg_columns_pop]
        columns_num = ','.join([str(i + 1) for i in range(len(agg_columns_l))])
        agg_columns = ','.join([i for i in columns_all if i not in self.report_agg_columns_pop])
        # print(agg_columns)

        first_values = self.first_value_sql(report_first_value_list)
        report_columns_fixed = ','.join(self.report_columns_fixed)
        
        if event_name in self.report_events.keys():
            # print(self.report_events[event_name].values())
            other_agg = []
            other_agg_as = []
            for k in self.report_events[event_name].values():
                if isinstance(k,dict):
                    agg_column = self.agg_func(k)
                    other_agg.append(agg_column)
                    other_agg_as.append(k.get('agg_column'))

            other_agg = ','.join(other_agg)
            func_column = ['{}_users'.format(event_name),'{}_times'.format(event_name)] + other_agg_as
            func_column = [i for i in func_column if i is not None]

            if other_agg:
                sql_for_report = '''
                    --{2}.{3}
                    create table `{0}.report_{2}.{3}`
                    partition by event_date
                    cluster by id,platform,country,group_id
                    as
                    with report_{3} as
                    (with temp_{3} as
                    (select {4},
                    {5},
                    {6}
                    from {1} 
                    where event_name = '{3}')
                    select {7},
                    {8},
                    {9}
                    from temp_{3})
                    select {12},
                    count(distinct user_pseudo_id) {3}_users,count(user_pseudo_id) {3}_times,{11}
                    from report_{3}
                    group by {10};
                '''.format(self.project_name,self.table_name,self.schema_name,event_name,self.base_fields_first,columns_from_unnest,self.base_fields_second,
                            report_columns_fixed,first_values,columns_name_str,columns_num,other_agg,agg_columns)
            else:
                sql_for_report = '''
                    --{2}.{3}
                    create table `{0}.report_{2}.{3}`
                    partition by event_date
                    cluster by id,platform,country,group_id
                    as
                    with report_{3} as
                    (with temp_{3} as
                    (select {4},
                    {5},
                    {6}
                    from {1} 
                    where event_name = '{3}')
                    select {7},
                    {8},
                    {9}
                    from temp_{3})
                    select {12},
                    count(distinct user_pseudo_id) {3}_users,count(user_pseudo_id) {3}_times
                    from report_{3}
                    group by {10};
                '''.format(self.project_name,self.table_name,self.schema_name,event_name,self.base_fields_first,columns_from_unnest,self.base_fields_second,
                            report_columns_fixed,first_values,columns_name_str,columns_num,other_agg,agg_columns)

        return re.sub('  ','',sql_for_report)

    def report_insert_table(self,df):
        
        sql_for_report = ''

        columns_from_unnest,event_name = self.columns_from_unnest(df)
        columns_name = re.findall('\) as (\w*)',columns_from_unnest)

        # print(event_name)
        # print(columns_name)

        report_first_value_list = []
        for c in self.report_first_value_list:
            if c not in report_first_value_list:
                report_first_value_list.append(c)
            for m in columns_name:
                if m.startswith(c):
                    report_first_value_list.append(m)
                    columns_name.pop(columns_name.index(m))
                    try:
                        report_first_value_list.pop(report_first_value_list.index(c))
                    except:
                        pass
        # print(columns_name)                
        columns_name_str = ','.join(columns_name)
        columns_name_str = self._pop_prefix(columns_name_str)
        # print(columns_name_str)

        for i in self.report_columns_pop:
            i = self._modify_column(i)
            try:
              columns_name.pop(columns_name.index(i))
            except:
                pass
        if event_name in self.report_events.keys():
            if self.report_events[event_name].get('pop'):
                for i in self.report_events[event_name].get('pop'):
                    i = self._modify_column(i)
                    try:
                        columns_name.pop(columns_name.index(i))
                    except:
                        pass
        # print(columns_name)

        columns_all = self.report_columns_fixed + columns_name + report_first_value_list
        agg_columns_l = [i for i in columns_all if i not in self.report_agg_columns_pop]
        columns_num = ','.join([str(i + 1) for i in range(len(agg_columns_l))])
        agg_columns = ','.join([i for i in columns_all if i not in self.report_agg_columns_pop])
        # print(agg_columns)

        first_values = self.first_value_sql(report_first_value_list)
        report_columns_fixed = ','.join(self.report_columns_fixed)
        
        if event_name in self.report_events.keys():
            # print(self.report_events[event_name].values())
            other_agg = []
            other_agg_as = []
            for k in self.report_events[event_name].values():
                if isinstance(k,dict):
                    agg_column = self.agg_func(k)
                    other_agg.append(agg_column)
                    other_agg_as.append(k.get('agg_column'))

            other_agg = ','.join(other_agg)
            func_column = ['{}_users'.format(event_name),'{}_times'.format(event_name)] + other_agg_as
            func_column = [i for i in func_column if i is not None]

            if other_agg:
                sql_for_report = '''
                    --{2}.{3}
                    delete `{0}.report_{2}.{3}` where event_date = {0};

                    insert into `{0}.report_{2}.{3}`
                    ({12},
                    {13})
                    with report_{3} as
                    (with temp_{3} as
                    (select {4},
                    {5},
                    {6}
                    from {1} 
                    where event_name = '{3}')
                    select {7},
                    {8},
                    {9}
                    from temp_{3})
                    select {12},
                    count(distinct user_pseudo_id) {3}_users,count(user_pseudo_id) {3}_times,{11}
                    from report_{3}
                    group by {10};
                '''.format(self.project_name,self.table_name,self.schema_name,event_name,self.base_fields_first,columns_from_unnest,self.base_fields_second,
                            report_columns_fixed,first_values,columns_name_str,columns_num,other_agg,agg_columns,(','.join(func_column)))
            else:
                sql_for_report = '''
                    --{2}.{3}
                    delete `{0}.report_{2}.{3}` where event_date = {0};

                    insert into `{0}.report_{2}.{3}`
                    ({12},
                    {13})
                    with report_{3} as
                    (with temp_{3} as
                    (select {4},
                    {5},
                    {6}
                    from {1} 
                    where event_name = '{3}')
                    select {7},
                    {8},
                    {9}
                    from temp_{3})
                    select {12},
                    count(distinct user_pseudo_id) {3}_users,count(user_pseudo_id) {3}_times
                    from report_{3}
                    group by {10};
                '''.format(self.project_name,self.table_name,self.schema_name,event_name,self.base_fields_first,columns_from_unnest,self.base_fields_second,
                            report_columns_fixed,first_values,columns_name_str,columns_num,other_agg,agg_columns,(','.join(func_column)))

        return re.sub('  ','',sql_for_report)

if __name__ == '__main__':

    c = create_for_bigquery()
    f = open(c.filepath, 'r')
    data = pd.read_csv(f, index_col=['event_name'])
    f.close()

    # data = data.loc['ad_close',:]
    # print(c.insert_table(data))

    for i in data.index.unique().values:
        result = data.loc[[i]].copy()
        # print(result)
        # with open(c.raw_createpath, 'a', encoding='utf-8') as f:
        #     f.write(c.raw_data_create_table(result))

        # with open(c.raw_insertpath, 'a', encoding='utf-8') as f:
        #     f.write(c.raw_data_insert_table(result))
        
        with open(c.report_createtpath, 'a', encoding='utf-8') as f:
            f.write(c.report_create_table(result))

        with open(c.report_insertpath, 'a', encoding='utf-8') as f:
            f.write(c.report_insert_table(result))

    with open(c.raw_createpath, 'a', encoding='utf-8') as f:
            f.write('\n出现额外的列名{}'.format(non_in_column))
