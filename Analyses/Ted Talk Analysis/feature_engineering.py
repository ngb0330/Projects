import ast
import pandas as pd
import numpy as np
import json
import datetime

def str_to_dict_list(original_data, column):
    new_data = original_data.copy()
    new_data['{}'.format(column)] = new_data[column].apply(lambda d: ast.literal_eval(str(d)))
    return new_data

def get_key_set(dataframe, column, key):
    return set(list_row[key] for row in dataframe[column] for list_row in row)

def add_dict_list_key_counts(dataframe, column, key_set, key1, key2):
    new_dataframe = dataframe[[column]].copy()
    for i,row in new_dataframe.iterrows():
        for dict_item in row[column]:
            if dict_item[key1] in key_set:
                      new_dataframe.at[i,'{}_{}'.format(dict_item[key1], column)] = dict_item[key2]
    return new_dataframe._get_numeric_data()


def string_to_list(original_data, column, sep):
    new_data = original_data.copy()
    new_data['{}'.format(column)] = new_data[column].str.strip('][').str.replace("'",'').str.split(sep)
    return new_data

def get_values_list(data_frame, column):
    column_list = []
    for i in range(data_frame.shape[0]):
        for value in data_frame[column][i]:
            column_list.append(value.strip())
    return column_list

def get_counts_dict(value_list):
    from collections import Counter
    cnt = Counter()
    for value in value_list:
        cnt[value]+=1
    return dict(cnt)

def get_counts_data_frame(data_frame, counts_dict):
    import pandas as pd
    key_array,value_array = counts_dict.keys(), counts_dict.values()
    value_percentages = [value/data_frame.shape[0] * 100 for value in value_array]
    new_data_frame = pd.DataFrame(list(zip(key_array,value_array,value_percentages)), 
                                 columns = ['Key', 'Value', 'Value_Percentage'])
    return new_data_frame

def add_list_dummies(original_data, column, dummies_list, threshhold):
    data = original_data[[column]].copy()
    tags_list = []
    for i,row in data.iterrows():
        for value in row[column]:
            data.at[i,'is_{}_{}'.format(value, column)] = int(value.strip() in dummies_list)
    data = data._get_numeric_data().fillna(0)
    new_threshhold = threshhold * original_data.shape[0]
    return data.loc[:, data.sum() > (threshhold)]

def string_column_length(data_frame, column):
    new_data_frame = data_frame[[column]].copy()
    new_data_frame['{}_length'.format(column)] = new_data_frame[column].str.len()
    return new_data_frame._get_numeric_data()

def unix_to_datetime(data_frame, column):
    import datetime, pandas as pd
    new_data_frame = data_frame[[column]].copy()
    new_data_frame['{}'.format(column)] = new_data_frame[column].apply(lambda d: datetime.datetime.fromtimestamp(int(d)).strftime('%Y-%m-%d %H:%M:%S'))
    return new_data_frame[[column]]

def days_between(d1, d2):
    d1 = datetime.datetime.strptime(d1, '%Y-%m-%d %H:%M:%S')
    d2 = datetime.datetime.strptime(d2, '%Y-%m-%d %H:%M:%S')
    return abs((d2 - d1).days)

def add_days_between(original_data, col1, col2):
    data = original_data.copy()[[col1,col2]]
    data['days_between_{}_and_{}'.format(col1,col2)] = data[[col1,col2]].apply(lambda x: days_between(*x), axis=1)
    return data[['days_between_{}_and_{}'.format(col1,col2)]]

def merge_dataframes(original_dataframe, dataframelist, how):
    new_dataframe = original_dataframe.copy()
    for df in dataframelist:
        new_dataframe = new_dataframe.join(df, how = how)
    return new_dataframe
