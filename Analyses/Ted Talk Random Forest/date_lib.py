import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
def get_past_date(str_days_ago, start_date = datetime.date.today()):
    splitted = str_days_ago.split()
    if splitted[0] == 'a':
        splitted[0] = 1
    if len(splitted) == 1 and splitted[0].lower() == 'today':
        return str(start_date.isoformat())
    elif len(splitted) == 1 and splitted[0].lower() == 'yesterday':
        date = start_date - relativedelta(days=1)
        return str(date.isoformat())
    elif len(splitted) == 1:
        np.nan
    elif splitted[1].lower() in ['hour', 'hours', 'hr', 'hrs', 'h']:
        date = start_date - relativedelta(hours=int(splitted[0]))
        return str(date.date().isoformat())
    elif splitted[1].lower() in ['day', 'days', 'd']:
        date = start_date - relativedelta(days=int(splitted[0]))
        return str(date.isoformat())
    elif splitted[1].lower() in ['wk', 'wks', 'week', 'weeks', 'w']:
        date = start_date - relativedelta(weeks=int(splitted[0]))
        return str(date.isoformat())
    elif splitted[1].lower() in ['mon', 'mons', 'month', 'months', 'm']:
        date = start_date - relativedelta(months=int(splitted[0]))
        return str(date.isoformat())
    elif splitted[1].lower() in ['yrs', 'yr', 'years', 'year', 'y']:
        date = start_date - relativedelta(years=int(splitted[0]))
        return str(date.isoformat())
    else:
        return "Wrong Argument format:" + splitted
    
import re
import pandas as pd
def add_datepart(df, fldname, drop=True, time=False, errors="raise"):
    fld = df[fldname]
    fld_dtype = fld.dtype
    if isinstance(fld_dtype, pd.core.dtypes.dtypes.DatetimeTZDtype):
        fld_dtype = np.datetime64

    if not np.issubdtype(fld_dtype, np.datetime64):
        df[fldname] = fld = pd.to_datetime(fld, infer_datetime_format=True, errors=errors)
    targ_pre = re.sub('[Dd]ate$', '', fldname)
    attr = ['Year', 'Month', 'Week', 'Day', 'Dayofweek', 'Dayofyear',
            'Is_month_end', 'Is_month_start', 'Is_quarter_end', 'Is_quarter_start', 'Is_year_end', 'Is_year_start']
    if time: attr = attr + ['Hour', 'Minute', 'Second']
    for n in attr: df[targ_pre + n] = getattr(fld.dt, n.lower())
    df[targ_pre + 'Elapsed'] = fld.astype(np.int64) // 10 ** 9
    if drop: df.drop(fldname, axis=1, inplace=True)