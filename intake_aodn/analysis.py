#!/usr/bin/env python
#-----------------------------------------------------------------------------
# Copyright (c) 2020 - 2021, CSIRO 
#
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------


                
def make_clim(da,time_res='month',**kwargs):
    import numpy as np
    if 'time_slice' in kwargs:
        ct = da.sel(time=slice(kwargs['time_slice'][0],kwargs['time_slice'][1])).groupby('time.' + time_res).count(dim='time')
        s = da.sel(time=slice(kwargs['time_slice'][0],kwargs['time_slice'][1])).groupby('time.' + time_res).std(dim='time')
        clim = da.sel(time=slice(kwargs['time_slice'][0],kwargs['time_slice'][1])).groupby('time.' + time_res).mean(dim='time')
    else:
        clim = da.groupby('time.' + time_res).mean(dim='time')
        s = da.groupby('time.' + time_res).std(dim='time')
        ct = da.groupby('time.' + time_res).count(dim='time')
        
    h95 = clim + 1.96*s/np.sqrt(ct)
    l95 = clim - 1.96*s/np.sqrt(ct)
    return clim,h95,l95



def time_average(da,dt,var,**kwargs):
    import pandas as pd #
    #dt specifies the type of resampling and can take values of 'M', 'Y' or 'DJF' for season
    if len(dt)==3:
        quarters = {'DJF':'Q-Feb','MAM':'Q-May','JJA':'Q-Aug','SON':'Q-Nov'}
        mth_num = {'DJF':2,'MAM':5,'JJA':8,'SON':11}
        m = mth_num[dt]
        dt_q = quarters[dt]
        t_unit = 'Y'
        # Method 1: ignores incomplete seasons
        if kwargs['ignore_inc']:
            avg_da=da[var].resample(time=dt_q).mean(skipna = True)
            avg_da = avg_da.sel(time=avg_da['time.month']==m).groupby('time.year').mean()
        else:
        # Method2: replaces incomplete seasons with Na
            avg_da=da[var].resample(time='1M').mean()
            avg_da = da.where(avg_da.time.dt.season==dt).rolling(min_periods=3,center=True,time=3).mean()
            avg_da = avg_da.groupby('time.year').mean('time')[var]
    else:
        t_unit = dt
        dt = '1'+dt
        avg_da=da[var].resample(time=dt).mean(skipna=True)
        avg_da['time'] = avg_da.time.astype('datetime64[' + t_unit +']')
        if not kwargs['ignore_inc']:
            ext_time = avg_da['time'][[0, len(avg_da['time'])-1]]
            if (da.time[len(da.time)-1].dt.day.values < 15) or ((da.time[len(da.time)-1].dt.month.values < 12) and (t_unit=='Y')):
                avg_da = avg_da.where(avg_da.time!=avg_da.time[len(avg_da.time)-1])

            if avg_da['time'][0] < da['time'][0]-pd.to_timedelta(15,'D'):
                avg_da = avg_da.where(avg_da.time!=avg_da.time[0])
            #last_day = da.time[(da.time + pd.to_timedelta(1,'D')).dt.month != (da.time).dt.month].astype('datetime64[M]') 
            #avg_da = avg_da.where((avg_da.time.isin(last_day) & avg_da.time.isin(da.time)))

    return avg_da

    
def lin_trend(da,coord,deg=1):
    import xarray as xr
    import numpy as np
    from scipy import stats
    
    f=da.polyfit(dim=coord,deg=1)
    fit = xr.polyval(da[coord],f)
    fit = fit.rename({'polyfit_coefficients':'linear_fit'})
    n = len(da[coord])
    x2 = xr.DataArray(range(1,len(da[coord])+1),dims =coord,coords={coord:da[coord]})
    serr= np.sqrt(((da-fit['linear_fit'])**2).sum(dim=coord)/(len(da[coord])-1)).expand_dims(dim = {coord:n})
    t = stats.t.ppf(1-0.025, len(da[coord]))
    B = np.sqrt(1/n + (x2 - np.mean(x2))**2 / np.sum((x2-np.mean(x2))**2))
    ci = B*serr*t
    ci+fit

    hci = ci+fit
    lci = fit-ci
   # f=da.polyfit(dim=coord,deg=1)
   # fit = xr.polyval(da[coord],f)
   # fit = fit.rename({'polyfit_coefficients':'linear_fit'})
   # n = len(da[coord])
   # x2 = range(1,len(da[coord])+1)
   # serr = np.sqrt(np.sum((da-fit)**2)/(len(da[coord])-1))
   # t = stats.t.ppf(1-0.025, len(da[coord]))

    #ci = t * serr['linear_fit'].values * np.sqrt(1/n + (x2 - np.mean(x2))**2 / np.sum((x2-np.mean(x2))**2))
    #hci = ci+fit
    #lci = fit-ci
    return f,fit,hci,lci

