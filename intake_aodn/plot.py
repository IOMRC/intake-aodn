#!/usr/bin/env python
#-----------------------------------------------------------------------------
# Copyright (c) 2020 - 2021, CSIRO 
#
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

def display_entry(entry):
    import pandas as pd #
    print(f'NAME: {entry.name}')
    print(f'DESCRIPTION: {entry.description}')
    print(f'TYPE: {entry.container}')
    print(f'METADATA: ')
    for k,v in entry.metadata.items():
       print(f'    {k} : {v}') 
    print('USER PARAMETERS:')
    user_params = pd.DataFrame(entry.describe()['user_parameters'])
    display(user_params)
    
def get_list_datasets(cat):
    global da_list
    da_list = []
    global ser_list 
    ser_list = []
    da_ser = ["aodn_s3","nci"]
    for ser in da_ser:
        for entry in cat[ser]:
            da_list.append(cat[ser][entry].name)
            ser_list.append(ser)
        
    return da_list,ser_list

def time_average(da,dt,**kwargs):
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
            avg_da=da[kwargs['var']].resample(time=dt_q).mean(skipna = True)
            avg_da = avg_da.sel(time=avg_da['time.month']==m).groupby('time.year').mean()
        else:
        # Method2: replaces incomplete seasons with Na
            avg_da=da[kwargs['var']].resample(time='1M').mean()
            avg_da = da.where(avg_da.time.dt.season==dt).rolling(min_periods=3,center=True,time=3).mean()
            avg_da = avg_da.groupby('time.year').mean('time')[kwargs['var']]
    else:
        t_unit = dt
        dt = '1'+dt
        avg_da=da[kwargs['var']].resample(time=dt).mean(skipna=True)
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


def save_excel(df_list,file_name = "myfile",*args):
# args can contain a list of the name of sheets (sheetname)
# large spatial daily files can take 10-20mins
    if 'sheetname' in args:
        sheetname = args["sheet_name"]
    else:
        sheetname = list(str(range(1,len(df_list)+1)))
        
    with pd.ExcelWriter(file_name+'.xlsx') as writer:
    
        for i in range(0,len(df_list)):
            if df_list[i].shape[0]>=2**20:
                print("Warning: data #" +str(i)+ " is too large to save as a .xlsx file")
            else:
                df_list[i].to_excel(writer,sheet_name=str(sheetname[i]))
                
                
def make_clim(da,time_res='month',**kwargs):
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


def Clim_plot(da,time_main,time_res,**kwargs):
    col_yr = ['red', 'blue', 'green', 'yellow', 'purple'] # this could be extended for more years
    
    fig, ax = plt.subplots(figsize=(7, 4))
    clim,h95,l95 = make_clim(da,time_res,time_slice=time_main)
    clim.plot(label='Clim',color = 'black')
    plt.fill_between(h95[time_res],l95,h95,alpha=0.5,color='grey')
    if 'time_recent' in kwargs:
        tt = make_clim(da,time_res,time_slice= kwargs['time_recent'])[0].plot(label='Recent',color = 'black',linestyle='dashed')
    if 'ind_yr' in kwargs:
        ind_yr = kwargs['ind_yr']
        i=0
        for y in kwargs['ind_yr']:
            time_ind_yr = [str(y) +'-01-01', str(y) +'-12-31']
            make_clim(da,time_res,time_slice=time_ind_yr)[0].plot(marker = '.',label=str(y),color = col_yr[i])
            i+=1
            
    xl = clim.coords[time_res].values
    plt.xlim(xl.min(),xl.max())
    if time_res == 'month':
        ax.set_xticks([2,4,6,8,10,12])
        tlab = [datetime.date(1990,x,1).strftime('%b') for x in ax.get_xticks().astype(int) if x > 0 and x <13]
        ax.set_xticklabels([datetime.date(1990,x,1).strftime('%b') for x in [2,4,6,8,10,12]])
    plt.legend(loc = 'lower left')
    return clim,ax
    
    ## correct mapping
def map_var(da,lim_lon,lim_lat,cmap,title = 'Map',subpos = 111,**map_kwargs):
    import cartopy
    import cartopy.crs as ccrs
    import matplotlib.pyplot as plt
    
    axpl = da.plot.contourf(transform = ccrs.PlateCarree(),x = 'longitude',y='latitude',extend='both',cmap=cmap,subplot_kws={'projection':ccrs.PlateCarree()},**map_kwargs)
    ax = plt.gca()
    gl = plt.gca().gridlines(draw_labels=True)
    gl.top_labels=False
    gl.right_labels=False
    ax.add_feature(cartopy.feature.GSHHSFeature(scale = 'high',facecolor = 'grey',edgecolor = 'black'))
    ax.set_extent(lim_lon+lim_lat)
    ax.set_title(title,fontsize = 20);
    return ax,gl,axpl

# Should be called if no existing cb
def create_cb(fig,ax,ax_proj,label = '',size = "4%", pad = 0.5,**kwargs):
    import matplotlib.pyplot as plt
    from mpl_toolkits.axes_grid1 import make_axes_locatable
    import numpy as np
    
    divider = make_axes_locatable(ax)
    ax_cb = divider.new_horizontal(size = size, pad = pad, axes_class = plt.Axes)
    fig.add_axes(ax_cb)
    cb = plt.colorbar(ax_proj,cax = ax_cb)
    cb.set_label(label,**kwargs)
    return cb