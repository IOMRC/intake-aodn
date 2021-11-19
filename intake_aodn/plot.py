#!/usr/bin/env python
#-----------------------------------------------------------------------------
# Copyright (c) 2020 - 2021, CSIRO 
#
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

def Clim_plot(da,time_res,time_main=0,**kwargs):
    import matplotlib.pyplot as plt1
    from .analysis import make_clim
    import datetime
    
    if time_main == 0:
        time_main=[da.time.min(),da.time.max()]

    col_yr = ['red', 'blue', 'green', 'yellow', 'purple'] # this could be extended for more years
    
    fig, ax = plt1.subplots(figsize=(7, 4))
    clim,h95,l95 = make_clim(da,time_res,time_slice=time_main)
    clim.plot(label='Clim',color = 'black')
    plt1.fill_between(h95[time_res],l95,h95,alpha=0.5,color='grey')
    if 'time_recent' in kwargs and time_res=='month':
        tt = make_clim(da,time_res,time_slice= kwargs['time_recent'])[0].plot(label='Recent',color = 'black',linestyle='dashed')
    if 'ind_yr' in kwargs and time_res=='month':
        ind_yr = kwargs['ind_yr']
        i=0
        for y in kwargs['ind_yr']:
            time_ind_yr = [str(y) +'-01-01', str(y) +'-12-31']
            make_clim(da,time_res,time_slice=time_ind_yr)[0].plot(marker = '.',label=str(y),color = col_yr[i])
            i+=1
            
    xl = clim.coords[time_res].values
    plt1.xlim(xl.min(),xl.max())
    if time_res == 'month':
        ax.set_xticks([2,4,6,8,10,12])
        tlab = [datetime.date(1990,x,1).strftime('%b') for x in ax.get_xticks().astype(int) if x > 0 and x <13]
        ax.set_xticklabels([datetime.date(1990,x,1).strftime('%b') for x in [2,4,6,8,10,12]])
    plt1.legend(loc = 'lower left')
    return clim,ax
    
    ## correct mapping
def map_var(da,lim_lon,lim_lat,cmap,title = 'Map',subpos = 111,**map_kwargs):
    import cartopy
    import cartopy.crs as ccrs
    import matplotlib.pyplot as plt
    import numpy as np
    
    # default vmin and vmax of plot
    da_plot = da.sel(longitude=slice(lim_lon[0],lim_lon[1]),latitude =slice(lim_lat[1],lim_lat[0]))
    [vmin,vmax] = np.round(da_plot.quantile([0.025,0.975]),2)
    if 'vmin' not in map_kwargs:
        map_kwargs['vmin'] = vmin
    if 'vmax' not in map_kwargs:
        map_kwargs['vmax'] = vmax   
    
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

def multimap(da,col,col_wrap,freq = 'month',**map_kwargs):
# This function uses the Facetgrid functionality of xarray to plot contours of a given dataset based on a coordinate.
# Required arguments are the data to be plotted, the coordinate to use as subplot (usually time) and the number of columns.
    import cartopy
    import cartopy.crs as ccrs
    import numpy as np
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    import math
    import pandas as pd
    
    defaults = {'levels':16,'robust':True,'extend':'both','cmap':None,'cbar_kwargs':{'shrink':0.7,'pad':0.05}}
    defaults.update(map_kwargs)
    
    # create dictionnary of format and add one as default
    format_dict = {'day':'%d-%b-%Y','month':'%Y-%b','year':'%Y'}
    form = format_dict[freq]
    
    # work out gridlines and ticks
    nb_line = 3
    inc_choice = [10, 5, 2, 1,.5]
    int_x = abs(da.longitude[-1]-da.longitude[0])/nb_line
    int_y = abs(da.latitude[-1]-da.latitude[0])/nb_line
    inc_x = inc_choice[(inc_choice-int_x.to_numpy()).argmin()]
    inc_y = inc_choice[(inc_choice-int_y.to_numpy()).argmin()]

    xtic = np.arange(math.ceil(da.longitude.min()/inc_x)*inc_x,da.longitude.max(),inc_x)
    ytic = np.arange(math.ceil(da.latitude.min()/inc_y)*inc_y,da.latitude.max(),inc_y)

    # add map_kwargs arguments
    
    fig = da.plot.contourf(transform = ccrs.PlateCarree(),col=col,col_wrap=col_wrap,
                     subplot_kws={'projection':ccrs.PlateCarree(),'xmargin':1},
                     **defaults)

    fig.map(lambda: plt.gca().add_feature(cartopy.feature.GSHHSFeature(scale = 'high',facecolor = 'grey',edgecolor = 'black')))
    for i in range(0,len(fig.axes.flat)):
        gl = fig.axes.flat[i].gridlines(draw_labels=True)
        gl.xlocator = mticker.FixedLocator(xtic)
        gl.ylocator = mticker.FixedLocator(ytic)
        gl.top_labels=False
        gl.right_labels=False
        gl.left_labels = True if i%(col_wrap) == 0 else False
        gl.bottom_labels = True if i>=(len(fig.axes.flat)-col_wrap) else False
        #gl.xlabel_style = {'size':10}
        #gl.ylabel_style = {'size':10}
        fig.axes.flat[i].set_title(pd.to_datetime(da.time[i].values).strftime(form),fontsize = 20);
    
    plt.subplots_adjust(wspace = 0.1,right = 0.8)
    lab = fig.cbar.ax.get_ylabel()
    #re-adjust cb position
    sub_pos = fig.axes.flat[col_wrap-1].get_position()
    cb_xpad = sub_pos.width*0.2
    cb_pos = fig.cbar.ax.get_position()
    cb_pos.x0 = sub_pos.x1+cb_xpad
    fig.cbar.ax.set_position(cb_pos) 
    
    fig.cbar.set_label(lab,size = 18)
    fig.cbar.ax.tick_params(labelsize = 12)
    
    return fig