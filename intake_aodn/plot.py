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

