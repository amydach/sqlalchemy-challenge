#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().run_line_magic('matplotlib', 'inline')
from matplotlib import style
style.use('fivethirtyeight')
import matplotlib.pyplot as plt


# In[2]:


import numpy as np
import pandas as pd


# In[3]:


import datetime as dt


# # Reflect Tables into SQLAlchemy ORM

# In[4]:


# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify


# In[5]:


engine = create_engine("sqlite:///Resources/hawaii.sqlite")


# In[6]:


# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)


# In[7]:


# We can view all of the classes that automap found
Base.classes.keys()


# In[8]:


# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station


# In[9]:


# Create our session (link) from Python to the DB
session = Session(engine)


# # Exploratory Climate Analysis

# In[10]:


# Design a query to retrieve the last 12 months of precipitation data and plot the results

# Perform a query to retrieve the data and precipitation scores
prev_year = dt.date(2017, 8, 23) - dt.timedelta(days=365)
results = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= prev_year).all()
#results = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= prev_year).limit(10)


# Save the query results as a Pandas DataFrame and set the index to the date column

    
df = pd.DataFrame(results, columns=['date', 'prcp'])
df.set_index('date', inplace=True, )

# Sort
df=df.sort_index()
df.head()

# Plot
df.plot(rot=90)


# In[11]:


# Export precipation data to csv to later import into SQLite
from pandas import DataFrame

precip_df = DataFrame(results, columns= ['date', 'prcp'])

precip_df.to_csv('precipitation.csv', header=True, sep=',', index=False)


# ### ![precipitation](Images/precipitation.png)

# In[12]:


# Calculate the date 1 year ago from the last data point in the database

prior_date = dt.date(2017, 8, 23) - dt.timedelta(days=365)
print(prior_date)
#measurement_list = session.query(Measurement)
#results = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= prev_year).all()


# In[13]:


# Use Pandas to calcualte the summary statistics for the precipitation data

df.describe()


# ![describe](Images/describe.png)

# In[14]:


# Design a query to show how many stations are available in this dataset?

station_list = session.query(Station)
results = session.query(func.count(Station.id))
stations_df = pd.DataFrame(results, columns=['count_1'])
stations_df


# In[15]:


# What are the most active stations? (i.e. what stations have the most rows)?

# List the stations and the counts in descending order.
active_stations=session.query(Measurement.station, func.count(Measurement.station)).    group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).all()
stations_df = pd.DataFrame(active_stations, columns=['stations','count'])
stations_df


# In[16]:


# Export station data to csv to later import into SQLite

stations_df.to_csv('stations.csv', header=True, sep=',', index=False)


# In[17]:


# Which station has the highest number of observations?

stations_df_max = stations_df.loc[stations_df['count'].idxmax()]
stations_df_max


# In[18]:


# Using the station id from the previous query, calculate the lowest temperature recorded for most active station?

station_min_data = session.query(Measurement.station, Measurement.tobs).filter(Measurement.station == 'USC00519281').    order_by((Measurement.tobs).desc()).all()

station_min_df = pd.DataFrame(station_min_data, columns=['station','tobs'])
station_min_df

stations_temp_min = station_min_df.loc[station_min_df['tobs'].idxmin()]
stations_temp_min


# In[19]:


# Using the station id from the previous query, calculate the highest temperature recorded for most active station?

station_max_data = session.query(Measurement.station, Measurement.tobs).filter(Measurement.station == 'USC00519281').    order_by((Measurement.tobs).desc()).all()

station_max_df = pd.DataFrame(station_max_data, columns=['station','tobs'])
station_max_df

stations_temp_max = station_max_df.loc[station_max_df['tobs'].idxmax()]
stations_temp_max


# In[20]:


# Using the station id from the previous query, calculate the average temperature recorded for most active station?

station_avg_data = session.query(Measurement.station, Measurement.tobs).filter(Measurement.station == 'USC00519281').    order_by((Measurement.tobs).desc()).all()

station_avg_df = pd.DataFrame(station_avg_data, columns=['station','tobs'])
station_avg_df

station_avg_df.loc[:,"tobs"].mean()


# In[21]:


# Export tobs data to csv to later import into SQLite

station_avg_df.to_csv('tobs.csv', header=True, sep=',', index=False)


# In[22]:


# Choose the station with the highest number of temperature observations.
# Query the last 12 months of temperature observation data for this station and plot the results as a histogram
histogram_data = session.query(Measurement.tobs).filter(Measurement.date >= prev_year).    filter(Measurement.station == 'USC00519281').all()

histogram_df = pd.DataFrame(histogram_data, columns=['tobs'])
histogram_df
hist = histogram_df.hist(bins=12)


# ![precipitation](Images/station-histogram.png)

# In[23]:


# This function called `calc_temps` will accept start date and end date in the format '%Y-%m-%d' 
# and return the minimum, average, and maximum temperatures for that range of dates
def calc_temps(start_date, end_date):
    """TMIN, TAVG, and TMAX for a list of dates.
    
    Args:
        start_date (string): A date string in the format %Y-%m-%d
        end_date (string): A date string in the format %Y-%m-%d
        
    Returns:
        TMIN, TAVE, and TMAX
    """
    
    return session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).        filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()

# function usage example
print(calc_temps('2012-02-28', '2012-03-05'))

