# Import the dependencies.
import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

from datetime import datetime, timedelta

#################################################
# Database Setup
#################################################

# Create engine using the `hawaii.sqlite` database file
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
# Declare a Base using `automap_base()`
Base = automap_base()
# Use the Base class to reflect the database tables
Base.prepare(autoload_with=engine)

# Assign the measurement class to a variable called `Measurement` and
# the station class to a variable called `Station`
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create a session
#session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)



#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/precipitation/&lt;date_in_format_YYYY-MM-DD&gt;<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/tobs/mostactivestation<br/>"
        f"/api/v1.0/&lt;start_date_in_format_YYYY-MM-DD&gt;<br/>"
        f"/api/v1.0/&lt;start_date_in_format_YYYY-MM-DD&gt;/&lt;end_date_in_format_YYYY-MM-DD&gt;"
    )

@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of all stations"""
    # Query all stations
    results = session.query(Station.station).all()

    session.close()

    # Convert list of tuples into normal list
    all_names = list(np.ravel(results))

    return jsonify(all_names)

@app.route("/api/v1.0/precipitation/<date>")
def precipitation_by_date(date):
    """Fetch the precipitation value from the date that matches
       the path variable supplied by the user, or a 404 if not."""
    try:
        date_format = datetime.strptime(date, '%Y-%m-%d').date()
    except ValueError:
        return jsonify({"error":"Invalid date format. Use YYYY-MM-DD."}), 400

    session = Session(engine)
    output = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date == date_format).all()
    session.close()

    if output:
        result = output[0]
        return jsonify({"date": result.date, "prcp": result.prcp})
    else:
        return jsonify({"error": f"Date of {date} not found"}), 404

@app.route("/api/v1.0/precipitation")
def precipitation_last_year():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    """Return a list of dates and precipitation data"""
    # Query all stations
    latest_date_str = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    latest_date = datetime.strptime(latest_date_str, '%Y-%m-%d')

    # Calculate the date one year from the last date in data set.
    one_year_ago = latest_date - timedelta(days=365)
    one_year_ago_m = one_year_ago - timedelta(days=1)

    # Perform a query to retrieve the data and precipitation scores
    last_year = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= one_year_ago_m).all()
    
    #close session
    session.close()

    # Create a dictionary from the row data and append to a list of all_passengers
    data = []
    for date, prcp in last_year:
        record = {}
        record["date"] = date
        record["prcp"] = prcp
        data.append(record)

    return jsonify(data)

@app.route("/api/v1.0/tobs")
def tobs_last_year():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    """Return the temperature observations of the previous year"""
    # Query all stations
    latest_date_str = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    latest_date = datetime.strptime(latest_date_str, '%Y-%m-%d')

    # Calculate the date one year from the last date in data set.
    one_year_ago = latest_date - timedelta(days=365)
    one_year_ago_m = one_year_ago - timedelta(days=1)

    # Perform a query to retrieve the data and tobs scores
    last_year = session.query(Measurement.date, Measurement.tobs).filter(Measurement.date >= one_year_ago_m).all()
    
    #close session
    session.close()

    # Create a dictionary from the row data and append to a list of all_passengers
    data = []
    for date, tobs in last_year:
        record = {}
        record["date"] = date
        record["tobs"] = tobs
        data.append(record)

    return jsonify(data)

@app.route("/api/v1.0/tobs/mostactivestation")
def tobs_most_active_station_last_year():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    """Return the most active stations summary data"""
    # Query all stations
    latest_date_str = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    latest_date = datetime.strptime(latest_date_str, '%Y-%m-%d')

    # Calculate the date one year from the last date in data set.
    one_year_ago = latest_date - timedelta(days=365)
    one_year_ago_m = one_year_ago - timedelta(days=1)

    # Perform a query to retrieve the data and precipitation scores
    last_year = session.query(Measurement.station, Measurement.date, Measurement.tobs).filter(Measurement.date >= one_year_ago_m).all()

    # Query to find the most active stations 
    most_active_station = session.query(Measurement.station, func.count(Measurement.station).label('count')).\
group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).first()[0]

    #join station and measurement to identify most active station name

    data = [Measurement.station, Measurement.date, Measurement.prcp, Measurement.tobs, Station.station, Station.name]

    combined_data = session.query(*data).filter(Measurement.station == Station.station).group_by(Measurement.station).\
    order_by(func.count(Measurement.station).desc()).first()

    if combined_data:
        most_active_station = combined_data.station
        station_name = combined_data.name
 
    #calculate the lowest, highest and average temperature
    temp_stats = session.query(
    func.min(Measurement.tobs).label('min_temp'),
    func.max(Measurement.tobs).label('max_temp'),
    func.avg(Measurement.tobs).label('avg_temp')).filter(Measurement.station == most_active_station).all()

    #close session
    session.close()

    # Create a dictionary from the row data and append to a list of all_passengers
    temp_stats_dict = {
        "station_id": most_active_station,
        "station_name": station_name,
        "min_temp": temp_stats[0].min_temp,
        "max_temp": temp_stats[0].max_temp,
        "avg_temp": temp_stats[0].avg_temp
    }

    return jsonify(temp_stats_dict)

@app.route("/api/v1.0/<start>")
def tobs_by_start_date(start):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    #format date correctly
    format_start_date = datetime.strptime(start, '%Y-%m-%d')

    #query to find stats based on start date
    date_temp_stats = session.query(
    func.min(Measurement.tobs).label('min_temp'),
    func.max(Measurement.tobs).label('max_temp'),
    func.avg(Measurement.tobs).label('avg_temp')).filter(Measurement.date >= format_start_date).all()
    #close session
    session.close()

    #put data in a dictionary
    temp_stats_dict = {
        "start_date": start,
        "min_temp": date_temp_stats[0].min_temp,
        "max_temp": date_temp_stats[0].max_temp,
        "avg_temp": date_temp_stats[0].avg_temp
    }
    return jsonify(temp_stats_dict)

@app.route("/api/v1.0/<start>/<end>")
def tobs_by_start_and_end_date(start,end):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    #format date correctly
    format_start_date = datetime.strptime(start, '%Y-%m-%d')
    format_end_date = datetime.strptime(end, '%Y-%m-%d')
    
    #query to find stats based on start date
    date_temp_stats = session.query(
    func.min(Measurement.tobs).label('min_temp'),
    func.max(Measurement.tobs).label('max_temp'),
    func.avg(Measurement.tobs).label('avg_temp')).filter(Measurement.date >= format_start_date).filter(Measurement.date <= format_end_date).all()
    #close session
    session.close()

    #put data in a dictionary
    temp_stats_dict = {
        "start_date": start,
        "end_date": end,
        "min_temp": date_temp_stats[0].min_temp,
        "max_temp": date_temp_stats[0].max_temp,
        "avg_temp": date_temp_stats[0].avg_temp
    }
    return jsonify(temp_stats_dict)


if __name__ == '__main__':
    app.run(debug=True)
