from flask import Flask, jsonify
from sqlalchemy import create_engine, func
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
import datetime as dt

app = Flask(__name__)

# Database setup
DATABASE_URI = "sqlite:///Resources/hawaii.sqlite"

# Connect to the database
engine = create_engine(DATABASE_URI, echo=False)
Base = automap_base()

# Reflect the database schema to the ORM classes
Base.prepare(engine, reflect=True)

# Map the classes to the database tables
Measurement = Base.classes.measurement
Station = Base.classes.station

# Utility function to validate the format of the input date
def validate_date(date_str):
    try:
        # Convert string to date object
        return dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        # Return None if the format is incorrect
        return None 

@app.route("/")
def home():
    """Display available API endpoints."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/&lt;start&gt;<br/>"
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt;<br/>"
        f"/api/v1.0/latest_date"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    """Retrieve precipitation data for the last year."""
    with Session(engine) as session:
        # Find the latest date in the dataset
        latest_date = session.query(func.max(Measurement.date)).scalar()
        # Calculate the date one year before the latest date
        last_year = dt.datetime.strptime(latest_date, '%Y-%m-%d').date() - dt.timedelta(days=365)
        
        # Query precipitation data for the last year
        precipitation_data = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= last_year).all()
        
        # Convert results into a dictionary and send as JSON
        return jsonify(dict(precipitation_data))

@app.route("/api/v1.0/stations")
def stations():
    """Retrieve all available stations."""
    with Session(engine) as session:
        # Query all stations and their names
        station_list = session.query(Station.station, Station.name).all()
        
        # Convert results into a dictionary and send as JSON
        return jsonify(dict(station_list))

@app.route("/api/v1.0/tobs")
def tobs():
    """Retrieve temperature observations for the most active station over the last year."""
    with Session(engine) as session:
        # Identify the station with the highest number of observations
        most_active_station = session.query(Measurement.station)\
                                    .group_by(Measurement.station)\
                                    .order_by(func.count(Measurement.station).desc())\
                                    .first()[0]
        
        # Find the latest date in the dataset
        latest_date = session.query(func.max(Measurement.date)).scalar()
        # Calculate the date one year before the latest date
        last_year = dt.datetime.strptime(latest_date, '%Y-%m-%d').date() - dt.timedelta(days=365)
        
        # Query temperature observations for the most active station over the last year
        temp_data = session.query(Measurement.date, Measurement.tobs)\
                           .filter(Measurement.station == most_active_station)\
                           .filter(Measurement.date >= last_year).all()
        
        # Convert results into a dictionary and send as JSON
        return jsonify(dict(temp_data))

@app.route("/api/v1.0/<start>", methods=['GET'])
@app.route("/api/v1.0/<start>/<end>", methods=['GET'])
def start_end(start, end=None):
    """Retrieve minimum, average, and maximum temperatures for a given date range."""
    
    # Validate the start and end dates using the utility function
    start_date = validate_date(start)
    end_date = validate_date(end) if end else None
    
    # If the start date is invalid, return an error message
    if not start_date:
        return jsonify({"error": "Please provide a valid start date format 'YYYY-MM-DD'."})
    
    # If provided, validate the end date
    if end and not end_date:
        return jsonify({"error": "Please provide a valid end date format 'YYYY-MM-DD'."})

    with Session(engine) as session:
        # Query the minimum, average, and maximum temperatures based on the provided dates
        if end_date:
            results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs))\
                             .filter(Measurement.date >= start_date)\
                             .filter(Measurement.date <= end_date).all()
        else:
            results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs))\
                             .filter(Measurement.date >= start_date).all()
        
        # If no results are found, return an error message
        if not results or not results[0] or results[0][0] is None:
            return jsonify({"error": "No temperature data found for the given date range."})

    # Organize results into a dictionary and send as JSON
    temp_data = {"TMIN": results[0][0], "TAVG": results[0][1], "TMAX": results[0][2]}
    return jsonify(temp_data)

if __name__ == "__main__":
    # Run the Flask application with debugging enabled
    app.run(debug=True)