# TDT4225 Group 35, Assignment 3 
In this project we are working with the Geolife GPS Trajectory dataset in MongoDB. The main goal is to get insights on user movement, such as transportation modes, distances covered, and activity trends, using MongoDB's flexible schema capabilities. The project includes modules to process raw GPS data, insert it into the database, and run analytical queries on the dataset.
As setup we have recieved access to a virtual machine at IDI’s cluster.
 

## Project Structure

The project consists of four main components:
1. **DbConnector.py**: Establishes the MongoDB connection.
2. **datahandling.py**: Manages data processing and insertion into the MongoDB collections.
3. **example.py**: Examples of creating collections, inserting documents, and retrieving data from MongoDB.
4. **queries.py**: Contains queries to analyze user data, such as counting activities, finding top transportation modes, and detecting users near significant landmarks.

## Requirements

To run this project, you need:
- MongoDB database setup
- Python 3.x
- The following Python packages, listed in `requirements.txt`:
  ```plaintext
  pymongo==4.10.1
  tabulate==0.9.0
  haversine==2.8.0
  ```

Install the dependencies using:
```bash
pip install -r requirements.txt
```

## Running the Code

1. **Set Up Database**:
   - Adjust the database credentials (`HOST`, `USER`, `PASSWORD`) in `DbConnector.py` to match your MongoDB setup.

2. **Data Processing and Insertion**:
   - The main script in `datahandling.py` processes user data and inserts it into MongoDB. Modify `DATA_PATH` and `LABELS_PATH` to point to the correct dataset directory.

   Run:
   ```bash
   python datahandling.py
   ```

3. **Querying the Data**:
   - Use `queries.py` to analyze the data after insertion. This script includes various tasks, such as counting user activities, calculating distances, and analyzing transportation modes.

   Run:
   ```bash
   python queries.py
   ```



## Project Overview

This project analyzes GPS trajectory data from the Geolife dataset using MongoDB to store and query high-dimensional movement data. It leverages MongoDB’s NoSQL capabilities to handle large datasets, making it ideal for complex geospatial and temporal analysis. Key objectives are:
- **Processing and Cleaning Data**: Ingesting and organizing the raw trajectory and label files.
- **Data Insertion**: Structuring data into `User`, `Activity`, and `TrackPoint` collections.
- **Data Analysis**: Using MongoDB aggregation queries for insights, like identifying common transportation modes, tracking distance covered by users, and spotting users in specific locations like the Forbidden City.


