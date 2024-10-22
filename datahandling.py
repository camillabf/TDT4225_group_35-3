import os
from datetime import datetime
from DbConnector import DbConnector
import pandas as pd

# Paths to dataset
DATA_PATH = "/Users/Magnusvik/Documents/NTNU 3/Data_semester 7/Store datamengder/dataset/dataset/Data/"
LABELS_PATH = "/Users/Magnusvik/Documents/NTNU 3/Data_semester 7/Store datamengder/dataset/dataset/labeled_ids.txt"


class GeolifeMongoDBProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db = self.connection.db
        self.user_collection = self.db['User']
        self.activity_collection = self.db['Activity']
        self.trackpoint_collection = self.db['TrackPoint']

    # Create the collections
    def create_collections(self):
        print("Collections will be created upon data insertion.")

    # Insert users into the 'User' collection
    def insert_users(self):
        users = []
        with open(LABELS_PATH, 'r') as f:
            labeled_users = [line.strip() for line in f.readlines()]
        
        for user_id in os.listdir(DATA_PATH):
            has_labels = user_id in labeled_users
            users.append({"_id": user_id, "has_labels": has_labels})
        
        self.user_collection.insert_many(users)
        print("Inserted all users.")

    # Insert activities and trackpoints
    def insert_activities_and_trackpoints(self):
        for user_id in os.listdir(DATA_PATH):
            user_folder = os.path.join(DATA_PATH, user_id, 'Trajectory')
            labels_file = os.path.join(DATA_PATH, user_id, 'labels.txt')

            # Read labels if they exist
            labels = {}
            if os.path.exists(labels_file):
                labels = self.read_labels_file(labels_file)

            if os.path.isdir(user_folder):
                for plt_file in os.listdir(user_folder):
                    if plt_file.endswith('.plt'):
                        file_path = os.path.join(user_folder, plt_file)
                        self.process_plt_file(file_path, user_id, labels)

    # Helper to read labels.txt
    def read_labels_file(self, labels_file):
        labels = {}
        with open(labels_file, 'r') as f:
            next(f)  # Skip header
            for line in f:
                start_time, end_time, mode = line.strip().split('\t')
                start_time = datetime.strptime(start_time, '%Y/%m/%d %H:%M:%S')
                end_time = datetime.strptime(end_time, '%Y/%m/%d %H:%M:%S')
                labels[start_time] = (end_time, mode)
        return labels

    # Helper to process .plt files and insert activities and trackpoints
    def process_plt_file(self, file_path, user_id, labels):
        try:
            # Read .plt file
            with open(file_path, 'r') as f:
                lines = f.readlines()[6:]  # Skip first 6 header lines
                if len(lines) > 2500:  # Skip if there are more than 2500 trackpoints
                    return

                # Extract start and end time from the file
                start_line = lines[0].strip().split(',')
                end_line = lines[-1].strip().split(',')

                start_date_time = datetime.strptime(f"{start_line[5]} {start_line[6]}", '%Y-%m-%d %H:%M:%S')
                end_date_time = datetime.strptime(f"{end_line[5]} {end_line[6]}", '%Y-%m-%d %H:%M:%S')

                # Match with transportation mode (if available)
                transportation_mode = None
                if start_date_time in labels and labels[start_date_time][0] == end_date_time:
                    transportation_mode = labels[start_date_time][1]

                # Insert the activity
                activity = {
                    "user_id": user_id,
                    "start_date_time": start_date_time,
                    "end_date_time": end_date_time,
                    "transportation_mode": transportation_mode
                }
                activity_id = self.activity_collection.insert_one(activity).inserted_id

                # Insert trackpoints in bulk
                trackpoints = []
                for line in lines:
                    lat, lon, _, altitude, _, date, time = line.strip().split(',')
                    date_time = datetime.strptime(f"{date} {time}", '%Y-%m-%d %H:%M:%S')
                    trackpoints.append({
                        "activity_id": activity_id,
                        "lat": float(lat),
                        "lon": float(lon),
                        "altitude": int(altitude),
                        "date_time": date_time
                    })

                self.trackpoint_collection.insert_many(trackpoints)
                print(f"Inserted activity {activity_id} and its trackpoints for user {user_id}.")
        
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")

    def close_connection(self):
        self.connection.close_connection()


def main():
    program = GeolifeMongoDBProgram()
    program.create_collections()
    program.insert_users()
    program.insert_activities_and_trackpoints()
    program.close_connection()


if __name__ == '__main__':
    main()
