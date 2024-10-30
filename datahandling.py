from datetime import datetime
from math import atan2, cos, radians, sin, sqrt
import os
from DbConnector import DbConnector

# Paths to dataset
DATA_PATH = "/your_data_path"
LABELS_PATH = "/your_labels_path"

class GeolifeMongoDBProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db = self.connection.db
        self.user_collection = self.db['User']
        self.activity_collection = self.db['Activity']
        self.trackpoint_collection = self.db['TrackPoint']

    def drop_collections(self):
        self.user_collection.drop()
        self.activity_collection.drop()
        self.trackpoint_collection.drop()
        print("Collections dropped.")

    def insert_users(self):
        users = []
        with open(LABELS_PATH, 'r') as f:
            labeled_users = set(line.strip() for line in f.readlines())

        for user_id in os.listdir(DATA_PATH):
            has_labels = user_id in labeled_users
            users.append({"_id": user_id, "has_labels": has_labels})
        self.user_collection.insert_many(users)
        print("Inserted all users.")

    def insert_activities_and_trackpoints(self):
        for user_id in os.listdir(DATA_PATH):
            user_folder = os.path.join(DATA_PATH, user_id, 'Trajectory')
            labels_file = os.path.join(DATA_PATH, user_id, 'labels.txt')

            labels = self.read_labels_file(labels_file) if os.path.exists(labels_file) else {}

            if os.path.isdir(user_folder):
                for plt_file in os.listdir(user_folder):
                    if plt_file.endswith('.plt'):
                        file_path = os.path.join(user_folder, plt_file)
                        self.process_plt_file(file_path, user_id, labels)

    def read_labels_file(self, labels_file):
        labels = {}
        with open(labels_file, 'r') as f:
            next(f)
            for line in f:
                start_time, end_time, mode = line.strip().split('\t')
                start_time = datetime.strptime(start_time, '%Y/%m/%d %H:%M:%S')
                end_time = datetime.strptime(end_time, '%Y/%m/%d %H:%M:%S')
                labels[start_time] = (end_time, mode)
        return labels

    def process_plt_file(self, file_path, user_id, labels):
        with open(file_path, 'r') as f:
            lines = f.readlines()[6:]  
            if len(lines) > 2500:
                return

            start_line = lines[0].strip().split(',')
            end_line = lines[-1].strip().split(',')

            start_date_time = datetime.strptime(f"{start_line[5]} {start_line[6]}", '%Y-%m-%d %H:%M:%S')
            end_date_time = datetime.strptime(f"{end_line[5]} {end_line[6]}", '%Y-%m-%d %H:%M:%S')

            label_entry = labels.get(start_date_time, (None, None))
            transportation_mode = label_entry[1] if label_entry and len(label_entry) > 1 else None


            total_distance = 0.0
            altitude_gain = 0
            is_valid = True
            previous_point = None

            trackpoints = []
            for line in lines:
                lat, lon, _, altitude, _, date, time = line.strip().split(',')
                altitude = float(altitude)

                date_time = datetime.strptime(f"{date} {time}", '%Y-%m-%d %H:%M:%S')
                current_point = {"lat": float(lat), "lon": float(lon), "altitude": altitude, "date_time": date_time}

                if previous_point:
                    time_diff = (date_time - previous_point["date_time"]).total_seconds() / 60
                    if time_diff >= 5:
                        is_valid = False

                    distance = distance = self.calculate_distance(
                        previous_point["lat"], previous_point["lon"],
                        current_point["lat"], current_point["lon"]
                    )
                    total_distance += distance
                    altitude_diff = current_point["altitude"] - previous_point["altitude"]
                    if altitude_diff > 0:
                        altitude_gain += altitude_diff

                trackpoints.append({
                    "activity_id": None,
                    "lat": current_point["lat"],
                    "lon": current_point["lon"],
                    "altitude": current_point["altitude"],
                    "date_time": current_point["date_time"]
                })
                previous_point = current_point

            activity = {
                "user_id": user_id,
                "start_date_time": start_date_time,
                "end_date_time": end_date_time,
                "transportation_mode": transportation_mode,
                "total_distance": total_distance,
                "altitude_gain": altitude_gain,
                "is_valid": is_valid
            }
            activity_id = self.activity_collection.insert_one(activity).inserted_id

            for tp in trackpoints:
                tp["activity_id"] = activity_id
            self.trackpoint_collection.insert_many(trackpoints)

    # Define calculate_distance to accept lat/lon pairs:
    def calculate_distance(self, lat1, lon1, lat2, lon2):
        R = 6371.0  # Earth radius in kilometers
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        return R * c


    def close_connection(self):
        self.connection.close_connection()


def main():
    program = GeolifeMongoDBProgram()
    program.drop_collections()
    program.insert_users()
    program.insert_activities_and_trackpoints()
    program.close_connection()

if __name__ == '__main__':
    main()
