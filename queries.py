from datetime import datetime
from DbConnector import DbConnector;
from math import radians, sin, cos, sqrt, atan2


class Queries:    
    def __init__(self):
        self.connection = DbConnector()
        self.db = self.connection.db
        
    def calculate_distance(self, lat1, lon1, lat2, lon2):
        R = 6371.0  # Radius of Earth in kilometers
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        return R * c

    # task 2.1
    def count_entries(self):
        user_count = self.db['User'].count_documents({})
        print(f"Total Users: {user_count}")

        activity_count = self.db['Activity'].count_documents({})
        print(f"Total Activities: {activity_count}")

        trackpoint_count = self.db['TrackPoint'].count_documents({})
        print(f"Total TrackPoints: {trackpoint_count}")
        
    # task 2.2
    def average_activities_per_user(self):
        pipeline = [
            {
                "$group": {
                    "_id": "$user_id",
                    "activity_count": {"$sum": 1}
                }
            },
            {
                "$group": {
                    "_id": None,  # Single document result
                    "avg_activities_per_user": {"$avg": "$activity_count"}  # Calculate the average
                }
            }
        ]
        
        result = list(self.db["Activity"].aggregate(pipeline))
        
        if result:
            avg_activities = result[0]['avg_activities_per_user']
            print(f"Average number of activities per user: {avg_activities:.2f}")
        else:
            print("No activities found.")
    
    # task 2.3
    def top_20_users_with_highest_activities(self):
        pipeline = [
            {
                '$group': {
                    '_id': '$user_id',
                    'activity_count': {'$sum': 1}
                }
            },
            {
                '$sort': {'activity_count': -1}
            },
            {
                '$limit': 20
            }
        ]
        result = self.db['Activity'].aggregate(pipeline)
        print("Top 20 users with the highest number of activities:")
        for row in result:
            print(f"User ID: {row['_id']}, Activities: {row['activity_count']}")

    # task 2.4
    def users_taken_taxi(self):
        result = self.db['Activity'].distinct("user_id", {"transportation_mode": "taxi"})
        
        print("Users who have taken a taxi:")
        for user in result:
            print(f"User ID: {user}")
    
    # task 2.5
    def count_transportation_modes(self):
        pipeline = [
            {"$match": {"transportation_mode": {"$ne": None}}},  # Exclude null values
            {"$group": {"_id": "$transportation_mode", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}  # Sort by count in descending order
        ]
        
        result = self.db['Activity'].aggregate(pipeline)
        
        print("Transportation modes and their activity counts:")
        for doc in result:
            print(f"Mode: {doc['_id']}, Activities: {doc['count']}")
    
    # task 2.6a)
    def year_with_most_activities(self):
        pipeline = [
            {"$group": {"_id": {"$year": "$start_date_time"}, "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ]
        
        result = self.db['Activity'].aggregate(pipeline)
        
        for doc in result:
            print(f"Year with the most activities: {doc['_id']} ({doc['count']} activities)")
        
    # task b)
    def year_with_most_hours(self):
        pipeline = [
            {"$group": {
                "_id": {"$year": "$start_date_time"},
                "total_hours": {
                    "$sum": {
                        "$divide": [
                            {"$subtract": ["$end_date_time", "$start_date_time"]},  # Difference in milliseconds
                            1000 * 60 * 60  # Convert to hours
                        ]
                    }
                }
            }},
            {"$sort": {"total_hours": -1}},
            {"$limit": 1}
        ]
        
        result = self.db['Activity'].aggregate(pipeline)
        
        for doc in result:
            print(f"Year with the most recorded hours: {doc['_id']} ({doc['total_hours']:.2f} hours)")    
    
    # task 2.7
    def total_distance_walked_2008(self):
        print("Retrieving walking activities for user 112 in 2008...")

        activity_docs = self.db["Activity"].find(
            {
                "user_id": "112",
                "transportation_mode": "walk",
                "start_date_time": {
                    "$gte": datetime(2008, 1, 1),  
                    "$lt": datetime(2009, 1, 1)
                }
            },
            {"_id": 1}
        )

        activity_ids = [activity["_id"] for activity in activity_docs]
        print(f"Found {len(activity_ids)} walking activities for user 112 in 2008.")

        total_distance = 0.0

        for activity_id in activity_ids:
            trackpoints = list(self.db["TrackPoint"].find(
                {"activity_id": activity_id},
                sort=[("date_time", 1)]
            ))

            if len(trackpoints) < 2:
                print(f"Not enough trackpoints to calculate distance for activity {activity_id}.")
                continue
            
            for i in range(1, len(trackpoints)):
                lat1, lon1 = trackpoints[i - 1]["lat"], trackpoints[i - 1]["lon"]
                lat2, lon2 = trackpoints[i]["lat"], trackpoints[i]["lon"]
                distance = self.calculate_distance(lat1, lon1, lat2, lon2)
                total_distance += distance

        print(f"Total distance walked in 2008 by user 112: {total_distance:.2f} km")

    
    # task 2.8
    def top_20_altitude_gains(self):
        # Dictionary to store total altitude gain for each user
        altitude_gain_by_user = {}

        for user in self.db["User"].find():
            user_id = user["_id"]
            total_altitude_gain = 0.0

            activities = list(self.db["Activity"].find({"user_id": user_id}))
            
            # Debug: Print number of activities for the user
            print(f"Processing User {user_id}, Number of activities: {len(activities)}")
            
            for activity in activities:
                activity_id = activity["_id"]

                trackpoints = list(self.db["TrackPoint"].find({"activity_id": activity_id, "altitude": {"$gt": -777}}).sort("date_time", 1))
                
                
                for i in range(1, len(trackpoints)):
                    previous_altitude = trackpoints[i - 1]["altitude"]
                    current_altitude = trackpoints[i]["altitude"]

                    if current_altitude > previous_altitude:
                        total_altitude_gain += (current_altitude - previous_altitude)

            altitude_gain_by_user[user_id] = total_altitude_gain

        top_users = sorted(altitude_gain_by_user.items(), key=lambda x: x[1], reverse=True)[:20]

        # Print results
        print("Top 20 users with the highest altitude gains:")
        for user_id, altitude_gain in top_users:
            print(f"User ID: {user_id}, Altitude Gain: {altitude_gain:.2f} meters")


    # task 2.9
    def find_invalid_activities(self):
        print("Finding users with invalid activities...")

        self.db["TrackPoint"].create_index("activity_id")
        self.db["Activity"].create_index("user_id")

        users_with_labels = self.db["User"].distinct("_id", {"has_labels": True})

        pipeline = [
            {
                "$match": {
                    "user_id": {"$in": users_with_labels}  
                }
            },
            {
                "$lookup": {
                    "from": "TrackPoint", 
                    "localField": "_id",  
                    "foreignField": "activity_id",
                    "as": "trackpoints"
                }
            },
            {
                "$match": {
                    "trackpoints.1": {"$exists": True}  # Only activities with at least 2 trackpoints
                }
            },
            {
                "$project": {
                    "user_id": 1,
                    "trackpoints.date_time": 1  
                }
            }
        ]

        activities = list(self.db["Activity"].aggregate(pipeline))
        
        if not activities:
            print("No activities found with the specified criteria.")
            return {}

        # Dictionary to store invalid activity count per user
        invalid_activities_per_user = {}

        for activity in activities:
            user_id = activity['user_id']
            trackpoints = activity['trackpoints']

            trackpoints = sorted(trackpoints, key=lambda tp: tp['date_time'])

            trackpoint_times = [tp['date_time'] for tp in trackpoints if 'date_time' in tp]
            
            # Check if any consecutive trackpoints have a time deviation of >= 5 minutes
            is_invalid = any(
                (trackpoint_times[i] - trackpoint_times[i - 1]).total_seconds() >= 300
                for i in range(1, len(trackpoint_times))
            )

            if is_invalid:
                invalid_activities_per_user[user_id] = invalid_activities_per_user.get(user_id, 0) + 1

        sorted_invalid_activities = sorted(invalid_activities_per_user.items())

        print("Users with invalid activities (time deviation >= 5 minutes):")
        for user_id, invalid_count in sorted_invalid_activities:
            print(f"User ID: {user_id}, Invalid Activities: {invalid_count}")

    # task 2.10
    def find_users_in_forbidden_city(self):
        forbidden_city_lat = 39.916
        forbidden_city_lon = 116.397
        threshold = 0.005  # Small threshold to account for proximity around the Forbidden City

        pipeline = [
            {
                "$match": {
                    "$and": [
                        {"lat": {"$gt": forbidden_city_lat - threshold, "$lt": forbidden_city_lat + threshold}},
                        {"lon": {"$gt": forbidden_city_lon - threshold, "$lt": forbidden_city_lon + threshold}}
                    ]
                }
            },
            {
                "$lookup": {
                    "from": "Activity",
                    "localField": "activity_id",
                    "foreignField": "_id",
                    "as": "activity"
                }
            },
            {
                "$unwind": "$activity"
            },
            {
                "$group": {
                    "_id": "$activity.user_id"
                }
            }
        ]

        result = self.db['TrackPoint'].aggregate(pipeline)

        print("Users who have tracked an activity in the Forbidden City:")
        for doc in result:
            print(f"User ID: {doc['_id']}")
    
    #task 2.11
    def most_used_transport_mode(self):
        pipeline = [
            {
                "$match": {
                    "transportation_mode": {"$ne": None}
                }
            },
            {
                "$group": {
                    "_id": {
                        "user_id": "$user_id", 
                        "transportation_mode": "$transportation_mode"
                    },
                    "count": {"$sum": 1}  
                }
            },
            {
                "$sort": {
                    "_id.user_id": 1,  
                    "count": -1  
                }
            },
            {
                "$group": {
                    "_id": "$_id.user_id",
                    "most_used_mode": {
                        "$first": "$_id.transportation_mode"  
                    }
                }
            },
            {
                "$sort": {
                    "_id": 1  
                }
            }
        ]

        result = self.db['Activity'].aggregate(pipeline)

        print("User and their most used transportation mode:")
        for doc in result:
            print(f"User ID: {doc['_id']}, Most Used Mode: {doc['most_used_mode']}")


    def close_connection(self):
        self.connection.close_connection()


def main():
    program = None
    try:
        program = Queries()
        
        print("Task 2.1:")
        program.count_entries()
        print("\nTask 2.2:")
        program.average_activities_per_user()
        print("\nTask 2.3:")
        program.top_20_users_with_highest_activities()
        print("\nTask 2.4:")
        program.users_taken_taxi()
        print("\nTask 2.5:")
        program.count_transportation_modes()
        print("\nTask 2.6 a):")
        program.year_with_most_activities()
        print("\nTask 2.6 b)")
        program.year_with_most_hours()
        print("\nTask 2.7:")
        program.total_distance_walked_2008()
        print("\nTask 2.8:")
        program.top_20_altitude_gains()
        print("\nTask 2.9:")
        program.find_invalid_activities()
        print("\nTask 2.10:")
        program.find_users_in_forbidden_city()
        print("\nTask 2.11:")
        program.most_used_transport_mode()
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.close_connection()


if __name__ == '__main__':
    main()