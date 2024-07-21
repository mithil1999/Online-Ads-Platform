import requests
import mysql.connector
from time import sleep
import random
import sys
import math

# Utility method for getting state for a city
def get_state_from_city(city):
    state_map = {"Mumbai": "Maharashtra", "Delhi": "Delhi", "Kolkata": "West Bengal", "Bangalore": "Karnataka",
                 "Chennai": "Tamil Nadu", "Hyderabad": "Telangana", "Ahmedabad": "Gujrat", "Pune": "Maharashtra",
                 "Surat": "Gujrat", "Jaipur": "Rajasthan", "Lucknow": "Uttar Pradesh", "Vadodra": "Gujrat",
                 "Nagpur": "Maharashtra", "Coimbatore": "Tamil Nadu", "Indore": "Madhya pradesh", "Ludhiana": "Punjab",
                 "Patna": "Bihar"}
    return state_map[city]


class UserSimulator:

    def __init__(self, database_host, database_username, database_password, database_name, protocol, ad_server_host,
                 ad_server_port, feedback_handler_host, feedback_handler_port):

        # Initialize database connection
        db = mysql.connector.connect(
            host=database_host,
            user=database_username,
            password=database_password,
            database=database_name
        )
        db_cursor = db.cursor()

        # Retrieve list of users from database
        db_cursor.execute("SELECT id, device_type, age FROM users")
        users = db_cursor.fetchall()

        # Close database connection
        db.close()

        # Set the parameter for API calls
        self.protocol = protocol
        self.ad_server_host = ad_server_host
        self.ad_server_port = ad_server_port
        self.feedback_handler_host = feedback_handler_host
        self.feedback_handler_port = feedback_handler_port

        # Variables for mocking user requests
        self.user_ages = []
        self.user_device_type_map = {}
        self.city_dist = {"Mumbai": 1640, "Delhi": 1250, "Kolkata": 627, "Bangalore": 851, "Chennai": 558,
                          "Hyderabad": 514,
                          "Ahmedabad": 372, "Pune": 362, "Surat": 297, "Jaipur": 235, "Lucknow": 195, "Vadodra": 185,
                          "Nagpur": 162,
                          "Coimbatore": 146, "Indore": 123, "Ludhiana": 123, "Patna": 120}

        self.age_bucket_based_user_action_dist = [{1: 4, 0: 256}, {1: 48, 0: 256}, {1: 36, 0: 256}, {1: 28, 0: 256},
                                                  {1: 24, 0: 256}, {1: 20, 0: 256}, {1: 8, 0: 256}, {1: 16, 0: 256},
                                                  {1: 4, 0: 256}, {1: 4, 0: 256}, {1: 4, 0: 256}, {1: 4, 0: 256}]
        self.click_acquisition_dist = {1: 16, 0: 256}
        self.privacy_enable_dist = {True: 5, False: 95}
        self.device_type_dist = {}

        for user in users:
            self.user_ages.append({'id': user[0], 'age': user[2]})
            self.user_device_type_map[user[0]] = user[1]
            if user[1] not in self.device_type_dist:
                self.device_type_dist[user[1]] = 1
            else:
                current_count = self.device_type_dist[user[1]]
                self.device_type_dist[user[1]] = current_count + 1

    # Utility method for simulating user requests
    def simulate_users(self):
        while True:
            try:
                # Determine the value of privacy enabled flag, if this flag is enabled,
                # then dummy user identifier will be sent in the request
                privacy_enabled = random.choices(list(self.privacy_enable_dist.keys()),
                                                 weights=list(self.privacy_enable_dist.values()), k=1)[0]
                user_id = '1111-1111-1111-1111'
                device_type = random.choices(list(self.device_type_dist.keys()),
                                             weights=list(self.device_type_dist.values()), k=1)[0]
                age_bucket = 0

                # If the flag is disabled, populate user_id and device type accordingly
                if not privacy_enabled:
                    user_age_obj = random.choices(self.user_ages, k=1)[0]
                    user_id = user_age_obj['id']
                    age_bucket = math.floor(float(user_age_obj['age'])/10)
                    device_type = self.user_device_type_map[user_id]

                # Populate location attributes
                city = random.choices(list(self.city_dist.keys()), weights=list(self.city_dist.values()), k=1)[0]
                state = get_state_from_city(city)

                # Populate Ad serving URL
                ad_server_url = self.protocol + "://" + self.ad_server_host + ":" + self.ad_server_port
                get_ads_url = ad_server_url + "/ad/user/" + user_id + "/serve?device_type=" + device_type + "&city=" \
                              + city + "&state=" + state
                print(get_ads_url)

                response_json = {}

                # Hit Ad Server API
                response = requests.get(get_ads_url)
                status_code = response.status_code
                if status_code == 200:
                    response_json = response.json()

                # CLose the connection
                response.close()

                # In case of valid Ad server response
                if status_code == 200 and "request_id" in response_json:

                    # Mock the user interaction feedback attributes
                    request_id = response_json['request_id']
                    acquisition = random.choices(list(self.age_bucket_based_user_action_dist[age_bucket].keys()),
                                                 weights=list(self.age_bucket_based_user_action_dist[age_bucket].values()), k=1)[0]
                    click = 1
                    if acquisition == 0:
                        click = random.choices(list(self.age_bucket_based_user_action_dist[age_bucket].keys()),
                                               weights=list(self.age_bucket_based_user_action_dist[age_bucket].values()), k=1)[0]

                    feedback = {
                        "view": 1,
                        "click": click,
                        "acquisition": acquisition
                    }

                    # Populate URL for User interaction feedback
                    feedback_handler_url = self.protocol + "://" + self.feedback_handler_host + ":" + \
                                           self.feedback_handler_port
                    post_feedback_url = feedback_handler_url + "/ad/" + request_id + "/feedback"
                    print(post_feedback_url)

                    # Hit Feedback Submission API
                    resp = requests.post(post_feedback_url, json=feedback)
                    resp.close()
            except:
                pass

            sleep(random.uniform(5, 12))


if __name__ == '__main__':

    # Validate Command line arguments
    if len(sys.argv) != 10:
        print("Usage: python user_simulator.py <database_host> <database_username> <database_password> <database_name>"
              " <protocol> <ad_server_host> <ad_server_port> <feedback_handler_host> <feedback_handler_port>")
        exit(-1)

    # Read the command line parameters
    database_host = sys.argv[1]
    database_username = sys.argv[2]
    database_password = sys.argv[3]
    database_name = sys.argv[4]
    protocol = sys.argv[5]
    ad_server_host = sys.argv[6]
    ad_server_port = sys.argv[7]
    feedback_handler_host = sys.argv[8]
    feedback_handler_port = sys.argv[9]

    user_simulator = UserSimulator(database_host, database_username, database_password, database_name, protocol,
                                   ad_server_host, ad_server_port, feedback_handler_host, feedback_handler_port)
    user_simulator.simulate_users()
