# importing System dependencies and required flask modules
import sys
import flask
import mysql.connector
import datetime
import time
import json
from flask import request, jsonify
from flask import abort
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

# FeedbackHandler Class with all neccessary action to be performed within
class FeedbackHandler:
    #Kafka_topic = "user-feedback"
    # Init Method to assign values to the class accessible variables
    def __init__(self,database_host,Kafka_topic,database_username,database_password,database_name):
        # Initializing DB connection variable and kafka topic.
        self.database_host = database_host
        self.Kafka_topic = Kafka_topic
        self.database_username = database_username
        self.database_password = database_password
        self.database_name = database_name

    # Initialize MySQL database connection methods
    def InitializeDBConnection(self):
        self.db = mysql.connector.connect(
            host=self.database_host,user=self.database_username,password=self.database_password,database=self.database_name
            )
    # Closing MySQL database connection methods
    def closeDBConnection(self):
        self.db.close()

    # Method fetch_server_adDetails from served_ads table for further processing
    def fetch_server_adDetails(self):

        # Sql fetch served ads details based on Request ID
        sql = "select * from served_ads where requestID ='" + self.ad_request_id +"';"

        # Initialize MySQL database connection
        self.InitializeDBConnection()

        # Initialize DB cursor
        db_cursor = self.db.cursor()

        # Retrieve auction ad details
        db_cursor.execute(sql)
        
        # Fetching all the details and assigning to the object
        self.servedDetails =  db_cursor.fetchall()

        if len(self.servedDetails) > 0:
            return True

        # terminating DB connection
        self.closeDBConnection()

        return False

    # Method to calculate the dervied data "user_action" and "expenditure"
    def user_interaction(self):
        if int(self.requestData["acquisition"]) == 1:
            return ["acquisition",float(self.servedDetails[0][5])]
        elif int(self.requestData["click"]) == 1:
            return ["click",float(self.servedDetails[0][4])]
        else:
            return ["view",0]
    
    # Method to create a dictionary based on all the data retrived from served_ads and response from user_simulator
    def final_kafka_data(self): 
        self.finaldict = {}
        self.finaldict["request_id"] = self.servedDetails[0][0]
        self.finaldict["campaign_id"] = self.servedDetails[0][1]
        self.finaldict["user_id"] = self.servedDetails[0][2]
        self.finaldict["click"] = self.requestData["click"]
        self.finaldict["view"] = self.requestData["view"]
        self.finaldict["acquisition"] = self.requestData["acquisition"]
        self.finaldict["auction_cpm"] = float(self.servedDetails[0][3])
        self.finaldict["auction_cpc"] = float(self.servedDetails[0][4])
        self.finaldict["auction_cpa"] = float(self.servedDetails[0][5])
        self.finaldict["target_age_range"] = self.servedDetails[0][6]
        self.finaldict["target_Location"] = (self.servedDetails[0][7]).replace(",",";").replace("and",";").replace(" ","")
        self.finaldict["target_gender"] = self.servedDetails[0][8]
        self.finaldict["target_income_bucket"] = self.servedDetails[0][9]
        self.finaldict["target_device_type"] = self.servedDetails[0][10]
        self.finaldict["campaign_start_time"] = str(self.servedDetails[0][11])
        self.finaldict["campaign_end_time"] = str(self.servedDetails[0][12])
        self.finaldict["user_action"] = (self.user_interaction())[0]
        self.finaldict["expenditure"] = (self.user_interaction())[1]
        self.finaldict["timestamp"] = str(self.servedDetails[0][13])

    # Method is to Calculate left over budget and current_slot_budget if expense > 0 update the ads table. 
    def expend_update_in_db(self):
        if self.finaldict["expenditure"] > 0:
            sql = "select budget,currentSlotBudget from ads where campaignID = '"+self.finaldict["campaign_id"]+"';"

            # Initialize DB cursor
            db_cursor = self.db.cursor()

            # Retrieve auction ad details
            db_cursor.execute(sql)
            
            # Fetching all the details and assigning to the object
            budgetData =  db_cursor.fetchall()

            if len(budgetData) > 0:
                budget = float(budgetData[0][0])
                current_slot_budget = float(budgetData[0][1])
                updated_budget = budget - float(self.finaldict["expenditure"])
                updated_current_slot_budget = current_slot_budget - float(self.finaldict["expenditure"])
                
                update_sql = ("update ads"
                            " set budget = "+ str(updated_budget) +","
                            " currentSlotBudget ="+ str(updated_current_slot_budget) + ""
                )

                if updated_budget <= 0:
                    update_sql += ", status ='INACTIVE'"

                update_sql += " where campaignID = '"+self.finaldict["campaign_id"]+"';"

                # Executing the sql statement
                db_cursor.execute(update_sql)

                # commiting changes to the DB
                self.db.commit()

                # terminating DB connection
                self.closeDBConnection()

    def create_KafkaTopic(self, topic):
        bootstrap_servers = ['44.214.172.144:9092']
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        topic_list = []
        topic_list.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
        admin_client.create_topics(new_topics=topic_list, validate_only=False)

    # Method is used to send the data to the kafka server.
    def send_to_KafkaProducer(self):
        bootstrap_servers = ['44.214.172.144:9092']
        topicName = self.Kafka_topic
        #self.create_KafkaTopic(topicName)
        producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
        jsonData = json.dumps(self.finaldict)
        producer.send(topicName, jsonData.encode('utf-8'))


    # Methods to call all the neccessary function is complete the operation
    def feedback_handler_process(self):
        if self.fetch_server_adDetails():
            self.final_kafka_data()
            self.expend_update_in_db()
            self.send_to_KafkaProducer()


if __name__ == "__main__":

    # Validate Command line arguments
    if len(sys.argv) != 6:
        print("Usage: <database_host> <Kafka_topic> <database_username> <database_password> <database_name>")
        exit(-1)

    # Assiging arguments with meaning full names
    database_host = sys.argv[1]
    kafka_Topic = sys.argv[2]
    database_username = sys.argv[3]
    database_password = sys.argv[4]
    database_name = sys.argv[5]

    try:
        feedback_handler = FeedbackHandler(database_host,kafka_Topic,database_username,database_password,database_name)

        # Basic Flask Configuration
        app = flask.Flask(__name__)
        app.config["DEBUG"] = True
        app.config["RESTFUL_JSON"] = {"ensure_ascii":False}

        # Http Get request  processing
        @app.route('/ad/<ad_request_id>/feedback', methods=['POST'])
        def request_handler(ad_request_id):

            # Ad server process initiation
            feedback_handler.ad_request_id = ad_request_id
            print(feedback_handler.ad_request_id)
            feedback_handler.requestData = request.json
            feedback_handler.feedback_handler_process()

            return jsonify({
                    "Success": True
                    })
            

        # Hosting web service at localhost and port 8000 
        app.run(host="0.0.0.0", port=8000)

    except KeyboardInterrupt:
        print("press control-c again to quit")

    finally:
        print("Completed")