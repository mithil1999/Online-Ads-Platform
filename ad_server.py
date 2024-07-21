# Importing necessary modules and libraries for the ad_server
import uuid
from flask import abort
import mysql.connector
import datetime
import sys
import flask
from flask import request, jsonify

# Creating a class for Ad_Server with all the methods in it
class Ad_Server:
    # Now we need to initialize the database with hosy, username, password abd db name
    def __init__(self,database_host,database_username,database_password,database_name):
        #initialize the connection
        self.db = mysql.connector.connect(host=database_host,user=database_username,password=database_password,database=database_name)

    #In order to fetch the ads details we create a funtion
    def fetch_ad_details(self):
        # We have to initialize the db_cursor in order to input the sql queries and values and execute them
        db_cursor = self.db.cursor()

        sql_fetch_auctionCost = "select cpm,cpc,cpa from ( select * from ads "
        sql_fetch_auctionWin = "select *  from ( select * from ads "

        # To implement some conditions on the above sql queries like ad_status = "active" and specific timeperiod create a condition variable
        condition_Statement = ("where status = 'active'"
        " and  CURRENT_TIMESTAMP() between TIMESTAMP(dateRangeStart,timeRangeStart) and TIMESTAMP(dateRangeEnd,timeRangeEnd) ")

        if self.device != "All" :
             condition_Statement += " and targetDevice = '" + self.device  + "' "

        if self.city != "All":
            condition_Statement += " and targetCity = '" + self.city  + "' "

        if self.state != "All":
            condition_Statement += " and targetState = '" + self.state  + "' "

        # Implementing Second higest bidder strategy where winner of auction pays Second higest bidder amount value
        sql_fetch_auctionCost+= condition_Statement + " order by cpm desc limit 2 ) as temp order by cpm limit 1;"
        sql_fetch_auctionWin += condition_Statement + " order by cpm desc limit 2) as temp limit 1;"

        # Execute the auction cost 
        db_cursor.execute(sql_fetch_auctionCost)
        self.DecidedAuctionCost =  db_cursor.fetchall()

        # Execute the ad details of the auction winner
        db_cursor.execute(sql_fetch_auctionWin)
        self.adDetails =  db_cursor.fetchall() 

    # Now that we have our query outputs we have to update the served_ads table 
    def selected_ad_details(self):
        db_cursor = self.db.cursor()
        if len(self.adDetails) != 0 and len(self.DecidedAuctionCost[0]) != 0:
            
            currentTimeStamp = datetime.datetime.now().replace(microsecond=0)
            startDateTime = datetime.datetime.combine(self.adDetails[0][18],(datetime.datetime.min + self.adDetails[0][20]).time())
            endDateTime = datetime.datetime.combine(self.adDetails[0][19],(datetime.datetime.min + self.adDetails[0][21]).time())

            #Now we create the sql statement to insert values in to the served_ads table 
            insert_sql = ("INSERT INTO served_ads("
                    "requestID ,"
                    "campaignID ,"
                    "userID ,"
                    "auctionCPM ,"
                    "auctionCPC ,"
                    "auctionCPA ,"
                    "targetAgeRange ,"
                    "targetLocation ,"
                    "targetGender ,"
                    "targetIncomeBucket ,"
                    "targetDeviceType ,"
                    "campaignStartTime ,"
                    "campaignEndTime ,"
                    "userFeedbackTimeStamp)"
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" 
                    )
            # Values to be inserted
            insert_values = (
                self.request_id,
                self.adDetails[0][3],
                self.user_id,
                self.DecidedAuctionCost[0][0],
                self.DecidedAuctionCost[0][1],
                self.DecidedAuctionCost[0][2],
                str(self.adDetails[0][6]) +"-"+ str(self.adDetails[0][7]),
                self.adDetails[0][8] +", "+ self.adDetails[0][9] +" and "+ self.adDetails[0][10],
                self.adDetails[0][5],
                self.adDetails[0][11],
                self.adDetails[0][12],
                startDateTime,
                endDateTime,
                currentTimeStamp
                )
            
            # Execute the query 
            db_cursor.execute(insert_sql, insert_values)

    # Combine both fetch and selection of ads process into one funtion
    def fetch_sel_ads(self):
        self.fetch_ad_details()
        self.selected_ad_details()

    def __del__(self):
        self.db.close()

if __name__ == "__main__":

    # Arguments validation
    if len(sys.argv) != 5:
        print("Usage: <database_host> <database_username> <database_password> <database_name>")
        exit(-1)

    # providing database_host, database_username, database_password and database_name
    database_host = sys.argv[1]
    database_username = sys.argv[2]
    database_password = sys.argv[3]
    database_name = sys.argv[4]

    try:
        ad_server_object = Ad_Server(database_host,database_username,database_password,database_name)

        # Basic Flask COnfiguration
        app = flask.Flask(__name__)
        app.config["DEBUG"] = True;
        app.config["RESTFUL_JSON"] = {"ensure_ascii":False}

        # Http Get request  processing
        @app.route('/ad/user/<user_id>/serve', methods=['GET'])
        def serve(user_id):
            # 
            if "state" not in request.args:
                return abort(400)

            if "city" not in request.args:
                 return abort(400)
            
            if "device_type" not in request.args:
                return abort(400)

            # Generate the request identifier
            request_id = str(uuid.uuid1())

            # Assigning values to Ad_Server Class object
            ad_server_object.user_id = user_id
            ad_server_object.request_id = request_id
            ad_server_object.state = request.args["state"]
            ad_server_object.city = request.args["city"]
            ad_server_object.device = request.args["device_type"]

            # Calling wrapped method to fetch and select the ads
            ad_server_object.fetch_sel_ads()
            
            # Validating the output
            if len(ad_server_object.adDetails) != 0:
                return jsonify({
                    "request_id": request_id,
                    "text": ad_server_object.adDetails[0][0]
                    })

            return jsonify({
                "status": "success",
                "request_id": request_id,
                "Sorry": "No ad to serve"
                })

        # Providing the host and the port values
        app.run(host="0.0.0.0", port=5000)

    except KeyboardInterrupt:
        print("press control-c again to quit")

    finally:
        if ad_server_object is not None:
            del ad_server_object 





