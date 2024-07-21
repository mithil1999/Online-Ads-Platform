# importing System dependencies and required flask modules
import sys
import mysql.connector
import datetime
from datetime import datetime as dt

class Slot_Budget_Manager:
    def __init__(self,database_host,database_username, database_password,database_name):
        #'Initialize MySQL database connection'
        self.db = mysql.connector.connect(
            host=database_host,user=database_username,password=database_password,database=database_name
            )
    # Fetching all active campaigns
    def Fetch_active_ad_campaign(self):

        # Get the db cursor
        db_cursor = self.db.cursor()

        # Retrieve list of active ads from database
        sql = "select * from ads where status = 'active'"
        db_cursor.execute(sql)
        self.activeAdsList = db_cursor.fetchall()

    # Re-Calculation slot budget based on available slots from current date to end range of the specific campign
    def Slot_Budget_Calculation(self):
        Slotslist = []
        self.current_slot_budget = 0

        if len(self.singleAdInfo) != 0:
            start = dt.strptime(str(self.singleAdInfo[18])+" "+str(self.singleAdInfo[20]), "%Y-%m-%d %H:%M:%S")
            end = dt.strptime(str(self.singleAdInfo[19])+" "+str(self.singleAdInfo[21]), "%Y-%m-%d %H:%M:%S")
            currentDateTime = datetime.datetime.now().replace(microsecond=0)

            if start < currentDateTime:
                start = currentDateTime

            while start <= end:
                Slotslist.append(start)
                start += datetime.timedelta(minutes=10)
            if len(Slotslist) != 0:
                self.current_slot_budget = (float(self.singleAdInfo[16])/len(Slotslist))

                return self.current_slot_budget

    # Updating the Current Slot budget amount in the Ads Table
    def Update_new_slot_budget(self):

        # Get the db cursor
        db_cursor = self.db.cursor()

        #Sql update statement
        sql = "update ads set currentSlotBudget = '" + str(self.current_slot_budget) +"' where campaignID = '"+ str(self.singleAdInfo[3]) +"'"
        db_cursor.execute(sql)
        self.db.commit()

    # Current Slot budget amount
    def Re_CalculateSlotBudget(self):
        for perAd in self.activeAdsList:
            self.singleAdInfo = perAd
            self.Slot_Budget_Calculation()
            if len(self.singleAdInfo) != 0 :
                self.Update_new_slot_budget()

    def Re_DistributeBudget(self):
        self.Fetch_active_ad_campaign()
        self.Re_CalculateSlotBudget()

    def __del__(self):
        # Cleanup database connection before termination
        self.db.close()

if __name__ == "__main__":

    # Validate Command line arguments
    if len(sys.argv) != 5:
        print("Usage: <database_host> <database_username> <database_password> <database_name>")
        exit(-1)

    database_host = sys.argv[1]
    database_username = sys.argv[2]
    database_password = sys.argv[3]
    database_name = sys.argv[4]

    print(datetime.datetime.now())

    try:
        slot_budget = Slot_Budget_Manager(database_host,database_username,database_password,database_name)
        slot_budget.Re_DistributeBudget()

    except KeyboardInterrupt:
        print("press control-c again to quit")

    finally:
        if slot_budget is not None:
            del slot_budget