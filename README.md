Developed an Ads Platform which works on Real-time Streaming data consumed from Apache Kafka, computed the KPIs using Apache Spark and stored them in a RDBMS, Data Lakes so that data can be furthur consumed for BI analytics.
Architecture of the project
![WhatsApp Image 2024-07-21 at 12 29 45 PM](https://github.com/user-attachments/assets/75ad7b69-136f-4111-97c4-4aa00c9c8bcc)

AIM:
With increasing digitisation, there has been a tremendous boom in the field of online advertising as more and more companies are willing to pay large amounts of money in order to reach the customers via online platform. So, in this project, we will be building an online advertising platform.

TASKS:
1. The platform will have an interface for campaign managers to run the Ad campaign and another interface for the client to present the Ads and send the user action back to the advertising platform.
2. Through the campaign manager, the Ad instructions (New Ad Campaign, Stopping the existing Ad campaign) will be published to a Kafka Queue. The ‘Ad Manager’ will read the message from that Kafka queue and update     the MySQL store accordingly.
3. An Ad Server will hold the auction of Ads for displaying the Ad to a user device. The auction winner will pay the amount bid by the second Ad. A user simulator will hit the Ad Server API for displaying Ads and      send the user interaction feedback back to the feedback handler through API.
4. Upon receiving the user interaction feedback, the feedback handler will publish that event to another Kafka queue.
5. The User feedback handler will collect the feedback through the feedback API and it will be responsible for updating the leftover budget for the Ad campaign into MySQL. It will also publish the feedback to the      internal Kafka queue, which will be later published to HIVE through user feedback writer(Spark Streaming) for billing and archiving.
6. The whole system needs to be run for an hour and after that, the report generator will generate the bill report for all Ads displayed so far.
