import pika
import json
import pickle
import mysql.connector
#Validating the customer ID input
while True:
    try: #If the Customer_ID input is indeed an integer, otherwise it gives out a Error statement and asks the user to repeat the action
        Customer_ID = int(input('What is the Customer ID?\n'))
    except ValueError:
        print("Please enter a valid Customer ID (integer)")
        continue
    break
#The mysql.connector.connect function basically orders the function to log-in to the local MYSQL server on my PC, with the user and password I have created
mydb = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "nickyako",
    database = "chinook"
    )
print(Customer_ID)
mycursor = mydb.cursor() #The Cursor function is used to carry out SQL Queries using MYSQL
mycursor.execute("SELECT customer.customerid,customer.FirstName,customer.LastName,invoice.invoiceid,invoiceline.UnitPrice,invoiceline.Quantity as Quantity_Bought,track.Name as Track_Name from customer inner join invoice on customer.customerid = invoice.customerid inner join invoiceline on invoice.InvoiceID = invoiceline.invoiceID inner join track on track.trackid  = invoiceline.trackid WHERE customer.customerid = " + str(Customer_ID)+";")
############################
##SQL Query Syntax, the Query commits an Inner Join between the 3 relevant Tables
##SELECT customer.customerid,customer.FirstName,customer.LastName,invoice.invoiceid,invoiceline.UnitPrice,invoiceline.Quantity as Quantity_Bought,track.Name as Track_Name from customer
##inner join invoice
##on customer.customerid = invoice.customerid
##inner join invoiceline
##on invoice.InvoiceID = invoiceline.invoiceID
##inner join track
##on track.trackid  = invoiceline.trackid
##WHERE customer.customerid = str(Customer_ID);
############################
column_names = [i[0] for i in mycursor.description] #Gets the Column names for each table for convenience and a more aesthetically pleasing output
print(column_names)
myresult = mycursor.fetchall() #Receives all the outcomes the Query had found and places them all in a list
for x in myresult:
    print(x)
connection_parameters = pika.ConnectionParameters('localhost') #Using the pika library, I connect to the local RabbitMQ server on my PC, firstly receiving all the connection parameters
#the local host server

connection = pika.BlockingConnection(connection_parameters) #This function blocks the server until the message is delievred, in my case this is the better option where as I don't
#need the program to execute any other actions until the message is delivered.

channel = connection.channel() #Creates the connection channel to the server
channel.queue_declare(queue = 'letterbox') #Declares on a Queue

message = pickle.dumps(myresult) #Dumps the result using pickle library, since the input needs to be serialized before entering the server

channel.basic_publish(exchange='',routing_key='letterbox',body = message) #Publishes the message to the server

connection.close() #Closes the connection to the server, ending our program

