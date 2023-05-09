import pika
import pickle
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost')) #Establishes a connection to the local RabbitMQ server
channel = connection.channel()
channel.queue_declare(queue = 'letterbox')
method,header,body = channel.basic_get(queue = 'letterbox',auto_ack = True) #Recieves the message in the server (if it exists), otherwise it returns a "no message at the moment" message
if method is None:
    print('No messages at the moment')
else:
    message = pickle.loads(body) #Decrypts the message back to the a list form
    purchase_sum = 0 
    for x in message:
        purchase_sum = purchase_sum + x[4] #Calculating the amount of money the Customer had spent with a for loop
    print("The Customer " + message[0][1] + " " + message[0][2] + " Has spent " + str(purchase_sum) + "$") #Final Output
#channel.basic_consume(queue = 'letterbox',on_message_callback=message_recieved,auto_ack = True)
