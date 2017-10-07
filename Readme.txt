# Example of Worker Queue

- The publisher publishes messages
- Workers picking the messages in round robin fashion

To test:

- Run two instances of the RabbitQueue.Worker
- Run the RabbitQueue.Worker and publish messages as

 RabbitQueue.Publisher "new message"
 

# Example of the RPC client and Server:

- Client is sending message to server and waiting for long running message
- Server process the request and sends the message to "Response" Queue
- Client is reading the message from the "Response" Queue and processing using the CorrelatedId of the message

To test:

Run the RabbitQueue.Server
Run the RabbitQueue.Client with different inputs. 
e.g. 

RabbitQueue.Client 5
RabbitQueue.Client 10
