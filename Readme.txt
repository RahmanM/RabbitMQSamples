Example of Worker Queue

- The publisher publishes messages
- Workers picking the messages in round robin fashion

To test:

- Run two instances of the RabbitQueue.Worker
- Run the RabbitQueue.Worker and publish messages as

 RabbitQueue.Publisher "new message"