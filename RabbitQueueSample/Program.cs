using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitQueueSample
{
    class Program
    {
        private const string ExchangeName = "test-exchange";
        private const string QueueName = "test-direct";

        static void Main(string[] args)
        {
            FireAndForget();

            PubSub();

            Routing();

            MessageWithAcknowlegement(); // doesn't work!!

            StoreObject();

            ReadMessage();

            Console.ReadLine();
        }

        #region Acknoledgement

        private static void MessageWithAcknowlegement()
        {
            SendAckMessage();

            ReadAckMessage();
        }

        private static void SendAckMessage()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                // Note: both Q and Message are durable!!
                channel.QueueDeclare(queue: "task_queue",
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                const string message = "-->Message with acknowlegment!";
                var body = Encoding.UTF8.GetBytes(message);

                var properties = channel.CreateBasicProperties();
                properties.Persistent = true;

                channel.BasicPublish(exchange: "",
                                     routingKey: "task_queue",
                                     basicProperties: properties,
                                     body: body);

                Console.WriteLine(" [x] Sent {0}", message);
            }
        }

        public static void ReadAckMessage()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "task_queue",
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                ///This tells RabbitMQ not to give more than one message to a worker at a time. 
                ///Or, in other words, don't dispatch a new message to a worker until it has processed and acknowledged the previous one. 
                ///Instead, it will dispatch it to the next worker that is not still busy.
                channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

                Console.WriteLine(" [*] Waiting for messages.");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine(" [x] Received {0}", message);

                    int dots = message.Split('.').Length - 1;
                    Thread.Sleep(dots * 1000);

                    Console.WriteLine(" [x] Done");

                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                };

                channel.BasicConsume(queue: "task_queue",
                                     autoAck: false,
                                     consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }

        #endregion

        #region Pubsub

        /// <summary>
        /// In fanout exchange type, send the messages to all Queues that are bind to the Queue
        /// </summary>
        public static void PubSub()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "test-fanout", type: "fanout", durable: true);

                var message = "Hello fanout Queues!";
                var body = Encoding.UTF8.GetBytes(message);
                channel.BasicPublish(exchange: "test-fanout",
                                     routingKey: "",
                                     basicProperties: null,
                                     body: body);
                Console.WriteLine(" [x] Sent {0}", message);
            }

        }

        #endregion

        #region FireAndForget_OneWayMessage
        private static void FireAndForget()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: QueueName,
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                string message = "Hello World, from .net code!";
                var body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish(exchange: "test-exchange",
                                     routingKey: "nb",
                                     basicProperties: null,
                                     body: body);
                Console.WriteLine(" [x] Sent {0}", message);
            }

        }

        #endregion

        #region MessageWithRouting

        /// <summary>
        /// sends messages selectively using the route name
        /// </summary>
        private static void Routing()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                string message = "Message to test-exchange and routekey of 'NB'";
                var body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish(ExchangeName,
                                     routingKey: "nb",
                                     basicProperties: null,
                                     body: body);

                Console.WriteLine(" [x] Sent {0}", message);
            }

        }

        #endregion

        #region StoreObject

        private static void StoreObject()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "save-users",
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                // instialize object
                var user = new User() { Id = 1, FirstName = "Rahman", LastName = "Mahmoodi" };
                var userJson = JsonConvert.SerializeObject(user);

                var body = Encoding.UTF8.GetBytes(userJson);

                channel.BasicPublish(exchange: "save-objects-exchange",
                                     routingKey: "",
                                     basicProperties: null,
                                     body: body);

                Console.WriteLine(" [x] Sent {0}", userJson);

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var received = ea.Body;
                    var message = Encoding.UTF8.GetString(received);
                    Console.WriteLine(" [x] Received {0}", message);

                    var userFromJson = JsonConvert.DeserializeObject<User>(message);
                    Console.WriteLine(" Object from message => {0}", userFromJson.FirstName + " " + userFromJson.LastName);
                };
                channel.BasicConsume(queue: "save-users",
                                     autoAck: true,
                                     consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }

        }

        #endregion


        private static void ReadMessage()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: ExchangeName,
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine(" [x] Received {0}", message);
                };
                channel.BasicConsume(queue: QueueName,
                                     autoAck: true,
                                     consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }



    }

    public class User
    {
        public int Id { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
    }

}
