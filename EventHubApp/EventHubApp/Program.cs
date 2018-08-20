using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace IntSol.Training.Applications
{
    public class Telemetry
    {
        public int SensorId { get; set; }
        public string Location { get; set; }
        public int Temperature { get; set; }
        public int Humidity { get; set; }
        public DateTime RecordedTime { get; set; }

        public override string ToString()
        {
            return string.Format(@"{0}, {1}, {2}, {3}, {4}",
                this.RecordedTime.ToString(),
                this.SensorId, this.Location, this.Temperature, this.Humidity);
        }
    }

    public static class MainClass
    {
        private const int MAX_LIMIT = 500;

        public static void Main(string[] args)
        {
            var connectionString = @"Endpoint=sb://iomegaehnamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=cCOcy8SlbkPAY0S0BDifYsv/Y1uGycJpmVa7nhr2Dws=";
            var eventHubPath = "iomegasensormessages";

            try
            {
                var eventHubConnectionStringBuilder = new EventHubsConnectionStringBuilder(connectionString)
                {
                    EntityPath = eventHubPath
                };
                var eventHubClient = EventHubClient.CreateFromConnectionString(eventHubConnectionStringBuilder.ToString());
                var registeredLocations = new string[] { "Bangalore", "Hyderabad", "Chennai", "Madurai", "Trivandrum" };
                var random = new Random();
                var lastTemperature = random.Next(20, 30);

                Parallel.ForEach<string>(registeredLocations,
                    location =>
                    {
                        var counter = 1;

                        while (true)
                        {
                            if (counter >= MAX_LIMIT)
                                break;

                            if (counter % 100 == 0)
                            {
                                Thread.Sleep(random.Next(2000, 5000));

                                Console.WriteLine();
                            }

                            var plusOrMinus = random.Next(1, 100000000) % 2 == 0;
                            var currentTemperature = lastTemperature + (plusOrMinus ? random.Next(1, 5) : (-random.Next(1, 5)));

                            var telemetryData = new Telemetry
                            {
                                SensorId = random.Next(1, 100),
                                Location = location,
                                RecordedTime = DateTime.Now,
                                Temperature = currentTemperature,
                                Humidity = random.Next(70, 90)
                            };

                            var content = JsonConvert.SerializeObject(telemetryData);
                            var eventData = new EventData(Encoding.ASCII.GetBytes(content));

                            //Console.WriteLine(content);

                            eventHubClient.SendAsync(eventData);

                            counter++;

                            Console.Write("*");
                        }
                    });
            }
            catch (Exception exceptionObject)
            {
                Console.WriteLine("Error Occurred, Details : " + exceptionObject.Message);
            }

            Console.WriteLine("End of App!");
            Console.ReadLine();
        }
    }
}