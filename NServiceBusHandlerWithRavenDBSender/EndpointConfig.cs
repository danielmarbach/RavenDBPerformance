


namespace NServiceBusHandlerWithRavenDBSender
{
    using System;
    using System.Threading.Tasks;

    using Messages;

    using NServiceBus;

    /*
		This class configures this endpoint as a Server. More information about how to configure the NServiceBus host
		can be found here: http://nservicebus.com/GenericHost.aspx
	*/
	public class EndpointConfig : IConfigureThisEndpoint, AsA_Publisher, IWantCustomInitialization
    {
	    public void Init()
	    {
            Configure.With()
                .DefaultBuilder()
                .XmlSerializer()
                .RavenSubscriptionStorage();
	    }
    }

    public class Runner : IWantToRunWhenBusStartsAndStops
    {
        public IBus Bus { get; set; }

        public void Start()
        {
            Parallel.For(
                0,
                1000,
                i =>
                    { Bus.Publish(new Event()); });

            Console.WriteLine("done");
        }

        public void Stop()
        {
        }
    }
}