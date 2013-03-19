using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RavenDbWcfHost
{
    using System.ServiceModel;
    using System.ServiceModel.Description;

    using Raven.Client.Embedded;

    class Program
    {
        public static EmbeddableDocumentStore documentStore;

        static void Main(string[] args)
        {
            documentStore = new EmbeddableDocumentStore
                       {
                           DataDirectory = @"c:\PerformanceTests", UseEmbeddedHttpServer = true
                           
                       };
            documentStore.Initialize();
            
            Uri baseAddress = new Uri("http://localhost:8180/raven");

            // Create the ServiceHost.
            using (ServiceHost host = new ServiceHost(typeof(RavenDbService), baseAddress))
            {
                // Enable metadata publishing.
                ServiceMetadataBehavior smb = new ServiceMetadataBehavior();
                smb.HttpGetEnabled = true;
                smb.MetadataExporter.PolicyVersion = PolicyVersion.Policy15;
                host.Description.Behaviors.Add(smb);

                // Open the ServiceHost to start listening for messages. Since
                // no endpoints are explicitly configured, the runtime will create
                // one endpoint per base address for each service contract implemented
                // by the service.
                host.Open();

                Console.WriteLine("The service is ready at {0}", baseAddress);
                Console.WriteLine("Press <Enter> to stop the service.");
                Console.ReadLine();

                // Close the ServiceHost.
                host.Close();
            }
        }
    }

    public class RavenDbService : IRavenDbService
    {
        public void Store(WcfData data)
        {
            using (var session = Program.documentStore.OpenSession())
            {
                session.Store(data);
                session.SaveChanges();
            }
        }
    }
}
