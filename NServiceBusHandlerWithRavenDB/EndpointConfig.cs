


namespace NServiceBusHandlerWithRavenDB
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;

    using Messages;

    using NServiceBus;
    using NServiceBus.UnitOfWork;

    using Raven.Client;
    using Raven.Client.Document;

    public class EndpointConfig : IConfigureThisEndpoint, AsA_Server, IWantCustomInitialization
    {
        public void Init()
        {
            //var store = new DocumentStore { Url = "http://localhost:8080", DefaultDatabase = "PerformanceTests" };
            //store.Initialize();

            Configure.With().DefaultBuilder().XmlSerializer().PurgeOnStartup(true);

            //Configure.Instance.Configurer.ConfigureComponent<UnitOfWork>(DependencyLifecycle.InstancePerCall);
            //Configure.Instance.Configurer.ConfigureComponent(store.OpenSession, DependencyLifecycle.InstancePerUnitOfWork);
        }
    }

    public class Handler : IHandleMessages<Event>
    {
        //private readonly IDocumentSession session;

        public Handler(/*IDocumentSession session*/)
        {
            //this.session = session;
        }

        public void Handle(Event message)
        {
            //this.session.Store(message);

            var messageId = this.Bus().CurrentMessageContext.Id;
            using (var stream = File.CreateText(string.Format(@".\{0}", messageId)))
            {
                stream.Write(messageId);
            }
        }
    }

    public class UnitOfWork : IManageUnitsOfWork
    {
        private readonly IDocumentSession session;

        private readonly Stopwatch stopwatch;

        private Statistic stats;

        public UnitOfWork(/*IDocumentSession session*/)
        {
            this.session = session;

            this.stopwatch = new Stopwatch();
        }

        public void Begin()
        {
            //this.stats = this.session.Load<Statistic>("statistics") ?? new Statistic();

            this.stopwatch.Start();
        }

        public void End(Exception ex = null)
        {
            if (ex == null)
            {
                //this.session.SaveChanges();

                this.stopwatch.Stop();

                //this.stats.Runs.Add(new Stats("DocumentWithNServiceBusHandler", 1, this.stopwatch.ElapsedMilliseconds));
                //this.session.Store(this.stats);

                //this.session.SaveChanges();
            }
        }
    }

    public class Statistic
    {
        public Statistic()
        {
            this.Runs = new List<Stats>();
        }

        public string Id
        {
            get
            {
                return "statistics";
            }
        }

        public List<Stats> Runs { get; set; }
    }

    public class Stats
    {
        public Stats(string description, long numberOfDocs, long timeInMs)
        {
            this.TimeInMs = timeInMs;
            this.NumberOfDocuments = numberOfDocs;
            this.At = DateTimeOffset.Now;
            this.Description = description;
            this.DocsPerSecond = timeInMs == 0 ? -1 : numberOfDocs * 1000 / timeInMs;
        }

        public string Description { get; private set; }

        public long NumberOfDocuments { get; private set; }

        public DateTimeOffset At { get; private set; }

        public long TimeInMs { get; private set; }

        public long DocsPerSecond { get; private set; }
    }
}