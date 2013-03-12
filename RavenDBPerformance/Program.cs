using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Document;

namespace RavenDBPerformance
{
    class Program
    {
        static void Main(string[] args)
        {
            var documentStore = new DocumentStore
                                    {
                                        Url = "http://localhost:8080",
                                        DefaultDatabase = "PerformanceTests"
                                    };

            documentStore.Initialize();

            var stopWatch = new Stopwatch();
            int numberOfDocuments = Convert.ToInt32(args[0]);

            DocumentsWithSessionPerDocument(numberOfDocuments, stopWatch, documentStore);
            DocumentsWithSessionForAll(numberOfDocuments, stopWatch, documentStore);
            DocumentsWithBulk(numberOfDocuments, stopWatch, documentStore);
        }

        private static void DocumentsWithSessionForAll(int numberOfDocuments, Stopwatch stopWatch,
                                                    DocumentStore documentStore)
        {
            Console.WriteLine("Writing {0} documents with a session for all document...", numberOfDocuments);

            stopWatch.Start();

            using (var session = documentStore.OpenSession())
            {
                for (int i = 0; i < numberOfDocuments; i++)
                {
                    session.Store(new Data {Counter = i});
                }

                session.SaveChanges();
            }

            stopWatch.Stop();

            SaveStats(documentStore, numberOfDocuments, stopWatch, "Documents with session for all document");

            Console.WriteLine("Writing {0} with a session for all document took {1}", numberOfDocuments,
                              stopWatch.ElapsedMilliseconds);

            stopWatch.Reset();
        }

        private static void DocumentsWithSessionPerDocument(int numberOfDocuments, Stopwatch stopWatch,
                                                            DocumentStore documentStore)
        {
            Console.WriteLine("Writing {0} documents with a session per document...", numberOfDocuments);

            stopWatch.Start();

            for (int i = 0; i < numberOfDocuments; i++)
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Data {Counter = i});
                    session.SaveChanges();
                }
            }

            stopWatch.Stop();

            SaveStats(documentStore, numberOfDocuments, stopWatch, "Documents with session per document");

            Console.WriteLine("Writing {0} with a session per document took {1}", numberOfDocuments,
                              stopWatch.ElapsedMilliseconds);

            stopWatch.Reset();
        }

        private static void DocumentsWithBulk(int numberOfDocuments, Stopwatch stopWatch, DocumentStore documentStore)
        {
            Console.WriteLine("Writing {0} documents with bulk inserts for all documents...", numberOfDocuments);

            stopWatch.Start();

            using (var bulkInsert = documentStore.BulkInsert())
            {
                for (int i = 0; i < numberOfDocuments; i++)
                {
                    bulkInsert.Store(new Data { Counter = i });
                }
            }

            stopWatch.Stop();

            SaveStats(documentStore, numberOfDocuments, stopWatch, "Documents with bulk inserts for all documents");

            Console.WriteLine("Writing {0} with bulk inserts for all documents took {1}", numberOfDocuments,
                              stopWatch.ElapsedMilliseconds);

            stopWatch.Reset();
        }

        private static void SaveStats(DocumentStore documentStore, int numberOfDocuments, Stopwatch stopWatch, string description)
        {
            using (var session = documentStore.OpenSession())
            {
                var stats = session.Load<Statistic>("statistics") ?? new Statistic();
                stats.Runs.Add(new Stats(description, numberOfDocuments, stopWatch.ElapsedMilliseconds));

                session.Store(stats);

                session.SaveChanges();
            }
        }

        public class Data
        {
            public int Id { get; set; }

            public long Counter { get; set; }
        }

        public class Statistic
        {
            public Statistic()
            {
                this.Runs = new List<Stats>();
            }

            public string Id { get { return "statistics"; } }

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
            }

            public string Description { get; private set; }

            public long NumberOfDocuments { get; private set; }

            public DateTimeOffset At { get; private set; }

            public long TimeInMs { get; private set; }
        }
    }
}
