using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Document;

namespace RavenDBPerformance
{
    class Program
    {
        static void Main(string[] args)
        {
            using(var documentStore = new DocumentStore
                                    {
                                        Url = "http://localhost:8080",
                                        DefaultDatabase = "PerformanceTests"
                                    })
            {

                documentStore.Initialize();

                var stopWatch = new Stopwatch();
                int numberOfDocuments = Convert.ToInt32(args[0]);

                DocumentsWithSessionPerDocument(numberOfDocuments, stopWatch, documentStore);
                DocumentsWithSessionPerThread(numberOfDocuments, stopWatch, documentStore);
                DocumentsWithSessionForAll(numberOfDocuments, stopWatch, documentStore);
                DocumentsWithBulk(numberOfDocuments, stopWatch, documentStore);
                DocumentsWithSessionPerDocumentAndMultipleThreads(numberOfDocuments, stopWatch, documentStore);
                DocumentsWithDocumentStorePerThread(numberOfDocuments, stopWatch, documentStore);
            }
        }

        [ThreadStatic]
        private static DocumentStore store;

        private static readonly ManualResetEvent syncEvent = new ManualResetEvent(false);

        private static void DocumentsWithDocumentStorePerThread(int numberOfDocuments, Stopwatch stopWatch, DocumentStore documentStore)
        {
            Console.WriteLine("Writing {0} documents with a store per thread...", numberOfDocuments);

            Thread[] threads = new Thread[10];

            for (int i = 0; i < 10; i++)
            {
                threads[i] = new Thread(WorkerForStorePerThread);
                threads[i].Start(numberOfDocuments / 10);
            }

            Thread.Sleep(TimeSpan.FromSeconds(10));

            stopWatch.Start();

            syncEvent.Set();

            for (int i = 0; i < 10; i++)
            {
                threads[i].Join();
            }

            stopWatch.Stop();

            SaveStats(documentStore, numberOfDocuments, stopWatch, "Documents with a store per thread");

            Console.WriteLine("Writing {0} with a store per thread {1}", numberOfDocuments,
                              stopWatch.ElapsedMilliseconds);

            stopWatch.Reset();
        }

        private static void WorkerForStorePerThread(object state)
        {
            var numberOfDocuments = (int) state;

            using (store = new DocumentStore
                               {
                                   Url = "http://localhost:8080",
                                   DefaultDatabase = "PerformanceTests"
                               })
            {
                store.Initialize();

                syncEvent.WaitOne();

                for (int i = 0; i < numberOfDocuments; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new Data {Counter = i});
                        session.SaveChanges();
                    }
                }
            }
        }

        private static void DocumentsWithSessionPerThread(int numberOfDocuments, Stopwatch stopWatch, DocumentStore documentStore)
        {
            Console.WriteLine("Writing {0} documents with a session per thread...", numberOfDocuments);

            Thread[] threads = new Thread[10];

            for (int i = 0; i < 10; i++)
            {
                threads[i] = new Thread(WorkerForSessionPerThread);
                threads[i].Start(new Tuple<DocumentStore, int>(documentStore, numberOfDocuments / 10));
            }

            Thread.Sleep(TimeSpan.FromSeconds(10));

            stopWatch.Start();

            syncEvent.Set();

            for (int i = 0; i < 10; i++)
            {
                threads[i].Join();
            }

            stopWatch.Stop();

            SaveStats(documentStore, numberOfDocuments, stopWatch, "Documents with a session per thread");

            Console.WriteLine("Writing {0} with a session per thread {1}", numberOfDocuments,
                              stopWatch.ElapsedMilliseconds);

            stopWatch.Reset();
        }

        private static void WorkerForSessionPerThread(object state)
        {
            var input = (Tuple<DocumentStore, int>)state;
            var documentStore = input.Item1;
            var numberOfDocuments = input.Item2;

            syncEvent.WaitOne();

            using (var session = documentStore.OpenSession())
            {
                for (int i = 0; i < numberOfDocuments; i++)
                {
                    session.Store(new Data {Counter = i});
                }

                session.SaveChanges();
            }
        }

        private static void DocumentsWithSessionPerDocumentAndMultipleThreads(int numberOfDocuments, Stopwatch stopWatch,
                                                    DocumentStore documentStore)
        {
            Console.WriteLine("Writing {0} documents with a session per document and multiple threads...", numberOfDocuments);

            stopWatch.Start();

            Parallel.For(0, numberOfDocuments, new ParallelOptions { MaxDegreeOfParallelism = 10 }, i =>
                                                   {
                                                       using (var session = documentStore.OpenSession())
                                                       {
                                                           session.Store(new Data { Counter = i });
                                                           session.SaveChanges();
                                                       }
                                                   });

            stopWatch.Stop();

            SaveStats(documentStore, numberOfDocuments, stopWatch, "Documents with session per document and multiple threads");

            Console.WriteLine("Writing {0} with a session per document and multiple threads took {1}", numberOfDocuments,
                              stopWatch.ElapsedMilliseconds);

            stopWatch.Reset();
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
