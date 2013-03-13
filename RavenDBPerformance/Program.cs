using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Client.Extensions;

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
                DocumentsWithAsyncSessionPerDocument(numberOfDocuments, stopWatch, documentStore);
                DocumentsWithSessionPerThread(numberOfDocuments, stopWatch, documentStore);
                DocumentsWithSessionForAll(numberOfDocuments, stopWatch, documentStore);
                DocumentsWithBulk(numberOfDocuments, stopWatch, documentStore);
                DocumentsWithSessionPerDocumentAndMultipleThreads(numberOfDocuments, stopWatch, documentStore);
                DocumentsWithDocumentStorePerThread(numberOfDocuments, stopWatch, documentStore);
                DocumentsWithDatabasePerThread(numberOfDocuments, stopWatch, documentStore);
                DocumentsWithDatabaseInstancePerThread(numberOfDocuments, stopWatch, documentStore);
            }

            Console.ReadLine();
        }

        [ThreadStatic]
        private static DocumentStore store;

        private static readonly ManualResetEvent syncEvent = new ManualResetEvent(false);

        private class ThreadConfig
        {
            public int NumberOfDocuments;
            public int DatabaseInstanceId;
            public int DatabaseId;
        }

        private static void DocumentsWithDocumentStorePerThread(int numberOfDocuments, Stopwatch stopWatch, DocumentStore documentStore)
        {
            syncEvent.Reset();
            Console.WriteLine("Writing {0} documents with a store per thread...", numberOfDocuments);

            Thread[] threads = new Thread[10];

            for (int i = 0; i < 10; i++)
            {
                threads[i] = new Thread(WorkerForStorePerThread);
                threads[i].Start(new ThreadConfig() { NumberOfDocuments = numberOfDocuments / 10, DatabaseInstanceId = 0, DatabaseId = 0 });
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

            Console.WriteLine("Writing {0} with a store per thread {1}, {2} docs/sec", numberOfDocuments,
                              stopWatch.ElapsedMilliseconds, numberOfDocuments * 1000 / stopWatch.ElapsedMilliseconds);

            stopWatch.Reset();
        }

        private static void DocumentsWithDatabasePerThread(int numberOfDocuments, Stopwatch stopWatch, DocumentStore documentStore)
        {
            syncEvent.Reset();
            Console.WriteLine("Writing {0} documents with a database per thread...", numberOfDocuments);

            Thread[] threads = new Thread[10];

            for (int i = 0; i < 10; i++)
            {
                threads[i] = new Thread(WorkerForStorePerThread);
                threads[i].Start(new ThreadConfig() { NumberOfDocuments = numberOfDocuments / 10, DatabaseInstanceId = 0, DatabaseId = i });
            }

            Thread.Sleep(TimeSpan.FromSeconds(10));

            stopWatch.Start();

            syncEvent.Set();

            for (int i = 0; i < 10; i++)
            {
                threads[i].Join();
            }

            stopWatch.Stop();

            SaveStats(documentStore, numberOfDocuments, stopWatch, "Documents with a database per thread");

            Console.WriteLine("Writing {0} with a database per thread {1}, {2} docs/sec", numberOfDocuments,
                              stopWatch.ElapsedMilliseconds, numberOfDocuments * 1000 / stopWatch.ElapsedMilliseconds);

            stopWatch.Reset();
        }

        private static void DocumentsWithDatabaseInstancePerThread(int numberOfDocuments, Stopwatch stopWatch, DocumentStore documentStore)
        {
            syncEvent.Reset();
            Console.WriteLine("Writing {0} documents with a database instance per thread...", numberOfDocuments);

            Thread[] threads = new Thread[10];

            for (int i = 0; i < 10; i++)
            {
                threads[i] = new Thread(WorkerForStorePerThread);
                threads[i].Start(new ThreadConfig() { NumberOfDocuments = numberOfDocuments / 10, DatabaseInstanceId = i, DatabaseId = i });
            }

            Thread.Sleep(TimeSpan.FromSeconds(10));

            stopWatch.Start();

            syncEvent.Set();

            for (int i = 0; i < 10; i++)
            {
                threads[i].Join();
            }

            stopWatch.Stop();

            SaveStats(documentStore, numberOfDocuments, stopWatch, "Documents with a database instance per thread");

            Console.WriteLine("Writing {0} with a database instance per thread {1}, {2} docs/sec", numberOfDocuments,
                              stopWatch.ElapsedMilliseconds, numberOfDocuments * 1000 / stopWatch.ElapsedMilliseconds);

            stopWatch.Reset();
        }
        
        private static void WorkerForStorePerThread(object state)
        {
            ThreadConfig config = (ThreadConfig)state;
            var numberOfDocuments = config.NumberOfDocuments;

            using (store = new DocumentStore
                               {
                                   Url = "http://localhost:" + (8080 + config.DatabaseInstanceId),
                                   DefaultDatabase = "PerformanceTests" + config.DatabaseId
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

            Console.WriteLine("Writing {0} with a session per thread {1}, {2} docs/sec", numberOfDocuments,
                              stopWatch.ElapsedMilliseconds, numberOfDocuments * 1000 / stopWatch.ElapsedMilliseconds);

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

            Console.WriteLine("Writing {0} with a session per document and multiple threads took {1}, {2} docs/sec", numberOfDocuments,
                              stopWatch.ElapsedMilliseconds, numberOfDocuments * 1000 / stopWatch.ElapsedMilliseconds);

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

            Console.WriteLine("Writing {0} with a session for all document took {1}, {2} docs/sec", numberOfDocuments,
                              stopWatch.ElapsedMilliseconds, numberOfDocuments * 1000 / stopWatch.ElapsedMilliseconds);

            stopWatch.Reset();
        }

        private static void DocumentsWithAsyncSessionPerDocument(int numberOfDocuments, Stopwatch stopWatch,
                                                            DocumentStore documentStore)
        {
            Console.WriteLine("Writing {0} documents with an async session per document...", numberOfDocuments);

            stopWatch.Start();

            for (int i = 0; i < numberOfDocuments; i++)
            {
                using (var session = documentStore.OpenAsyncSession())
                {
                    session.Store(new Data {Counter = i});
                    session.SaveChangesAsync();
                }
            }

            stopWatch.Stop();

            SaveStats(documentStore, numberOfDocuments, stopWatch, "Documents with async session per document");

            Console.WriteLine("Writing {0} with an async session per document took {1}, {2} docs/sec", numberOfDocuments,
                              stopWatch.ElapsedMilliseconds, numberOfDocuments * 1000 / stopWatch.ElapsedMilliseconds);

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

            Console.WriteLine("Writing {0} with a session per document took {1}, {2} docs/sec", numberOfDocuments,
                              stopWatch.ElapsedMilliseconds, numberOfDocuments * 1000 / stopWatch.ElapsedMilliseconds);

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

            Console.WriteLine("Writing {0} with bulk inserts for all documents took {1}, {2} docs/sec", numberOfDocuments,
                              stopWatch.ElapsedMilliseconds, numberOfDocuments * 1000 / stopWatch.ElapsedMilliseconds);

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
                this.DocsPerSecond = timeInMs == 0 ? -1 : numberOfDocs * 1000 / timeInMs;
            }

            public string Description { get; private set; }

            public long NumberOfDocuments { get; private set; }

            public DateTimeOffset At { get; private set; }

            public long TimeInMs { get; private set; }

            public long DocsPerSecond { get; private set; }
        }
    }
}
