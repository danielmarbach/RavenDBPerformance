using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Extensions;
using System.Linq;

namespace RavenDBPerformance
{
    using System.Transactions;

    using RavenDbWcfHost;

    class Program
    {
        static void Main(string[] args)
        {
            using (var documentStore = CreateDocumentStore(args.ElementAtOrDefault(1)))
            {
                documentStore.Initialize();

                var stopWatch = new Stopwatch();
                int numberOfDocuments = Convert.ToInt32(args[0]);
                bool embedded = !string.IsNullOrEmpty(args.ElementAtOrDefault(1));
                DocumentsWithSessionPerDocument(numberOfDocuments, stopWatch, documentStore);
                DocumentsWithSessionPerDocumentTwoPhaseCommit(numberOfDocuments, stopWatch, documentStore);
                DocumentsWithAsyncSessionPerDocument(numberOfDocuments, stopWatch, documentStore);
                DocumentsWithSessionPerThread(numberOfDocuments, stopWatch, documentStore);
                DocumentsWithSessionForAll(numberOfDocuments, stopWatch, documentStore);
                DocumentsWithBulk(numberOfDocuments, stopWatch, documentStore);
                DocumentsWithSessionPerDocumentAndMultipleThreads(numberOfDocuments, stopWatch, documentStore);
                if (!embedded) DocumentsWithDocumentStorePerThread(numberOfDocuments, stopWatch, documentStore);
                if (!embedded) DocumentsWithDatabasePerThread(numberOfDocuments, stopWatch, documentStore, 10, 1);
                if (!embedded) DocumentsWithDatabasePerThread(numberOfDocuments, stopWatch, documentStore, 5, 2);
                if (!embedded) DocumentsWithDatabasePerThread(numberOfDocuments, stopWatch, documentStore, 3, 4);
                if (!embedded) DocumentsWithDatabaseInstancePerThread(numberOfDocuments, stopWatch, documentStore, 10, 1);
                if (!embedded) DocumentsWithDatabaseInstancePerThread(numberOfDocuments, stopWatch, documentStore, 5, 2);
                if (!embedded) DocumentsWithDatabaseInstancePerThread(numberOfDocuments, stopWatch, documentStore, 3, 4);
                if (!embedded) UseWcfRavenDb(numberOfDocuments, stopWatch, documentStore);
            }

            Console.ReadLine();
        }

        private static DocumentStore CreateDocumentStore(string embedded)
        {
            if (string.IsNullOrEmpty(embedded))
            {
                return new DocumentStore
                {
                    Url = "http://localhost:8080",
                    DefaultDatabase = "PerformanceTests"
                };
            }

            return new EmbeddableDocumentStore
                       {
                           DataDirectory = @"c:\PerformanceTests", 
                       };
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

        private static void DocumentsWithDatabasePerThread(int numberOfDocuments, Stopwatch stopWatch, DocumentStore documentStore, int numberOfDatabases, int numberOfThreadsPerDatabase)
        {
            int totalThreads = numberOfDatabases * numberOfThreadsPerDatabase;
            syncEvent.Reset();
            Console.WriteLine("Writing {0} documents with a database per {1} thread(s), {2} total threads...", numberOfDocuments, numberOfThreadsPerDatabase, totalThreads);

            Thread[] threads = new Thread[totalThreads];
            int documentsPerThread = numberOfDocuments / totalThreads;

            for (int i = 0; i < numberOfDatabases; i++)
            for (int j = 0; j < numberOfThreadsPerDatabase; j++)
            {
                int threadId = i * numberOfThreadsPerDatabase + j;
                threads[threadId] = new Thread(WorkerForStorePerThread);
                threads[threadId].Start(new ThreadConfig() { NumberOfDocuments = documentsPerThread, DatabaseInstanceId = 0, DatabaseId = i });
            }

            Thread.Sleep(TimeSpan.FromSeconds(10));

            stopWatch.Start();

            syncEvent.Set();

            for (int i = 0; i < totalThreads; i++)
            {
                threads[i].Join();
            }

            stopWatch.Stop();

            SaveStats(documentStore, numberOfDocuments, stopWatch, string.Format("Documents with a database per {0} threads, {1} total threads", numberOfThreadsPerDatabase, totalThreads));

            Console.WriteLine("Writing {0} with a database per {3} threads, {4} total threads: {1}, {2} docs/sec", numberOfDocuments,
                              stopWatch.ElapsedMilliseconds, numberOfDocuments * 1000 / stopWatch.ElapsedMilliseconds, numberOfThreadsPerDatabase, totalThreads);

            stopWatch.Reset();
        }

        private static void DocumentsWithDatabaseInstancePerThread(int numberOfDocuments, Stopwatch stopWatch, DocumentStore documentStore, int numberOfDatabases, int numberOfThreadsPerDatabase)
        {
            syncEvent.Reset();
            int totalThreads = numberOfDatabases * numberOfThreadsPerDatabase;
            Console.WriteLine("Writing {0} documents with a database instance per {1} thread(s), {2} thread total...", numberOfDocuments, numberOfThreadsPerDatabase, totalThreads);

            Thread[] threads = new Thread[totalThreads];
            int documentsPerThread = numberOfDocuments / totalThreads;

            for (int i = 0; i < numberOfDatabases; i++)
            for (int j = 0; j < numberOfThreadsPerDatabase; j++)
            {
                int threadId = i * numberOfThreadsPerDatabase + j;
                threads[threadId] = new Thread(WorkerForStorePerThread);
                threads[threadId].Start(new ThreadConfig() { NumberOfDocuments = documentsPerThread, DatabaseInstanceId = i, DatabaseId = i });
            }

            Thread.Sleep(TimeSpan.FromSeconds(10));

            stopWatch.Start();

            syncEvent.Set();

            for (int i = 0; i < totalThreads; i++)
            {
                threads[i].Join();
            }

            stopWatch.Stop();

            SaveStats(documentStore, numberOfDocuments, stopWatch, string.Format("Documents with a database instance per {0} threads, {1} total threads", numberOfThreadsPerDatabase, totalThreads));

            Console.WriteLine("Writing {0} with a database instance per {3} threads, {4} total threads: {1}, {2} docs/sec", numberOfDocuments,
                              stopWatch.ElapsedMilliseconds, numberOfDocuments * 1000 / stopWatch.ElapsedMilliseconds, numberOfThreadsPerDatabase, totalThreads);

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

        private static void DocumentsWithSessionPerDocumentTwoPhaseCommit(int numberOfDocuments, Stopwatch stopWatch,
                                                            DocumentStore documentStore)
        {
            Console.WriteLine("Writing {0} documents with a session per document with two pase commit...", numberOfDocuments);

            var enlistment = new TwoPhaseCommitEnlistment();
            stopWatch.Start();

            for (int i = 0; i < numberOfDocuments; i++)
            {
                using (var scope = new TransactionScope())
                {
                    Transaction.Current.EnlistDurable(Guid.NewGuid(), enlistment, EnlistmentOptions.None);
                    using (var session = documentStore.OpenSession())
                    {
                        session.Store(new Data { Counter = i });
                        session.SaveChanges();
                    }
                }
            }

            stopWatch.Stop();

            SaveStats(documentStore, numberOfDocuments, stopWatch, "Documents with session per document with two pase commit");

            Console.WriteLine("Writing {0} with a session per document with two pase commit took {1}, {2} docs/sec", numberOfDocuments,
                              stopWatch.ElapsedMilliseconds, numberOfDocuments * 1000 / stopWatch.ElapsedMilliseconds);

            stopWatch.Reset();
        }

        private static void UseWcfRavenDb(int numberOfDocuments, Stopwatch stopWatch, DocumentStore documentStore)
        {
            Console.WriteLine("Writing {0} documents with a session per document using WcfHost...", numberOfDocuments);

            var client = new RavenDB.RavenDbServiceClient();

            stopWatch.Start();

            for (int i = 0; i < numberOfDocuments; i++)
            {
                client.Store(new WcfData { Counter = i });
            }

            stopWatch.Stop();

            SaveStats(documentStore, numberOfDocuments, stopWatch, "Documents with session per document using WcfHost");

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

        [Serializable]
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

    public class TwoPhaseCommitEnlistment : IEnlistmentNotification
    {
        public void Prepare(PreparingEnlistment preparingEnlistment)
        {
            preparingEnlistment.Prepared();
        }

        public void Commit(Enlistment enlistment)
        {
            enlistment.Done();
        }

        public void Rollback(Enlistment enlistment)
        {
            enlistment.Done();
        }

        public void InDoubt(Enlistment enlistment)
        {
            enlistment.Done();
        }
    }
}
