using System;
using System.Collections.Generic;
using System.Diagnostics;
using Simple.Data;

namespace SqlServerPerformance.SimpleData
{
    class Program
    {
        static void Main(string[] args)
        {
            var stopWatch = new Stopwatch();
            int numberOfDocuments = Convert.ToInt32(args[0]);

            DocumentsWithTransactionPerDocument(Database.Open(), numberOfDocuments, stopWatch);
            DocumentsWithTransactionForAllDocument(Database.Open(), numberOfDocuments, stopWatch);
            DocumentsWithBulkInsert(Database.Open(), numberOfDocuments, stopWatch);
        }

        private static void DocumentsWithTransactionPerDocument(dynamic database, int numberOfDocuments, Stopwatch stopWatch)
        {
            Console.WriteLine("Writing {0} with transaction per document...", numberOfDocuments);

            stopWatch.Start();

            for (int i = 0; i < numberOfDocuments; i++)
            {
                using (var tx = database.BeginTransaction())
                {
                    tx.Users.Insert(new Data { Counter = i });

                    tx.Commit();
                }
            }

            stopWatch.Stop();

            SaveStats(database, numberOfDocuments, stopWatch, "Documents with transaction per document.");

            Console.WriteLine("Writing {0} with transaction per document {1}, {2} docs/sec", numberOfDocuments,
                              stopWatch.ElapsedMilliseconds, numberOfDocuments*1000/stopWatch.ElapsedMilliseconds);

            stopWatch.Reset();
        }

        private static void DocumentsWithTransactionForAllDocument(dynamic database, int numberOfDocuments, Stopwatch stopWatch)
        {
            Console.WriteLine("Writing {0} with transaction for all document...", numberOfDocuments);

            stopWatch.Start();

            using (var tx = database.BeginTransaction())
            {
                for (int i = 0; i < numberOfDocuments; i++)
                {
                    tx.Users.Insert(new Data { Counter = 1});
                }

                tx.Commit();
            }

            stopWatch.Stop();

            SaveStats(database, numberOfDocuments, stopWatch, "Documents with transaction for all document.");

            Console.WriteLine("Writing {0} with transaction for all document {1}, {2} docs/sec", numberOfDocuments,
                              stopWatch.ElapsedMilliseconds, numberOfDocuments * 1000 / stopWatch.ElapsedMilliseconds);

            stopWatch.Reset();
        }

        private static void DocumentsWithBulkInsert(dynamic database, int numberOfDocuments, Stopwatch stopWatch)
        {
            Console.WriteLine("Writing {0} with bulk insert...", numberOfDocuments);

            stopWatch.Start();

            var datas = new List<Data>();

            for (int i = 0; i < numberOfDocuments; i++)
            {
                datas.Add(new Data { Counter = 1 });
            }

            using (var tx = database.BeginTransaction())
            {
                tx.Users.Insert(datas);

                tx.Commit();
            }

            stopWatch.Stop();

            SaveStats(database, numberOfDocuments, stopWatch, "Documents with bulk insert.");

            Console.WriteLine("Writing {0} with bulk insert {1}, {2} docs/sec", numberOfDocuments,
                              stopWatch.ElapsedMilliseconds, numberOfDocuments * 1000 / stopWatch.ElapsedMilliseconds);

            stopWatch.Reset();
        }

        private static void SaveStats(dynamic database, int numberOfDocuments, Stopwatch stopWatch, string description)
        {
            using (var tx = database.BeginTransaction())
            {
                var stats = new Stats(description, numberOfDocuments, stopWatch.ElapsedMilliseconds);

                tx.Stats.Insert(stats);

                tx.Commit();
            }
        }

        public class Data
        {
            public long Counter { get; set; }
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
