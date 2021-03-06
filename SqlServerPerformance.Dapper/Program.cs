﻿using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Transactions;
using DapperExtensions;
using DapperExtensions.Mapper;

namespace SqlServerPerformance.Dapper
{
    using System.Threading;

    class Program
    {
        static void Main(string[] args)
        {
            var stopWatch = new Stopwatch();
            int numberOfDocuments = Convert.ToInt32(args[0]);

            DapperExtensions.DapperExtensions.DefaultMapper = typeof(PluralizedAutoClassMapper<>);

            DocumentsWithTransactionPerDocument(numberOfDocuments, stopWatch);
            DocumentsWithTransactionPerDocument10Threads(numberOfDocuments, stopWatch);

            Console.ReadLine();
        }

        private static void DocumentsWithTransactionPerDocument(int numberOfDocuments, Stopwatch stopWatch)
        {
            Console.WriteLine("Writing {0} with transaction per document...", numberOfDocuments);

            using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnectionString"].ConnectionString))
            {
                connection.Open();

                stopWatch.Start();

                for (int i = 0; i < numberOfDocuments; i++)
                {
                    using (var nested = new TransactionScope(TransactionScopeOption.RequiresNew))
                    {
                        connection.Insert(new Data {Counter = i});

                        nested.Complete();
                    }
                }

                stopWatch.Stop();

                SaveStats(connection, numberOfDocuments, stopWatch, "Documents with transaction per document.");
            }

            Console.WriteLine("Writing {0} with transaction per document {1}, {2} docs/sec", numberOfDocuments,
                              stopWatch.ElapsedMilliseconds, numberOfDocuments * 1000 / stopWatch.ElapsedMilliseconds);

            stopWatch.Reset();
        }        
        
        private static void DocumentsWithTransactionPerDocument10Threads(int numberOfDocuments, Stopwatch stopWatch)
        {
            Console.WriteLine("Writing {0} with transaction per document 10 threads...", numberOfDocuments);

            using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnectionString"].ConnectionString))
            {
                connection.Open();
                ManualResetEvent startSignal = new ManualResetEvent(false);

                var threads = new Thread[10];
                for (int j = 0; j < 10; j++)
                {
                    threads[j] = new Thread(
                        () =>
                            {
                                using (
                                    var connection2 =
                                        new SqlConnection(
                                            ConfigurationManager.ConnectionStrings["DefaultConnectionString"]
                                                .ConnectionString))
                                {
                                    connection2.Open();
                                    startSignal.WaitOne();
                                    for (int i = 0; i < numberOfDocuments / 10; i++)
                                    {
                                        using (var nested = new TransactionScope(TransactionScopeOption.RequiresNew))
                                        {
                                            connection2.Insert(new Data { Counter = i });

                                            nested.Complete();
                                        }
                                    }
                                }
                            });
                    threads[j].Start();
                }

                Thread.Sleep(3);
                stopWatch.Start();
                
                startSignal.Set();
                foreach (var thread in threads)
                {
                    thread.Join();
                }

                stopWatch.Stop();

                SaveStats(connection, numberOfDocuments, stopWatch, "Documents with transaction per document 10 threads.");
            }

            Console.WriteLine("Writing {0} with transaction per document 10 threads {1}, {2} docs/sec", numberOfDocuments,
                              stopWatch.ElapsedMilliseconds, numberOfDocuments * 1000 / stopWatch.ElapsedMilliseconds);

            stopWatch.Reset();
        }

        private static void SaveStats(IDbConnection database, int numberOfDocuments, Stopwatch stopWatch, string description)
        {
            using (var scope = new TransactionScope(TransactionScopeOption.RequiresNew))
            {
                database.Insert(new Statistic(description, numberOfDocuments, stopWatch.ElapsedMilliseconds));

                scope.Complete();
            }
        }

        public class Data
        {
            public int Id { get; set; }

            public long Counter { get; set; }
        }

        public class Statistic
        {
            public Statistic(string description, long numberOfDocs, long timeInMs)
            {
                this.TimeInMs = timeInMs;
                this.NumberOfDocuments = numberOfDocs;
                this.At = DateTime.Now;
                this.Description = description;
                this.DocsPerSecond = timeInMs == 0 ? -1 : numberOfDocs * 1000 / timeInMs;
            }

            public string Description { get; private set; }

            public long NumberOfDocuments { get; private set; }

            public DateTime At { get; private set; }

            public long TimeInMs { get; private set; }

            public long DocsPerSecond { get; private set; }
        }
    }
}
