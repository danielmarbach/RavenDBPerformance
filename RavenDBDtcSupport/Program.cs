namespace RavenDBDtcSupport
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Transactions;

    using Raven.Abstractions.Data;
    using Raven.Abstractions.Exceptions;
    using Raven.Client.Document;
    using Raven.Imports.Newtonsoft.Json;

    class Program
    {
        static void Main(string[] args)
        {
            var countdown = new CountdownEvent(2);
            var firstNotification = new FirstNotification(countdown);
            var secondNotification = new SecondNotification(countdown);

            var documentStore = new DocumentStore
            {
                Url = "http://localhost:8080",
                DefaultDatabase = "DtcSupport"
            };

            documentStore.Initialize();

            using (var session = documentStore.OpenSession())
            {
                session.Store(new Person { Id = "persons/oren", Name = "oren" });
                session.SaveChanges();
            }

            var t1 = Task.Factory.StartNew(
                () =>
                    {
                        using (var tx = new TransactionScope())
                        {
                            using (var session = documentStore.OpenSession())
                            {
                                session.Advanced.UseOptimisticConcurrency = true;
                                var person = session.Load<Person>("persons/oren");
                                person.Etag = session.Advanced.GetEtagFor(person);

                                try
                                {
                                    person.Name = "oren eini";

                                    session.Store(person, person.Etag);
                                    session.SaveChanges();
                                }
                                catch (ConcurrencyException ex)
                                {
                                    Console.WriteLine(ex.Message);
                                    throw;
                                }
                            }

                            //Transaction.Current.EnlistDurable(Guid.NewGuid(), firstNotification, EnlistmentOptions.None);

                            tx.Complete();
                        }
                    });

            var t2 = Task.Factory.StartNew(() =>
                {
                    using (var tx = new TransactionScope())
                    {
                        using (var session = documentStore.OpenSession())
                        {
                            session.Advanced.UseOptimisticConcurrency = true;
                            var person = session.Load<Person>("persons/oren");
                            person.Etag = session.Advanced.GetEtagFor(person);

                            try
                            {
                                person.Name = "eini";

                                session.Store(person, person.Etag);
                                session.SaveChanges();
                            }
                            catch (ConcurrencyException ex)
                            {
                                Console.WriteLine(ex.Message);
                                throw;
                            }
                        }

                        //Transaction.Current.EnlistDurable(Guid.NewGuid(), secondNotification, EnlistmentOptions.None);

                        tx.Complete();
                    }
                });

            Task.WaitAll(new[] { t1, t2 });
        }
    }

    public class Person
    {
        public string Id { get; set; }
        [JsonIgnore]
        public Etag Etag { get; set; }
        public string Name { get; set; }
    }

    public class FirstNotification : ISinglePhaseNotification
    {
        private readonly CountdownEvent countdown;

        public FirstNotification(CountdownEvent countdown)
        {
            this.countdown = countdown;
        }

        public void Prepare(PreparingEnlistment preparingEnlistment)
        {
            preparingEnlistment.Prepared();

            this.countdown.Signal();
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

        public void SinglePhaseCommit(SinglePhaseEnlistment singlePhaseEnlistment)
        {
            singlePhaseEnlistment.Committed();
        }
    }

    public class SecondNotification : ISinglePhaseNotification
    {
        private readonly CountdownEvent countdown;

        public SecondNotification(CountdownEvent countdown)
        {
            this.countdown = countdown;
        }

        public void Prepare(PreparingEnlistment preparingEnlistment)
        {
            preparingEnlistment.Prepared();

            this.countdown.Signal();
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

        public void SinglePhaseCommit(SinglePhaseEnlistment singlePhaseEnlistment)
        {
            singlePhaseEnlistment.Committed();
        }
    }
}
