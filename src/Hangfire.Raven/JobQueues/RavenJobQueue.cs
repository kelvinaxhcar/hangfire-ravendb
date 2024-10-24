using Hangfire.Annotations;
using Hangfire.Logging;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.Storage;
using Hangfire.Storage;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using System;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;

namespace Hangfire.Raven.JobQueues
{
    public class RavenJobQueue : IPersistentJobQueue
    {
        private static readonly ILog Logger = LogProvider.For<RavenJobQueue>();
        private readonly RavenStorage _storage;
        private readonly RavenStorageOptions _options;
        private static readonly object _lockObject = new object();
        internal static readonly AutoResetEvent NewItemInQueueEvent = new AutoResetEvent(true);

        public RavenJobQueue([NotNull] RavenStorage storage, RavenStorageOptions options)
        {
            storage.ThrowIfNull(nameof(storage));
            options.ThrowIfNull(nameof(options));
            _storage = storage;
            _options = options;
        }

        [NotNull]
        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            queues.ThrowIfNull(nameof(queues));

            if (queues.Length == 0)
                throw new ArgumentException("Queue array must be non-empty.", nameof(queues));

            Expression<Func<JobQueue, bool>>[] expressionArray =
            [
        job => job.FetchedAt == null,
        job => job.FetchedAt < DateTime.UtcNow.AddSeconds(-_options.InvisibilityTimeout.TotalSeconds)
            ];

            int index = 0;

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                Expression<Func<JobQueue, bool>> expression = expressionArray[index];

                using (IDocumentSession documentSession = _storage.Repository.OpenSession())
                {
                    documentSession.Advanced.UseOptimisticConcurrency = true;

                    lock (_lockObject)
                    {
                        foreach (string queue in queues)
                        {
                            JobQueue jobQueue = documentSession
                                .Query<JobQueue>()
                                .Customize(x => x.WaitForNonStaleResults())
                                .Where(expression)
                                .Where(j => j.Queue == queue)
                                .FirstOrDefault();

                            if (jobQueue != null)
                            {
                                try
                                {
                                    jobQueue.FetchedAt = DateTime.UtcNow;
                                    documentSession.SaveChanges();

                                    return new RavenFetchedJob(_storage, jobQueue);
                                }
                                catch (ConcurrencyException)
                                {

                                }
                            }
                        }
                    }
                }

                index = (index + 1) % expressionArray.Length;

                if (index == expressionArray.Length - 1)
                {
                    WaitHandle.WaitAny(new WaitHandle[]
                    {
                cancellationToken.WaitHandle,
                NewItemInQueueEvent
                    }, _options.QueuePollInterval);

                    cancellationToken.ThrowIfCancellationRequested();
                }
            }
        }


        public void Enqueue(string queue, string jobId)
        {
            using (IDocumentSession documentSession = _storage.Repository.OpenSession())
            {
                JobQueue entity = new JobQueue()
                {
                    Id = _storage.Repository.GetId(typeof(JobQueue), queue, jobId),
                    JobId = jobId,
                    Queue = queue
                };
                documentSession.Store((object)entity);
                documentSession.SaveChanges();
            }
        }
    }
}
