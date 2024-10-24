using Hangfire.Annotations;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.Storage;
using Raven.Client.Documents.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Hangfire.Raven.JobQueues
{
    public class RavenJobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private RavenStorage _storage;

        public RavenJobQueueMonitoringApi([NotNull] RavenStorage storage)
        {
            storage.ThrowIfNull(nameof(storage));
            _storage = storage;
        }

        public IEnumerable<string> GetQueues()
        {
            using var documentSession = _storage.Repository.OpenSession();
            return documentSession
                .Query<JobQueue>()
                .Select(x => x.Queue)
                .Distinct()
                .ToList();
        }

        public IEnumerable<string> GetEnqueuedJobIds(string queue, int pageFrom, int perPage)
        {
            using var documentSession = _storage.Repository.OpenSession();
            return documentSession
                .Query<JobQueue>()
                .Where(a => a.Queue == queue && a.FetchedAt == new DateTime?())
                .Skip(pageFrom)
                .Take(perPage)
                .Select(a => a.JobId)
                .ToList();
        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int pageFrom, int perPage)
        {
            using var documentSession = _storage.Repository.OpenSession();
            return documentSession
                .Query<JobQueue>()
                .Where(a => a.Queue == queue && a.FetchedAt != new DateTime?()).Skip(pageFrom)
                .Take(perPage)
                .Select(a => a.JobId)
                .ToList();
        }

        public EnqueuedAndFetchedCount GetEnqueuedAndFetchedCount(string queue)
        {
            using var documentSession = _storage.Repository.OpenSession();
            var num1 = documentSession.Query<JobQueue>().Where(a => a.FetchedAt != new DateTime?() && a.Queue == queue).Count();
            var num2 = documentSession.Query<JobQueue>().Where(a => a.FetchedAt == new DateTime?() && a.Queue == queue).Count();
            return new EnqueuedAndFetchedCount()
            {
                EnqueuedCount = new int?(num2),
                FetchedCount = new int?(num1)
            };
        }
    }
}
