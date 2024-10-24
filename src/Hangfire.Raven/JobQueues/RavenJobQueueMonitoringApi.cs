using Hangfire.Annotations;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.Storage;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Hangfire.Raven.JobQueues
{
    public class RavenJobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private RavenStorage _storage;

        public RavenJobQueueMonitoringApi([NotNull] RavenStorage storage)
        {
            storage.ThrowIfNull(nameof(storage));
            this._storage = storage;
        }

        public IEnumerable<string> GetQueues()
        {
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
                return (IEnumerable<string>)documentSession.Query<JobQueue>().Select<JobQueue, string>((Expression<Func<JobQueue, string>>)(x => x.Queue)).Distinct<string>().ToList<string>();
        }

        public IEnumerable<string> GetEnqueuedJobIds(string queue, int pageFrom, int perPage)
        {
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
                return (IEnumerable<string>)documentSession.Query<JobQueue>().Where<JobQueue>((Expression<Func<JobQueue, bool>>)(a => a.Queue == queue && a.FetchedAt == new DateTime?())).Skip<JobQueue>(pageFrom).Take<JobQueue>(perPage).Select<JobQueue, string>((Expression<Func<JobQueue, string>>)(a => a.JobId)).ToList<string>();
        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int pageFrom, int perPage)
        {
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
                return (IEnumerable<string>)documentSession.Query<JobQueue>().Where<JobQueue>((Expression<Func<JobQueue, bool>>)(a => a.Queue == queue && a.FetchedAt != new DateTime?())).Skip<JobQueue>(pageFrom).Take<JobQueue>(perPage).Select<JobQueue, string>((Expression<Func<JobQueue, string>>)(a => a.JobId)).ToList<string>();
        }

        public EnqueuedAndFetchedCount GetEnqueuedAndFetchedCount(string queue)
        {
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
            {
                int num1 = documentSession.Query<JobQueue>().Where<JobQueue>((Expression<Func<JobQueue, bool>>)(a => a.FetchedAt != new DateTime?() && a.Queue == queue)).Count<JobQueue>();
                int num2 = documentSession.Query<JobQueue>().Where<JobQueue>((Expression<Func<JobQueue, bool>>)(a => a.FetchedAt == new DateTime?() && a.Queue == queue)).Count<JobQueue>();
                return new EnqueuedAndFetchedCount()
                {
                    EnqueuedCount = new int?(num2),
                    FetchedCount = new int?(num1)
                };
            }
        }
    }
}
