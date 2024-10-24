using Hangfire.Annotations;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.Storage;
using Hangfire.Storage;
using Raven.Client.Documents.Session;
using System;

namespace Hangfire.Raven.Entities
{
    public class RavenFetchedJob : IFetchedJob, IDisposable
    {
        private readonly RavenStorage _storage;

        private bool _requeued { get; set; }

        private bool _removedFromQueue { get; set; }

        private bool _disposed { get; set; }

        public string Id { get; set; }

        public string JobId { get; set; }

        public string Queue { get; set; }

        public RavenFetchedJob([NotNull] RavenStorage storage, JobQueue jobQueue)
        {
            storage.ThrowIfNull(nameof(storage));
            jobQueue.ThrowIfNull(nameof(jobQueue));
            this._storage = storage;
            this.JobId = jobQueue.JobId;
            this.Queue = jobQueue.Queue;
            this.Id = jobQueue.Id;
        }

        public void RemoveFromQueue()
        {
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
            {
                JobQueue entity = documentSession.Load<JobQueue>(this.Id);
                if (entity != null)
                {
                    documentSession.Delete<JobQueue>(entity);
                    documentSession.SaveChanges();
                }
            }
            this._removedFromQueue = true;
        }

        public void Requeue()
        {
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
                documentSession.Load<JobQueue>(this.Id).FetchedAt = new DateTime?();
            this._requeued = true;
        }

        public void Dispose()
        {
            if (this._disposed)
                return;
            if (!this._removedFromQueue && !this._requeued)
                this.Requeue();
            this._disposed = true;
        }
    }
}
