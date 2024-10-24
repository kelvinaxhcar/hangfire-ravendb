using Hangfire.Common;
using Hangfire.Raven.DistributedLocks;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.JobQueues;
using Hangfire.Raven.Storage;
using Hangfire.Server;
using Hangfire.Storage;
using Raven.Client.Documents.Session;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;

namespace Hangfire.Raven
{
    public class RavenConnection : JobStorageConnection
    {
        private readonly RavenStorage _storage;

        public RavenConnection(RavenStorage storage)
        {
            storage.ThrowIfNull(nameof(storage));
            this._storage = storage;
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new RavenWriteOnlyTransaction(_storage);
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return new RavenDistributedLock(_storage, "HangFire/" + resource, timeout, _storage.Options);
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0)
                throw new ArgumentNullException(nameof(queues));
            IPersistentJobQueueProvider[] array = queues.Select(_storage.QueueProviders.GetProvider).Distinct().ToArray();
            if (array.Length != 1)
                throw new InvalidOperationException("Multiple provider instances registered for queues: " + string.Join(", ", queues) + ". You should choose only one type of persistent queues per server instance.");
            return array[0].GetJobQueue().Dequeue(queues, cancellationToken);
        }

        public override string CreateExpiredJob(
          Job job,
          IDictionary<string, string> parameters,
          DateTime createdAt,
          TimeSpan expireIn)
        {
            job.ThrowIfNull(nameof(job));
            parameters.ThrowIfNull(nameof(parameters));
            var invocationData = InvocationData.SerializeJob(job);
            var expiredJob = Guid.NewGuid().ToString();
            var entity = new RavenJob()
            {
                Id = this._storage.Repository.GetId(typeof(RavenJob), expiredJob),
                InvocationData = invocationData,
                CreatedAt = createdAt,
                Parameters = parameters
            };
            using (IDocumentSession session = this._storage.Repository.OpenSession())
            {
                session.Store((object)entity);
                session.SetExpiry<RavenJob>(entity, createdAt + expireIn);
                session.SaveChanges();
                return expiredJob;
            }
        }

        public override JobData GetJobData(string key)
        {
            key.ThrowIfNull(nameof(key));
            using var documentSession = _storage.Repository.OpenSession();
            var id = _storage.Repository.GetId(typeof(RavenJob), key);
            var ravenJob = documentSession.Load<RavenJob>(id);
            if (ravenJob == null)
                return null;
            var job = (Job)null;
            var jobLoadException = (JobLoadException)null;
            try
            {
                job = ravenJob.InvocationData.DeserializeJob();
            }
            catch (JobLoadException ex)
            {
                jobLoadException = ex;
            }
            return new JobData()
            {
                Job = job,
                State = ravenJob.StateData?.Name,
                CreatedAt = ravenJob.CreatedAt,
                LoadException = jobLoadException
            };
        }

        public override StateData GetStateData(string jobId)
        {
            jobId.ThrowIfNull(nameof(jobId));
            using var documentSession = _storage.Repository.OpenSession();
            var id = _storage.Repository.GetId(typeof(RavenJob), jobId);
            return documentSession.Load<RavenJob>(id)?.StateData;
        }

        public override void SetJobParameter(string jobId, string name, string value)
        {
            jobId.ThrowIfNull(nameof(jobId));
            name.ThrowIfNull(nameof(name));
            using var documentSession = _storage.Repository.OpenSession();
            string id = _storage.Repository.GetId(typeof(RavenJob), jobId);
            documentSession.Load<RavenJob>(id).Parameters[name] = value;
            documentSession.SaveChanges();
        }

        public override string GetJobParameter(string jobId, string name)
        {
            jobId.ThrowIfNull(nameof(jobId));
            name.ThrowIfNull(nameof(name));
            using var documentSession = _storage.Repository.OpenSession();
            var id = _storage.Repository.GetId(typeof(RavenJob), jobId);
            var ravenJob = documentSession.Load<RavenJob>(id);
            if (ravenJob == null)
                return null;
            if (ravenJob.Parameters.TryGetValue(name, out string jobParameter))
                return jobParameter;
            if (!(name == "RetryCount"))
                return null;
            ravenJob.Parameters["RetryCount"] = "0";
            documentSession.SaveChanges();
            return "0";
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            key.ThrowIfNull(nameof(key));
            using var documentSession = _storage.Repository.OpenSession();
            var id = _storage.Repository.GetId(typeof(RavenSet), key);
            var ravenSet = documentSession.Load<RavenSet>(id);
            return ravenSet == null ? new HashSet<string>() : new HashSet<string>(ravenSet.Scores.Keys);
        }

        public override string GetFirstByLowestScoreFromSet(
          string key,
          double fromScore,
          double toScore)
        {
            key.ThrowIfNull(nameof(key));
            if (toScore < fromScore)
                throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");
            using var documentSession = _storage.Repository.OpenSession();
            string id = this._storage.Repository.GetId(typeof(RavenSet), key);
            var ravenSet = documentSession.Load<RavenSet>(id);
            return ravenSet?.Scores.Where(a => a.Value >= fromScore && a.Value <= toScore)
                                   .OrderBy(a => a.Value)
                                   .Select(a => a.Key)
                                   .FirstOrDefault();
        }

        public override void SetRangeInHash(
          string key,
          IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            key.ThrowIfNull(nameof(key));
            keyValuePairs.ThrowIfNull(nameof(keyValuePairs));
            using var documentSession = _storage.Repository.OpenSession();
            var id = this._storage.Repository.GetId(typeof(RavenHash), key);
            var entity = documentSession.Load<RavenHash>(id);
            if (entity == null)
            {
                entity = new RavenHash() { Id = id };
                documentSession.Store(entity);
            }
            foreach (KeyValuePair<string, string> keyValuePair in keyValuePairs)
                entity.Fields[keyValuePair.Key] = keyValuePair.Value;
            documentSession.SaveChanges();
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            key.ThrowIfNull(nameof(key));
            using var documentSession = _storage.Repository.OpenSession();
            return documentSession.Load<RavenHash>(_storage.Repository.GetId(typeof(RavenHash), key))?.Fields;
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            serverId.ThrowIfNull(nameof(serverId));
            context.ThrowIfNull(nameof(context));
            using var documentSession = this._storage.Repository.OpenSession();
            var id = this._storage.Repository.GetId(typeof(RavenServer), serverId);
            var entity = documentSession.Load<RavenServer>(id);
            if (entity == null)
            {
                entity = new RavenServer()
                {
                    Id = id,
                    Data = new RavenServer.ServerData()
                    {
                        StartedAt = new DateTime?(DateTime.UtcNow)
                    }
                };
                documentSession.Store(entity);
            }
            entity.Data.WorkerCount = context.WorkerCount;
            entity.Data.Queues = context.Queues;
            entity.Data.StartedAt = new DateTime?(DateTime.UtcNow);
            entity.LastHeartbeat = DateTime.UtcNow;
            documentSession.SaveChanges();
        }

        public override void RemoveServer(string serverId)
        {
            serverId.ThrowIfNull(nameof(serverId));
            using var documentSession = _storage.Repository.OpenSession();
            var id = this._storage.Repository.GetId(typeof(RavenServer), serverId);
            documentSession.Delete(id);
            documentSession.SaveChanges();
        }

        public override void Heartbeat(string serverId)
        {
            serverId.ThrowIfNull(nameof(serverId));
            using var documentSession = _storage.Repository.OpenSession();
            var id = _storage.Repository.GetId(typeof(RavenServer), serverId);
            var entity = documentSession.Load<RavenServer>(id);
            if (entity == null)
            {
                entity = new RavenServer() { Id = id };
                documentSession.Store(entity);
            }
            entity.LastHeartbeat = DateTime.UtcNow;
            documentSession.SaveChanges();
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
                throw new ArgumentException("The `timeOut` value must be positive.", nameof(timeOut));
            using var documentSession = _storage.Repository.OpenSession();
            var heartBeatCutOff = DateTime.UtcNow.Add(timeOut.Negate());
            List<RavenServer> list = [.. documentSession.Query<RavenServer>().Where((Expression<Func<RavenServer, bool>>)(t => t.LastHeartbeat < heartBeatCutOff))];
            foreach (RavenServer entity in list)
                documentSession.Delete(entity);
            documentSession.SaveChanges();
            return list.Count;
        }

        public override long GetSetCount(string key)
        {
            key.ThrowIfNull(nameof(key));
            using var documentSession = _storage.Repository.OpenSession();
            var id = _storage.Repository.GetId(typeof(RavenSet), key);
            var ravenSet = documentSession.Load<RavenSet>(id);
            return ravenSet == null ? 0L : (long)ravenSet.Scores.Count;
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            key.ThrowIfNull(nameof(key));
            using var documentSession = _storage.Repository.OpenSession();
            var id = _storage.Repository.GetId(typeof(RavenSet), key);
            var ravenSet = documentSession.Load<RavenSet>(id);
            return ravenSet == null ? [] : ravenSet.Scores.Skip(startingFrom).Take(endingAt - startingFrom + 1).Select(t => t.Key).ToList();
        }

        public override TimeSpan GetSetTtl(string key)
        {
            key.ThrowIfNull(nameof(key));
            using var session = _storage.Repository.OpenSession();
            var id = _storage.Repository.GetId(typeof(RavenSet), key);
            var ravenSet = session.Load<RavenSet>(id);
            if (ravenSet == null)
                return TimeSpan.FromSeconds(-1.0);
            DateTime? expiry = session.GetExpiry<RavenSet>(ravenSet);
            return !expiry.HasValue ? TimeSpan.FromSeconds(-1.0) : expiry.Value - DateTime.UtcNow;
        }

        public override long GetCounter(string key)
        {
            key.ThrowIfNull(nameof(key));
            using var documentSession = _storage.Repository.OpenSession();
            var id = _storage.Repository.GetId(typeof(Counter), key);
            var counter = documentSession.Load<Counter>(id);
            return counter == null ? 0L : (long)counter.Value;
        }

        public override long GetHashCount(string key)
        {
            key.ThrowIfNull(nameof(key));
            using var documentSession = _storage.Repository.OpenSession();
            var ravenHash = documentSession.Load<RavenHash>(_storage.Repository.GetId(typeof(RavenHash), key));
            return ravenHash == null ? 0L : (long)ravenHash.Fields.Count;
        }

        public override TimeSpan GetHashTtl(string key)
        {
            key.ThrowIfNull(nameof(key));
            using var session = _storage.Repository.OpenSession();
            var id = _storage.Repository.GetId(typeof(RavenHash), key);
            var ravenHash = session.Load<RavenHash>(id);
            if (ravenHash == null)
                return TimeSpan.FromSeconds(-1.0);
            DateTime? expiry = session.GetExpiry<RavenHash>(ravenHash);
            return !expiry.HasValue ? TimeSpan.FromSeconds(-1.0) : expiry.Value - DateTime.UtcNow;
        }

        public override string GetValueFromHash(string key, string name)
        {
            key.ThrowIfNull(nameof(key));
            name.ThrowIfNull(nameof(name));
            using var documentSession = this._storage.Repository.OpenSession();
            var ravenHash = documentSession.Load<RavenHash>(_storage.Repository.GetId(typeof(RavenHash), key));
            return ravenHash == null || !ravenHash.Fields.TryGetValue(name, out string str) ? null : str;
        }

        public override long GetListCount(string key)
        {
            key.ThrowIfNull(nameof(key));
            using var documentSession = _storage.Repository.OpenSession();
            var id = _storage.Repository.GetId(typeof(RavenList), key);
            var ravenList = documentSession.Load<RavenList>(id);
            return ravenList == null ? 0L : ravenList.Values.Count;
        }

        public override TimeSpan GetListTtl(string key)
        {
            key.ThrowIfNull(nameof(key));
            using var session = _storage.Repository.OpenSession();
            var id = _storage.Repository.GetId(typeof(RavenList), key);
            var ravenList = session.Load<RavenList>(id);
            if (ravenList == null)
                return TimeSpan.FromSeconds(-1.0);
            DateTime? expiry = session.GetExpiry(ravenList);
            return !expiry.HasValue ? TimeSpan.FromSeconds(-1.0) : expiry.Value - DateTime.UtcNow;
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            key.ThrowIfNull(nameof(key));
            using var documentSession = _storage.Repository.OpenSession();
            var id = _storage.Repository.GetId(typeof(RavenList), key);
            var ravenList = documentSession.Load<RavenList>(id);
            return ravenList == null ? new List<string>() : ravenList.Values.Skip(startingFrom).Take(endingAt - startingFrom + 1).ToList();
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            key.ThrowIfNull(nameof(key));
            using var documentSession = _storage.Repository.OpenSession();
            var id = _storage.Repository.GetId(typeof(RavenList), key);
            var ravenList = documentSession.Load<RavenList>(id);
            return ravenList == null ? new List<string>() : ravenList.Values;
        }
    }
}
