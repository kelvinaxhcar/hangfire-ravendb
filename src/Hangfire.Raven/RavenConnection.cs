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
            return (IWriteOnlyTransaction)new RavenWriteOnlyTransaction(this._storage);
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return (IDisposable)new RavenDistributedLock(this._storage, "HangFire/" + resource, timeout, this._storage.Options);
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0)
                throw new ArgumentNullException(nameof(queues));
            IPersistentJobQueueProvider[] array = ((IEnumerable<string>)queues).Select<string, IPersistentJobQueueProvider>((Func<string, IPersistentJobQueueProvider>)(queue => this._storage.QueueProviders.GetProvider(queue))).Distinct<IPersistentJobQueueProvider>().ToArray<IPersistentJobQueueProvider>();
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
            InvocationData invocationData = InvocationData.Serialize(job);
            string expiredJob = Guid.NewGuid().ToString();
            RavenJob entity = new RavenJob()
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
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
            {
                string id = this._storage.Repository.GetId(typeof(RavenJob), key);
                RavenJob ravenJob = documentSession.Load<RavenJob>(id);
                if (ravenJob == null)
                    return (JobData)null;
                Job job = (Job)null;
                JobLoadException jobLoadException = (JobLoadException)null;
                try
                {
                    job = ravenJob.InvocationData.Deserialize();
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
        }

        public override StateData GetStateData(string jobId)
        {
            jobId.ThrowIfNull(nameof(jobId));
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
            {
                string id = this._storage.Repository.GetId(typeof(RavenJob), jobId);
                return documentSession.Load<RavenJob>(id)?.StateData;
            }
        }

        public override void SetJobParameter(string jobId, string name, string value)
        {
            jobId.ThrowIfNull(nameof(jobId));
            name.ThrowIfNull(nameof(name));
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
            {
                string id = this._storage.Repository.GetId(typeof(RavenJob), jobId);
                documentSession.Load<RavenJob>(id).Parameters[name] = value;
                documentSession.SaveChanges();
            }
        }

        public override string GetJobParameter(string jobId, string name)
        {
            jobId.ThrowIfNull(nameof(jobId));
            name.ThrowIfNull(nameof(name));
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
            {
                string id = this._storage.Repository.GetId(typeof(RavenJob), jobId);
                RavenJob ravenJob = documentSession.Load<RavenJob>(id);
                if (ravenJob == null)
                    return (string)null;
                string jobParameter;
                if (ravenJob.Parameters.TryGetValue(name, out jobParameter))
                    return jobParameter;
                if (!(name == "RetryCount"))
                    return (string)null;
                ravenJob.Parameters["RetryCount"] = "0";
                documentSession.SaveChanges();
                return "0";
            }
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            key.ThrowIfNull(nameof(key));
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
            {
                string id = this._storage.Repository.GetId(typeof(RavenSet), key);
                RavenSet ravenSet = documentSession.Load<RavenSet>(id);
                return ravenSet == null ? new HashSet<string>() : new HashSet<string>((IEnumerable<string>)ravenSet.Scores.Keys);
            }
        }

        public override string GetFirstByLowestScoreFromSet(
          string key,
          double fromScore,
          double toScore)
        {
            key.ThrowIfNull(nameof(key));
            if (toScore < fromScore)
                throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
            {
                string id = this._storage.Repository.GetId(typeof(RavenSet), key);
                RavenSet ravenSet = documentSession.Load<RavenSet>(id);
                return ravenSet == null ? (string)null : ravenSet.Scores.Where<KeyValuePair<string, double>>((Func<KeyValuePair<string, double>, bool>)(a => a.Value >= fromScore && a.Value <= toScore)).OrderBy<KeyValuePair<string, double>, double>((Func<KeyValuePair<string, double>, double>)(a => a.Value)).Select<KeyValuePair<string, double>, string>((Func<KeyValuePair<string, double>, string>)(a => a.Key)).FirstOrDefault<string>();
            }
        }

        public override void SetRangeInHash(
          string key,
          IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            key.ThrowIfNull(nameof(key));
            keyValuePairs.ThrowIfNull(nameof(keyValuePairs));
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
            {
                string id = this._storage.Repository.GetId(typeof(RavenHash), key);
                RavenHash entity = documentSession.Load<RavenHash>(id);
                if (entity == null)
                {
                    entity = new RavenHash() { Id = id };
                    documentSession.Store((object)entity);
                }
                foreach (KeyValuePair<string, string> keyValuePair in keyValuePairs)
                    entity.Fields[keyValuePair.Key] = keyValuePair.Value;
                documentSession.SaveChanges();
            }
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            key.ThrowIfNull(nameof(key));
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
                return documentSession.Load<RavenHash>(this._storage.Repository.GetId(typeof(RavenHash), key))?.Fields;
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            serverId.ThrowIfNull(nameof(serverId));
            context.ThrowIfNull(nameof(context));
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
            {
                string id = this._storage.Repository.GetId(typeof(RavenServer), serverId);
                RavenServer entity = documentSession.Load<RavenServer>(id);
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
                    documentSession.Store((object)entity);
                }
                entity.Data.WorkerCount = context.WorkerCount;
                entity.Data.Queues = (IEnumerable<string>)context.Queues;
                entity.Data.StartedAt = new DateTime?(DateTime.UtcNow);
                entity.LastHeartbeat = DateTime.UtcNow;
                documentSession.SaveChanges();
            }
        }

        public override void RemoveServer(string serverId)
        {
            serverId.ThrowIfNull(nameof(serverId));
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
            {
                string id = this._storage.Repository.GetId(typeof(RavenServer), serverId);
                documentSession.Delete(id);
                documentSession.SaveChanges();
            }
        }

        public override void Heartbeat(string serverId)
        {
            serverId.ThrowIfNull(nameof(serverId));
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
            {
                string id = this._storage.Repository.GetId(typeof(RavenServer), serverId);
                RavenServer entity = documentSession.Load<RavenServer>(id);
                if (entity == null)
                {
                    entity = new RavenServer() { Id = id };
                    documentSession.Store((object)entity);
                }
                entity.LastHeartbeat = DateTime.UtcNow;
                documentSession.SaveChanges();
            }
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
                throw new ArgumentException("The `timeOut` value must be positive.", nameof(timeOut));
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
            {
                DateTime heartBeatCutOff = DateTime.UtcNow.Add(timeOut.Negate());
                List<RavenServer> list = documentSession.Query<RavenServer>().Where<RavenServer>((Expression<Func<RavenServer, bool>>)(t => t.LastHeartbeat < heartBeatCutOff)).ToList<RavenServer>();
                foreach (RavenServer entity in list)
                    documentSession.Delete<RavenServer>(entity);
                documentSession.SaveChanges();
                return list.Count;
            }
        }

        public override long GetSetCount(string key)
        {
            key.ThrowIfNull(nameof(key));
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
            {
                string id = this._storage.Repository.GetId(typeof(RavenSet), key);
                RavenSet ravenSet = documentSession.Load<RavenSet>(id);
                return ravenSet == null ? 0L : (long)ravenSet.Scores.Count;
            }
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            key.ThrowIfNull(nameof(key));
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
            {
                string id = this._storage.Repository.GetId(typeof(RavenSet), key);
                RavenSet ravenSet = documentSession.Load<RavenSet>(id);
                return ravenSet == null ? new List<string>() : ravenSet.Scores.Skip<KeyValuePair<string, double>>(startingFrom).Take<KeyValuePair<string, double>>(endingAt - startingFrom + 1).Select<KeyValuePair<string, double>, string>((Func<KeyValuePair<string, double>, string>)(t => t.Key)).ToList<string>();
            }
        }

        public override TimeSpan GetSetTtl(string key)
        {
            key.ThrowIfNull(nameof(key));
            using (IDocumentSession session = this._storage.Repository.OpenSession())
            {
                string id = this._storage.Repository.GetId(typeof(RavenSet), key);
                RavenSet ravenSet = session.Load<RavenSet>(id);
                if (ravenSet == null)
                    return TimeSpan.FromSeconds(-1.0);
                DateTime? expiry = session.GetExpiry<RavenSet>(ravenSet);
                return !expiry.HasValue ? TimeSpan.FromSeconds(-1.0) : expiry.Value - DateTime.UtcNow;
            }
        }

        public override long GetCounter(string key)
        {
            key.ThrowIfNull(nameof(key));
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
            {
                string id = this._storage.Repository.GetId(typeof(Counter), key);
                Counter counter = documentSession.Load<Counter>(id);
                return counter == null ? 0L : (long)counter.Value;
            }
        }

        public override long GetHashCount(string key)
        {
            key.ThrowIfNull(nameof(key));
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
            {
                RavenHash ravenHash = documentSession.Load<RavenHash>(this._storage.Repository.GetId(typeof(RavenHash), key));
                return ravenHash == null ? 0L : (long)ravenHash.Fields.Count;
            }
        }

        public override TimeSpan GetHashTtl(string key)
        {
            key.ThrowIfNull(nameof(key));
            using (IDocumentSession session = this._storage.Repository.OpenSession())
            {
                string id = this._storage.Repository.GetId(typeof(RavenHash), key);
                RavenHash ravenHash = session.Load<RavenHash>(id);
                if (ravenHash == null)
                    return TimeSpan.FromSeconds(-1.0);
                DateTime? expiry = session.GetExpiry<RavenHash>(ravenHash);
                return !expiry.HasValue ? TimeSpan.FromSeconds(-1.0) : expiry.Value - DateTime.UtcNow;
            }
        }

        public override string GetValueFromHash(string key, string name)
        {
            key.ThrowIfNull(nameof(key));
            name.ThrowIfNull(nameof(name));
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
            {
                RavenHash ravenHash = documentSession.Load<RavenHash>(this._storage.Repository.GetId(typeof(RavenHash), key));
                string str;
                return ravenHash == null || !ravenHash.Fields.TryGetValue(name, out str) ? (string)null : str;
            }
        }

        public override long GetListCount(string key)
        {
            key.ThrowIfNull(nameof(key));
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
            {
                string id = this._storage.Repository.GetId(typeof(RavenList), key);
                RavenList ravenList = documentSession.Load<RavenList>(id);
                return ravenList == null ? 0L : (long)ravenList.Values.Count;
            }
        }

        public override TimeSpan GetListTtl(string key)
        {
            key.ThrowIfNull(nameof(key));
            using (IDocumentSession session = this._storage.Repository.OpenSession())
            {
                string id = this._storage.Repository.GetId(typeof(RavenList), key);
                RavenList ravenList = session.Load<RavenList>(id);
                if (ravenList == null)
                    return TimeSpan.FromSeconds(-1.0);
                DateTime? expiry = session.GetExpiry<RavenList>(ravenList);
                return !expiry.HasValue ? TimeSpan.FromSeconds(-1.0) : expiry.Value - DateTime.UtcNow;
            }
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            key.ThrowIfNull(nameof(key));
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
            {
                string id = this._storage.Repository.GetId(typeof(RavenList), key);
                RavenList ravenList = documentSession.Load<RavenList>(id);
                return ravenList == null ? new List<string>() : ravenList.Values.Skip<string>(startingFrom).Take<string>(endingAt - startingFrom + 1).ToList<string>();
            }
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            key.ThrowIfNull(nameof(key));
            using (IDocumentSession documentSession = this._storage.Repository.OpenSession())
            {
                string id = this._storage.Repository.GetId(typeof(RavenList), key);
                RavenList ravenList = documentSession.Load<RavenList>(id);
                return ravenList == null ? new List<string>() : ravenList.Values;
            }
        }
    }
}
