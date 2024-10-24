using Hangfire.Annotations;
using Hangfire.Logging;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.JobQueues;
using Hangfire.Raven.Storage;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Hangfire.Raven
{
    public class RavenWriteOnlyTransaction : JobStorageTransaction
    {
        private static readonly ILog Logger = LogProvider.For<RavenWriteOnlyTransaction>();
        private readonly RavenStorage _storage;
        private IDocumentSession _session;
        private List<KeyValuePair<string, PatchRequest>> _patchRequests;
        private readonly Queue<Action> _afterCommitCommandQueue = new Queue<Action>();

        public RavenWriteOnlyTransaction([NotNull] RavenStorage storage)
        {
            storage.ThrowIfNull(nameof(storage));
            _storage = storage;
            _patchRequests = new List<KeyValuePair<string, PatchRequest>>();
            _session = _storage.Repository.OpenSession();
            _session.Advanced.UseOptimisticConcurrency = false;
            _session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        }

        public override void Commit()
        {
            foreach (IGrouping<string, PatchRequest> grouping in (IEnumerable<IGrouping<string, PatchRequest>>)_patchRequests.ToLookup(a => a.Key, a => a.Value))
            {
                IGrouping<string, PatchRequest> item = grouping;
                foreach (ICommandData command in item.Select(x => new PatchCommandData(item.Key, null, x, null)))
                    _session.Advanced.Defer(command);
            }
            try
            {
                _session.SaveChanges();
                _session.Dispose();
            }
            catch
            {
                Logger.Error("- Concurrency exception");
                _session.Dispose();
                throw;
            }
            foreach (Action afterCommitCommand in _afterCommitCommandQueue)
                afterCommitCommand();
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            _session.SetExpiry<RavenJob>(_storage.Repository.GetId(typeof(RavenJob), jobId), expireIn);
        }

        public override void PersistJob(string jobId)
        {
            _session.RemoveExpiry<RavenJob>(_storage.Repository.GetId(typeof(RavenJob), jobId));
        }

        public override void SetJobState(string jobId, IState state)
        {
            var ravenJob = _session.Load<RavenJob>(_storage.Repository.GetId(typeof(RavenJob), jobId));
            ravenJob.History.Insert(0, new StateHistoryDto()
            {
                StateName = state.Name,
                Data = state.SerializeData(),
                Reason = state.Reason,
                CreatedAt = DateTime.UtcNow
            });
            ravenJob.StateData = new StateData()
            {
                Name = state.Name,
                Data = state.SerializeData(),
                Reason = state.Reason
            };
        }

        public override void AddJobState(string jobId, IState state) => SetJobState(jobId, state);

        public override void AddRangeToSet(string key, IList<string> items)
        {
            key.ThrowIfNull(nameof(key));
            items.ThrowIfNull(nameof(items));
            var orCreateSet = FindOrCreateSet(_storage.Repository.GetId(typeof(RavenSet), key));
            foreach (string key1 in (IEnumerable<string>)items)
                orCreateSet.Scores[key1] = 0.0;
        }

        public override void AddToQueue(string queue, string jobId)
        {
            var jobQueue = _storage.QueueProviders.GetProvider(queue).GetJobQueue();
            jobQueue.Enqueue(queue, jobId);
            if (!(jobQueue.GetType() == typeof(RavenJobQueue)))
                return;
            _afterCommitCommandQueue.Enqueue(() => RavenJobQueue.NewItemInQueueEvent.Set());
        }

        public override void IncrementCounter(string key)
        {
            IncrementCounter(key, TimeSpan.MinValue);
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            UpdateCounter(key, expireIn, 1);
        }

        public override void DecrementCounter(string key)
        {
            DecrementCounter(key, TimeSpan.MinValue);
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            UpdateCounter(key, expireIn, -1);
        }

        public void UpdateCounter(string key, TimeSpan expireIn, int value)
        {
            string id = _storage.Repository.GetId(typeof(Counter), key);
            if (_session.Load<Counter>(id) == null)
            {
                Counter entity = new Counter()
                {
                    Id = id,
                    Value = value
                };
                _session.Store(entity);
                if (!(expireIn != TimeSpan.MinValue))
                    return;
                _session.SetExpiry(entity, expireIn);
            }
            else
                _patchRequests.Add(new KeyValuePair<string, PatchRequest>(id, new PatchRequest()
                {
                    Script = string.Format("Value += {0}", value)
                }));
        }

        public override void AddToSet(string key, string value) => AddToSet(key, value, 0.0);

        public override void AddToSet(string key, string value, double score)
        {
            FindOrCreateSet(_storage.Repository.GetId(typeof(RavenSet), key)).Scores[value] = score;
        }

        public override void RemoveFromSet(string key, string value)
        {
            key.ThrowIfNull(nameof(key));
            var orCreateSet = FindOrCreateSet(_storage.Repository.GetId(typeof(RavenSet), key));
            orCreateSet.Scores.Remove(value);
            if (orCreateSet.Scores.Count != 0)
                return;
            _session.Delete(orCreateSet);
        }

        public override void RemoveSet(string key)
        {
            key.ThrowIfNull(nameof(key));
            _session.Delete(_storage.Repository.GetId(typeof(RavenSet), key));
        }

        public override void ExpireSet([NotNull] string key, TimeSpan expireIn)
        {
            key.ThrowIfNull(nameof(key));
            _session.SetExpiry(FindOrCreateSet(_storage.Repository.GetId(typeof(RavenSet), key)), expireIn);
        }

        public override void PersistSet([NotNull] string key)
        {
            key.ThrowIfNull(nameof(key));
            _session.RemoveExpiry(FindOrCreateSet(_storage.Repository.GetId(typeof(RavenSet), key)));
        }

        public override void InsertToList(string key, string value)
        {
            key.ThrowIfNull(nameof(key));
            FindOrCreateList(_storage.Repository.GetId(typeof(RavenList), key)).Values.Add(value);
        }

        public override void RemoveFromList(string key, string value)
        {
            key.ThrowIfNull(nameof(key));
            var orCreateList = FindOrCreateList(_storage.Repository.GetId(typeof(RavenList), key));
            orCreateList.Values.RemoveAll(v => v == value);
            if (orCreateList.Values.Count != 0)
                return;
            _session.Delete(orCreateList);
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            var orCreateList = FindOrCreateList(_storage.Repository.GetId(typeof(RavenList), key));
            orCreateList.Values = orCreateList.Values.Skip(keepStartingFrom).Take(keepEndingAt - keepStartingFrom + 1).ToList();
            if (orCreateList.Values.Count != 0)
                return;
            _session.Delete(orCreateList);
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            key.ThrowIfNull(nameof(key));
            _session.SetExpiry(FindOrCreateList(_storage.Repository.GetId(typeof(RavenList), key)), expireIn);
        }

        public override void PersistList(string key)
        {
            key.ThrowIfNull(nameof(key));
            _session.RemoveExpiry(FindOrCreateList(_storage.Repository.GetId(typeof(RavenList), key)));
        }

        public override void SetRangeInHash(
          string key,
          IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            key.ThrowIfNull(nameof(key));
            keyValuePairs.ThrowIfNull(nameof(keyValuePairs));
            var orCreateHash = FindOrCreateHash(_storage.Repository.GetId(typeof(RavenHash), key));
            foreach (KeyValuePair<string, string> keyValuePair in keyValuePairs)
                orCreateHash.Fields[keyValuePair.Key] = keyValuePair.Value;
        }

        public override void RemoveHash(string key)
        {
            key.ThrowIfNull(nameof(key));
            _session.Delete(_storage.Repository.GetId(typeof(RavenHash), key));
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            key.ThrowIfNull(nameof(key));
            _session.SetExpiry(FindOrCreateHash(_storage.Repository.GetId(typeof(RavenHash), key)), expireIn);
        }

        public override void PersistHash([NotNull] string key)
        {
            key.ThrowIfNull(nameof(key));
            _session.RemoveExpiry(FindOrCreateHash(_storage.Repository.GetId(typeof(RavenHash), key)));
        }

        private RavenSet FindOrCreateSet(string id)
        {
            return FindOrCreate(id, () => new RavenSet()
            {
                Id = id
            });
        }

        private RavenHash FindOrCreateHash(string id)
        {
            return FindOrCreate(id, () => new RavenHash()
            {
                Id = id
            });
        }

        private RavenList FindOrCreateList(string id)
        {
            return FindOrCreate(id, () => new RavenList()
            {
                Id = id
            });
        }

        private T FindOrCreate<T>(string id, Func<T> builder)
        {
            T entity = _session.Load<T>(id);
            if (entity == null)
            {
                entity = builder();
                _session.Store(entity);
            }
            return entity;
        }
    }
}
