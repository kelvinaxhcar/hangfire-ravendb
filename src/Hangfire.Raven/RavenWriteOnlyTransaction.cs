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
            this._storage = storage;
            this._patchRequests = new List<KeyValuePair<string, PatchRequest>>();
            this._session = this._storage.Repository.OpenSession();
            this._session.Advanced.UseOptimisticConcurrency = true;
            this._session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        }

        public override void Commit()
        {
            foreach (IGrouping<string, PatchRequest> grouping in (IEnumerable<IGrouping<string, PatchRequest>>)this._patchRequests.ToLookup<KeyValuePair<string, PatchRequest>, string, PatchRequest>((Func<KeyValuePair<string, PatchRequest>, string>)(a => a.Key), (Func<KeyValuePair<string, PatchRequest>, PatchRequest>)(a => a.Value)))
            {
                IGrouping<string, PatchRequest> item = grouping;
                foreach (ICommandData command in item.Select<PatchRequest, PatchCommandData>((Func<PatchRequest, PatchCommandData>)(x => new PatchCommandData(item.Key, (string)null, x, (PatchRequest)null))))
                    this._session.Advanced.Defer(command);
            }
            try
            {
                this._session.SaveChanges();
                this._session.Dispose();
            }
            catch
            {
                RavenWriteOnlyTransaction.Logger.Error("- Concurrency exception");
                this._session.Dispose();
                throw;
            }
            foreach (Action afterCommitCommand in this._afterCommitCommandQueue)
                afterCommitCommand();
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            this._session.SetExpiry<RavenJob>(this._storage.Repository.GetId(typeof(RavenJob), jobId), expireIn);
        }

        public override void PersistJob(string jobId)
        {
            this._session.RemoveExpiry<RavenJob>(this._storage.Repository.GetId(typeof(RavenJob), jobId));
        }

        public override void SetJobState(string jobId, IState state)
        {
            RavenJob ravenJob = this._session.Load<RavenJob>(this._storage.Repository.GetId(typeof(RavenJob), jobId));
            ravenJob.History.Insert(0, new StateHistoryDto()
            {
                StateName = state.Name,
                Data = (IDictionary<string, string>)state.SerializeData(),
                Reason = state.Reason,
                CreatedAt = DateTime.UtcNow
            });
            ravenJob.StateData = new StateData()
            {
                Name = state.Name,
                Data = (IDictionary<string, string>)state.SerializeData(),
                Reason = state.Reason
            };
        }

        public override void AddJobState(string jobId, IState state) => this.SetJobState(jobId, state);

        public override void AddRangeToSet(string key, IList<string> items)
        {
            key.ThrowIfNull(nameof(key));
            items.ThrowIfNull(nameof(items));
            RavenSet orCreateSet = this.FindOrCreateSet(this._storage.Repository.GetId(typeof(RavenSet), key));
            foreach (string key1 in (IEnumerable<string>)items)
                orCreateSet.Scores[key1] = 0.0;
        }

        public override void AddToQueue(string queue, string jobId)
        {
            IPersistentJobQueue jobQueue = this._storage.QueueProviders.GetProvider(queue).GetJobQueue();
            jobQueue.Enqueue(queue, jobId);
            if (!(jobQueue.GetType() == typeof(RavenJobQueue)))
                return;
            this._afterCommitCommandQueue.Enqueue((Action)(() => RavenJobQueue.NewItemInQueueEvent.Set()));
        }

        public override void IncrementCounter(string key)
        {
            this.IncrementCounter(key, TimeSpan.MinValue);
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            this.UpdateCounter(key, expireIn, 1);
        }

        public override void DecrementCounter(string key)
        {
            this.DecrementCounter(key, TimeSpan.MinValue);
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            this.UpdateCounter(key, expireIn, -1);
        }

        public void UpdateCounter(string key, TimeSpan expireIn, int value)
        {
            string id = this._storage.Repository.GetId(typeof(Counter), key);
            if (this._session.Load<Counter>(id) == null)
            {
                Counter entity = new Counter()
                {
                    Id = id,
                    Value = value
                };
                this._session.Store((object)entity);
                if (!(expireIn != TimeSpan.MinValue))
                    return;
                this._session.SetExpiry<Counter>(entity, expireIn);
            }
            else
                this._patchRequests.Add(new KeyValuePair<string, PatchRequest>(id, new PatchRequest()
                {
                    Script = string.Format("this.Value += {0}", (object)value)
                }));
        }

        public override void AddToSet(string key, string value) => this.AddToSet(key, value, 0.0);

        public override void AddToSet(string key, string value, double score)
        {
            this.FindOrCreateSet(this._storage.Repository.GetId(typeof(RavenSet), key)).Scores[value] = score;
        }

        public override void RemoveFromSet(string key, string value)
        {
            key.ThrowIfNull(nameof(key));
            RavenSet orCreateSet = this.FindOrCreateSet(this._storage.Repository.GetId(typeof(RavenSet), key));
            orCreateSet.Scores.Remove(value);
            if (orCreateSet.Scores.Count != 0)
                return;
            this._session.Delete<RavenSet>(orCreateSet);
        }

        public override void RemoveSet(string key)
        {
            key.ThrowIfNull(nameof(key));
            this._session.Delete(this._storage.Repository.GetId(typeof(RavenSet), key));
        }

        public override void ExpireSet([NotNull] string key, TimeSpan expireIn)
        {
            key.ThrowIfNull(nameof(key));
            this._session.SetExpiry<RavenSet>(this.FindOrCreateSet(this._storage.Repository.GetId(typeof(RavenSet), key)), expireIn);
        }

        public override void PersistSet([NotNull] string key)
        {
            key.ThrowIfNull(nameof(key));
            this._session.RemoveExpiry<RavenSet>(this.FindOrCreateSet(this._storage.Repository.GetId(typeof(RavenSet), key)));
        }

        public override void InsertToList(string key, string value)
        {
            key.ThrowIfNull(nameof(key));
            this.FindOrCreateList(this._storage.Repository.GetId(typeof(RavenList), key)).Values.Add(value);
        }

        public override void RemoveFromList(string key, string value)
        {
            key.ThrowIfNull(nameof(key));
            RavenList orCreateList = this.FindOrCreateList(this._storage.Repository.GetId(typeof(RavenList), key));
            orCreateList.Values.RemoveAll((Predicate<string>)(v => v == value));
            if (orCreateList.Values.Count != 0)
                return;
            this._session.Delete<RavenList>(orCreateList);
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            RavenList orCreateList = this.FindOrCreateList(this._storage.Repository.GetId(typeof(RavenList), key));
            orCreateList.Values = orCreateList.Values.Skip<string>(keepStartingFrom).Take<string>(keepEndingAt - keepStartingFrom + 1).ToList<string>();
            if (orCreateList.Values.Count != 0)
                return;
            this._session.Delete<RavenList>(orCreateList);
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            key.ThrowIfNull(nameof(key));
            this._session.SetExpiry<RavenList>(this.FindOrCreateList(this._storage.Repository.GetId(typeof(RavenList), key)), expireIn);
        }

        public override void PersistList(string key)
        {
            key.ThrowIfNull(nameof(key));
            this._session.RemoveExpiry<RavenList>(this.FindOrCreateList(this._storage.Repository.GetId(typeof(RavenList), key)));
        }

        public override void SetRangeInHash(
          string key,
          IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            key.ThrowIfNull(nameof(key));
            keyValuePairs.ThrowIfNull(nameof(keyValuePairs));
            RavenHash orCreateHash = this.FindOrCreateHash(this._storage.Repository.GetId(typeof(RavenHash), key));
            foreach (KeyValuePair<string, string> keyValuePair in keyValuePairs)
                orCreateHash.Fields[keyValuePair.Key] = keyValuePair.Value;
        }

        public override void RemoveHash(string key)
        {
            key.ThrowIfNull(nameof(key));
            this._session.Delete(this._storage.Repository.GetId(typeof(RavenHash), key));
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            key.ThrowIfNull(nameof(key));
            this._session.SetExpiry<RavenHash>(this.FindOrCreateHash(this._storage.Repository.GetId(typeof(RavenHash), key)), expireIn);
        }

        public override void PersistHash([NotNull] string key)
        {
            key.ThrowIfNull(nameof(key));
            this._session.RemoveExpiry<RavenHash>(this.FindOrCreateHash(this._storage.Repository.GetId(typeof(RavenHash), key)));
        }

        private RavenSet FindOrCreateSet(string id)
        {
            return this.FindOrCreate<RavenSet>(id, (Func<RavenSet>)(() => new RavenSet()
            {
                Id = id
            }));
        }

        private RavenHash FindOrCreateHash(string id)
        {
            return this.FindOrCreate<RavenHash>(id, (Func<RavenHash>)(() => new RavenHash()
            {
                Id = id
            }));
        }

        private RavenList FindOrCreateList(string id)
        {
            return this.FindOrCreate<RavenList>(id, (Func<RavenList>)(() => new RavenList()
            {
                Id = id
            }));
        }

        private T FindOrCreate<T>(string id, Func<T> builder)
        {
            T entity = this._session.Load<T>(id);
            if ((object)entity == null)
            {
                entity = builder();
                this._session.Store((object)entity);
            }
            return entity;
        }
    }
}
