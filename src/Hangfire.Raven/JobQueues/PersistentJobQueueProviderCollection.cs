﻿using Hangfire.Raven.Extensions;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Hangfire.Raven.JobQueues
{
    public class PersistentJobQueueProviderCollection :
      IEnumerable<IPersistentJobQueueProvider>,
      IEnumerable
    {
        private readonly List<IPersistentJobQueueProvider> _providers = new List<IPersistentJobQueueProvider>();
        private readonly Dictionary<string, IPersistentJobQueueProvider> _providersByQueue = new Dictionary<string, IPersistentJobQueueProvider>((IEqualityComparer<string>)StringComparer.OrdinalIgnoreCase);
        private readonly IPersistentJobQueueProvider _defaultProvider;

        public PersistentJobQueueProviderCollection(IPersistentJobQueueProvider defaultProvider)
        {
            defaultProvider.ThrowIfNull(nameof(defaultProvider));
            this._defaultProvider = defaultProvider;
            this._providers.Add(this._defaultProvider);
        }

        public void Add(IPersistentJobQueueProvider provider, IEnumerable<string> queues)
        {
            provider.ThrowIfNull(nameof(provider));
            queues.ThrowIfNull(nameof(queues));
            this._providers.Add(provider);
            foreach (string queue in queues)
                this._providersByQueue.Add(queue, provider);
        }

        public IPersistentJobQueueProvider GetProvider(string queue)
        {
            return this._providersByQueue.ContainsKey(queue) ? this._providersByQueue[queue] : this._defaultProvider;
        }

        public IEnumerator<IPersistentJobQueueProvider> GetEnumerator()
        {
            return (IEnumerator<IPersistentJobQueueProvider>)this._providers.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() => (IEnumerator)this.GetEnumerator();
    }
}
