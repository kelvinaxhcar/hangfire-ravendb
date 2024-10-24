using Hangfire.Annotations;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.Storage;

namespace Hangfire.Raven.JobQueues
{
    public class RavenJobQueueProvider : IPersistentJobQueueProvider
    {
        private readonly IPersistentJobQueue _jobQueue;
        private readonly IPersistentJobQueueMonitoringApi _monitoringApi;

        public RavenJobQueueProvider([NotNull] RavenStorage storage, [NotNull] RavenStorageOptions options)
        {
            storage.ThrowIfNull(nameof(storage));
            options.ThrowIfNull(nameof(options));
            this._jobQueue = (IPersistentJobQueue)new RavenJobQueue(storage, options);
            this._monitoringApi = (IPersistentJobQueueMonitoringApi)new RavenJobQueueMonitoringApi(storage);
        }

        public IPersistentJobQueue GetJobQueue() => this._jobQueue;

        public IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi() => this._monitoringApi;
    }
}
