using Hangfire.Raven.Entities;
using System.Collections.Generic;

namespace Hangfire.Raven.JobQueues
{
    public interface IPersistentJobQueueMonitoringApi
    {
        IEnumerable<string> GetQueues();

        IEnumerable<string> GetEnqueuedJobIds(string queue, int from, int perPage);

        IEnumerable<string> GetFetchedJobIds(string queue, int from, int perPage);

        EnqueuedAndFetchedCount GetEnqueuedAndFetchedCount(string queue);
    }
}
