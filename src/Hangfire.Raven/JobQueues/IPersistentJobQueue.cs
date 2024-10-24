using Hangfire.Storage;
using System.Threading;

namespace Hangfire.Raven.JobQueues
{
    public interface IPersistentJobQueue
    {
        IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken);

        void Enqueue(string queue, string jobId);
    }
}
