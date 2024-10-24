namespace Hangfire.Raven.JobQueues
{
    public interface IPersistentJobQueueProvider
    {
        IPersistentJobQueue GetJobQueue();

        IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi();
    }
}
