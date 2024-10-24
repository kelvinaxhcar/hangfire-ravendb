namespace Hangfire.Raven.Entities
{
    public class DistributedLock
    {
        public string Id => "DistributedLocks/" + this.Resource;

        public string Resource { get; set; }

        public string ClientId { get; set; }
    }
}
