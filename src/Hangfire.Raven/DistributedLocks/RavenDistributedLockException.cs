using System;

namespace Hangfire.Raven.DistributedLocks
{
    [Serializable]
    public class RavenDistributedLockException : Exception
    {
        public RavenDistributedLockException(string message)
          : base(message)
        {
        }

        public RavenDistributedLockException(string message, Exception innerException)
          : base(message, innerException)
        {
        }
    }
}
