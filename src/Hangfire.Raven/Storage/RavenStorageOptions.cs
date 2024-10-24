using System;
using System.Collections.Generic;

namespace Hangfire.Raven.Storage
{
    public class RavenStorageOptions
    {
        private readonly string _clientId = (string)null;
        private TimeSpan _queuePollInterval;
        private TimeSpan _distributedLockLifetime;

        public RavenStorageOptions()
        {
            QueuePollInterval = TimeSpan.FromSeconds(15.0);
            InvisibilityTimeout = TimeSpan.FromMinutes(30.0);
            JobExpirationCheckInterval = TimeSpan.FromHours(1.0);
            CountersAggregateInterval = TimeSpan.FromMinutes(5.0);
            TransactionTimeout = TimeSpan.FromMinutes(1.0);
            DistributedLockLifetime = TimeSpan.FromSeconds(30.0);
            _clientId = Guid.NewGuid().ToString().Replace("-", string.Empty);
        }

        public TimeSpan QueuePollInterval
        {
            get => _queuePollInterval;
            set
            {
                string message = string.Format("The QueuePollInterval property value should be positive. Given: {0}.", (object)value);
                if (value == TimeSpan.Zero)
                    throw new ArgumentException(message, nameof(value));
                _queuePollInterval = !(value != value.Duration()) ? value : throw new ArgumentException(message, nameof(value));
            }
        }

        public TimeSpan InvisibilityTimeout { get; set; }

        public TimeSpan JobExpirationCheckInterval { get; set; }

        public TimeSpan CountersAggregateInterval { get; set; }

        public TimeSpan TransactionTimeout { get; set; }

        public TimeSpan DistributedLockLifetime
        {
            get => _distributedLockLifetime;
            set
            {
                string message = string.Format("The DistributedLockLifetime property value should be positive. Given: {0}.", (object)value);
                if (value == TimeSpan.Zero)
                    throw new ArgumentException(message, nameof(value));
                _distributedLockLifetime = !(value != value.Duration()) ? value : throw new ArgumentException(message, nameof(value));
            }
        }

        public IEnumerable<string> QueueNames { get; set; }

        public string ClientId => _clientId;
    }
}
