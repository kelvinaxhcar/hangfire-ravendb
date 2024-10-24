using Hangfire.Logging;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.JobQueues;
using Hangfire.Storage;

namespace Hangfire.Raven.Storage
{
    public class RavenStorage : JobStorage
    {
        private readonly RavenStorageOptions _options;
        private readonly IRepository _repository;

        public RavenStorage(RepositoryConfig config)
          : this(config, new RavenStorageOptions())
        {
        }

        public RavenStorage(RepositoryConfig config, RavenStorageOptions options)
          : this((IRepository)new Hangfire.Raven.Repository(config), options)
        {
        }

        public RavenStorage(IRepository repository)
          : this(repository, new RavenStorageOptions())
        {
        }

        public RavenStorage(IRepository repository, RavenStorageOptions options)
        {
            repository.ThrowIfNull(nameof(repository));
            options.ThrowIfNull(nameof(options));
            _options = options;
            _repository = repository;
            _repository.Create();
            InitializeQueueProviders();
        }

        public RavenStorageOptions Options => _options;

        public IRepository Repository => _repository;

        public virtual PersistentJobQueueProviderCollection QueueProviders { get; private set; }

        public override IMonitoringApi GetMonitoringApi()
        {
            return (IMonitoringApi)new RavenStorageMonitoringApi(this);
        }

        public override IStorageConnection GetConnection()
        {
            return (IStorageConnection)new RavenConnection(this);
        }

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for Raven job storage:");
        }

        private void InitializeQueueProviders()
        {
            QueueProviders = new PersistentJobQueueProviderCollection((IPersistentJobQueueProvider)new RavenJobQueueProvider(this, _options));
        }
    }
}
