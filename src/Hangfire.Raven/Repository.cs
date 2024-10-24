using Hangfire.Raven.Extensions;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using System;
using System.Collections.Generic;

namespace Hangfire.Raven
{
    public class Repository : IRepository, IDisposable
    {
        private DocumentStore _documentStore;
        private readonly string _database;

        public Repository(RepositoryConfig config)
        {
            DocumentStore documentStore = new DocumentStore();
            documentStore.Urls = new string[1]
            {
        config.ConnectionUrl
            };
            documentStore.Database = config.Database;
            documentStore.Certificate = config.Certificate;
            _documentStore = documentStore;
            _documentStore.Initialize();
            _database = _documentStore.Database;
        }

        public void ExecuteIndexes(List<AbstractIndexCreationTask> indexes)
        {
            _documentStore.ExecuteIndexes(indexes, null);
        }

        public void Destroy()
        {
            if (_database == null || !_documentStore.DatabaseExists(_database))
                return;
            _documentStore.Maintenance.Server.Send(new DeleteDatabasesOperation(_database, true));
        }

        public void Create()
        {
            if (_database == null || _documentStore.DatabaseExists(_database))
                return;
            _documentStore.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(_database)));
            _documentStore.Maintenance.Send(new ConfigureExpirationOperation(new ExpirationConfiguration()
            {
                Disabled = false,
                DeleteFrequencyInSec = new long?(60L)
            }));
        }

        public void Dispose() => _documentStore.Dispose();

        IDocumentSession IRepository.OpenSession() => _documentStore.OpenSession();

        public string GetId(Type type, params string[] id)
        {
            return type.ToString() + "/" + string.Join("/", id);
        }
    }
}
