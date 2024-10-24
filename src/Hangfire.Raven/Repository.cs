using Hangfire.Raven.Extensions;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
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
            this._documentStore = documentStore;
            this._documentStore.Initialize();
            this._database = this._documentStore.Database;
        }

        public void ExecuteIndexes(List<AbstractIndexCreationTask> indexes)
        {
            this._documentStore.ExecuteIndexes((IEnumerable<IAbstractIndexCreationTask>)indexes, (string)null);
        }

        public void Destroy()
        {
            if (this._database == null || !this._documentStore.DatabaseExists(this._database))
                return;
            this._documentStore.Maintenance.Server.Send<DeleteDatabaseResult>((IServerOperation<DeleteDatabaseResult>)new DeleteDatabasesOperation(this._database, true));
        }

        public void Create()
        {
            if (this._database == null || this._documentStore.DatabaseExists(this._database))
                return;
            this._documentStore.Maintenance.Server.Send<DatabasePutResult>((IServerOperation<DatabasePutResult>)new CreateDatabaseOperation(new DatabaseRecord(this._database)));
            this._documentStore.Maintenance.Send<ConfigureExpirationOperationResult>((IMaintenanceOperation<ConfigureExpirationOperationResult>)new ConfigureExpirationOperation(new ExpirationConfiguration()
            {
                Disabled = false,
                DeleteFrequencyInSec = new long?(60L)
            }));
        }

        public void Dispose() => this._documentStore.Dispose();

        IDocumentSession IRepository.OpenSession() => this._documentStore.OpenSession();

        public string GetId(Type type, params string[] id)
        {
            return type.ToString() + "/" + string.Join("/", id);
        }
    }
}
