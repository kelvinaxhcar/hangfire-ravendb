using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using System;
using System.Collections.Generic;

namespace Hangfire.Raven
{
    public interface IRepository : IDisposable
    {
        void Create();

        void Destroy();

        void ExecuteIndexes(List<AbstractIndexCreationTask> indexes);

        string GetId(Type type, params string[] id);

        IDocumentSession OpenSession();
    }
}
