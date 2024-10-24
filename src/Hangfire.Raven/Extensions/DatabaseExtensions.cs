using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;

namespace Hangfire.Raven.Extensions
{
    public static class DatabaseExtensions
    {
        public static bool DatabaseExists(this IDocumentStore documentStore, string database)
        {
            GetDatabaseRecordOperation operation = new GetDatabaseRecordOperation(database);
            return documentStore.Maintenance.Server.Send<DatabaseRecordWithEtag>((IServerOperation<DatabaseRecordWithEtag>)operation) != null;
        }
    }
}
