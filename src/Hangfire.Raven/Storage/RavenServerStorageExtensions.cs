using Hangfire.Raven.Extensions;
using Raven.Client.Documents.Session;
using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;

namespace Hangfire.Raven.Storage
{
    public static class RavenServerStorageExtensions
    {
        public static void AddExpire<T>(
          this IAsyncAdvancedSessionOperations advanced,
          T obj,
          DateTime dateTime)
        {
            advanced.GetMetadataFor<T>(obj)["@expires"] = (object)dateTime;
        }

        public static void RemoveExpire<T>(this IAsyncAdvancedSessionOperations advanced, T obj)
        {
            ((IDictionary<string, object>)advanced.GetMetadataFor<T>(obj)).Remove("Raven-Expiration-Date");
        }

        public static DateTime? GetExpire<T>(this IAsyncAdvancedSessionOperations advanced, T obj)
        {
            object obj1;
            return advanced.GetMetadataFor<T>(obj).TryGetValue("Raven-Expiration-Date", out obj1) ? new DateTime?((DateTime)obj1) : new DateTime?();
        }

        public static IGlobalConfiguration<RavenStorage> UseRavenStorage(
          this IGlobalConfiguration configuration,
          RavenStorage storage)
        {
            storage.ThrowIfNull(nameof(storage));
            return configuration.UseStorage<RavenStorage>(storage);
        }

        public static IGlobalConfiguration<RavenStorage> UseRavenStorage(
          this IGlobalConfiguration configuration,
          string connectionUrl,
          string database)
        {
            configuration.ThrowIfNull(nameof(configuration));
            connectionUrl.ThrowIfNull(nameof(connectionUrl));
            database.ThrowIfNull(nameof(database));
            RavenStorage storage = connectionUrl.StartsWith("http") ? new RavenStorage(new RepositoryConfig()
            {
                ConnectionUrl = connectionUrl,
                Database = database
            }) : throw new ArgumentException("Connection Url must begin with http or https!");
            return configuration.UseStorage<RavenStorage>(storage);
        }

        public static IGlobalConfiguration<RavenStorage> UseRavenStorage(
          this IGlobalConfiguration configuration,
          string connectionUrl,
          string database,
          X509Certificate2 certificate)
        {
            configuration.ThrowIfNull(nameof(configuration));
            connectionUrl.ThrowIfNull(nameof(connectionUrl));
            database.ThrowIfNull(nameof(database));
            if (!connectionUrl.StartsWith("http"))
                throw new ArgumentException("Connection Url must begin with http or https!");
            RavenStorage storage = new RavenStorage(new RepositoryConfig()
            {
                ConnectionUrl = connectionUrl,
                Database = database,
                Certificate = certificate
            });
            return configuration.UseStorage<RavenStorage>(storage);
        }

        public static IGlobalConfiguration<RavenStorage> UseRavenStorage(
          this IGlobalConfiguration configuration,
          string connectionUrl,
          string database,
          RavenStorageOptions options)
        {
            configuration.ThrowIfNull(nameof(configuration));
            connectionUrl.ThrowIfNull(nameof(connectionUrl));
            database.ThrowIfNull(nameof(database));
            options.ThrowIfNull(nameof(options));
            if (!connectionUrl.StartsWith("http"))
                throw new ArgumentException("Connection Url must begin with http or https!");
            RavenStorage storage = new RavenStorage(new RepositoryConfig()
            {
                ConnectionUrl = connectionUrl,
                Database = database
            }, options);
            return configuration.UseStorage<RavenStorage>(storage);
        }
    }
}
