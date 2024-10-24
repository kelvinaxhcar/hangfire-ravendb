using Raven.Client.Documents.Session;
using System;
using System.Collections.Generic;

namespace Hangfire.Raven.Extensions
{
    public static class IDocumentSessionExtensions
    {
        private static IMetadataDictionary GetMetadataForId<T>(this IDocumentSession session, string id)
        {
            return session.Advanced.GetMetadataFor<T>(session.Load<T>(id));
        }

        private static IMetadataDictionary GetMetadataForObject<T>(this IDocumentSession session, T obj)
        {
            return session.Advanced.GetMetadataFor<T>(obj);
        }

        public static void SetExpiry<T>(this IDocumentSession session, string id, TimeSpan expireIn)
        {
            IDocumentSessionExtensions.SetExpiry(session.GetMetadataForId<T>(id), expireIn);
        }

        public static void SetExpiry<T>(this IDocumentSession session, T obj, TimeSpan expireIn)
        {
            IDocumentSessionExtensions.SetExpiry(session.GetMetadataForObject<T>(obj), expireIn);
        }

        public static void SetExpiry<T>(this IDocumentSession session, T obj, DateTime expireAt)
        {
            IDocumentSessionExtensions.SetExpiry(session.GetMetadataForObject<T>(obj), expireAt);
        }

        private static void SetExpiry(IMetadataDictionary metadata, DateTime expireAt)
        {
            metadata["@expires"] = (object)expireAt.ToString("O");
        }

        private static void SetExpiry(IMetadataDictionary metadata, TimeSpan expireIn)
        {
            metadata["@expires"] = (object)(DateTime.UtcNow + expireIn).ToString("O");
        }

        public static void RemoveExpiry<T>(this IDocumentSession session, string id)
        {
            IDocumentSessionExtensions.RemoveExpiry(session.GetMetadataForId<T>(id));
        }

        public static void RemoveExpiry<T>(this IDocumentSession session, T obj)
        {
            IDocumentSessionExtensions.RemoveExpiry(session.GetMetadataForObject<T>(obj));
        }

        public static void RemoveExpiry(IMetadataDictionary metadata)
        {
            ((IDictionary<string, object>)metadata).Remove("@expires");
        }

        public static DateTime? GetExpiry<T>(this IDocumentSession session, string id)
        {
            return IDocumentSessionExtensions.GetExpiry(session.GetMetadataForId<T>(id));
        }

        public static DateTime? GetExpiry<T>(this IDocumentSession session, T obj)
        {
            return IDocumentSessionExtensions.GetExpiry(session.GetMetadataForObject<T>(obj));
        }

        private static DateTime? GetExpiry(IMetadataDictionary metadata)
        {
            return metadata.ContainsKey("@expires") ? new DateTime?(DateTime.Parse(metadata["@expires"].ToString())) : new DateTime?();
        }
    }
}
