using System.Security.Cryptography.X509Certificates;

namespace Hangfire.Raven
{
    public class RepositoryConfig
    {
        public string ConnectionUrl { get; set; }

        public string Database { get; set; }

        public X509Certificate2 Certificate { get; set; }
    }
}
