using System.Collections.Generic;

namespace Hangfire.Raven.Entities
{
    public class RavenSet
    {
        public RavenSet() => Scores = new Dictionary<string, double>();

        public string Id { get; set; }

        public Dictionary<string, double> Scores { get; set; }
    }
}
