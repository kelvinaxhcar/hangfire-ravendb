using System;
using System.Collections.Generic;

namespace Hangfire.Raven.Entities
{
    public class RavenServer
    {
        public string Id { get; set; }

        public DateTime LastHeartbeat { get; set; }

        public RavenServer.ServerData Data { get; set; }

        public class ServerData
        {
            public int WorkerCount { get; set; }

            public IEnumerable<string> Queues { get; set; }

            public DateTime? StartedAt { get; set; }
        }
    }
}
