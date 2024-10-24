using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.JobQueues;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using Raven.Client.Documents.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Hangfire.Raven.Storage
{
    public class RavenStorageMonitoringApi : IMonitoringApi
    {
        private readonly RavenStorage _storage;
        private const int DefaultBatchSize = 1000;

        public RavenStorageMonitoringApi([NotNull] RavenStorage storage)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        }

        public long EnqueuedCount(string queue)
        {
            var queueApi = GetQueueApi(queue);
            var counts = queueApi.GetEnqueuedAndFetchedCount(queue);
            return counts.EnqueuedCount ?? 0;
        }

        public long FetchedCount(string queue)
        {
            var queueApi = GetQueueApi(queue);
            var counts = queueApi.GetEnqueuedAndFetchedCount(queue);
            return counts.FetchedCount ?? 0;
        }

        public long DeletedListCount() => GetNumberOfJobsByStateName(DeletedState.StateName);
        public long FailedCount() => GetNumberOfJobsByStateName(FailedState.StateName);
        public long ProcessingCount() => GetNumberOfJobsByStateName(ProcessingState.StateName);
        public long ScheduledCount() => GetNumberOfJobsByStateName(ScheduledState.StateName);
        public long SucceededListCount() => GetNumberOfJobsByStateName(SucceededState.StateName);

        private long GetNumberOfJobsByStateName(string stateName)
        {
            var session = _storage.Repository.OpenSession();
            return session.Query<RavenJob>()
                         .Count(x => x.StateData.Name == stateName);
        }

        public IDictionary<DateTime, long> FailedByDatesCount() => GetTimelineStats("failed");
        public IDictionary<DateTime, long> SucceededByDatesCount() => GetTimelineStats("succeeded");
        public IDictionary<DateTime, long> HourlyFailedJobs() => GetHourlyTimelineStats("failed");
        public IDictionary<DateTime, long> HourlySucceededJobs() => GetHourlyTimelineStats("succeeded");

        private Dictionary<DateTime, long> GetHourlyTimelineStats(string type)
        {
            var dates = Enumerable.Range(0, 24)
                                .Select(i => DateTime.UtcNow.AddHours(-i))
                                .ToList();

            return GetTimelineStats(
                dates,
                x => $"stats:{type}:{x:yyyy-MM-dd-HH}");
        }

        private Dictionary<DateTime, long> GetTimelineStats(string type)
        {
            var dates = Enumerable.Range(0, 7)
                                .Select(i => DateTime.UtcNow.Date.AddDays(-i))
                                .ToList();

            return GetTimelineStats(
                dates,
                x => $"stats:{type}:{x:yyyy-MM-dd}");
        }

        private Dictionary<DateTime, long> GetTimelineStats(
            List<DateTime> dates,
            Func<DateTime, string> formatAction)
        {
            var session = _storage.Repository.OpenSession();
            var result = new Dictionary<DateTime, long>();

            foreach (var date in dates)
            {
                var id = _storage.Repository.GetId(typeof(Counter), formatAction(date));
                var counter = session.Load<Counter>(id);
                result[date] = counter?.Value ?? 0;
            }

            return result;
        }

        public StatisticsDto GetStatistics()
        {
            var session = _storage.Repository.OpenSession();

            session.Query<RavenServer>()
                   .Statistics(out var stats)
                   .Take(0)
                   .ToList();

            var recurringJobsSet = session.Load<RavenSet>(
                _storage.Repository.GetId(typeof(RavenSet), "recurring-jobs"));

            var jobStateCounts = session.Query<RavenJob>()
                                      .GroupBy(x => x.StateData.Name)
                                      .Select(x => new { State = x.Key, Count = x.Count() })
                                      .ToDictionary(x => x.State, x => x.Count);

            var queueCount = session.Query<JobQueue>().Count();

            return new StatisticsDto
            {
                Servers = stats.TotalResults,
                Queues = queueCount,
                Recurring = recurringJobsSet?.Scores?.Count ?? 0,
                Succeeded = GetStateCount(jobStateCounts, SucceededState.StateName),
                Scheduled = GetStateCount(jobStateCounts, ScheduledState.StateName),
                Enqueued = GetStateCount(jobStateCounts, EnqueuedState.StateName),
                Failed = GetStateCount(jobStateCounts, FailedState.StateName),
                Processing = GetStateCount(jobStateCounts, ProcessingState.StateName),
                Deleted = GetStateCount(jobStateCounts, DeletedState.StateName)
            };
        }

        private static long GetStateCount(Dictionary<string, int> stateCounts, string stateName)
        {
            return stateCounts.TryGetValue(stateName, out var count) ? count : 0;
        }

        public JobList<DeletedJobDto> DeletedJobs(int from, int count)
        {

            return GetJobs(from, count, DeletedState.StateName, (job, deserializedJob, stateData) =>

                new DeletedJobDto
                {
                    Job = deserializedJob,
                    DeletedAt = JobHelper.DeserializeNullableDateTime(stateData.FirstOrDefault(x=> x.Key == "DeletedAt").Value)
                });
        }

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
        {
            var jobIds = GetQueueApi(queue).GetEnqueuedJobIds(queue, from, perPage);
            return GetJobsById<EnqueuedJobDto>(jobIds, CreateEnqueuedJobDto);
        }

        private EnqueuedJobDto CreateEnqueuedJobDto(RavenJob job, Job deserializedJob, Dictionary<string, string> stateData)
        {
            return new EnqueuedJobDto
            {
                Job = deserializedJob,
                State = job.StateData?.Name,
                EnqueuedAt = job.StateData?.Name == EnqueuedState.StateName
                    ? JobHelper.DeserializeNullableDateTime(stateData.FirstOrDefault(x => x.Key == "EnqueuedAt").Value)
                    : null
            };
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
        {
            var jobIds = GetQueueApi(queue).GetFetchedJobIds(queue, from, perPage);
            return GetJobsById<FetchedJobDto>(jobIds, CreateFetchedJobDto);
        }

        private FetchedJobDto CreateFetchedJobDto(RavenJob job, Job deserializedJob, Dictionary<string, string> stateData)
        {
            return new FetchedJobDto
            {
                Job = deserializedJob,
                State = job.StateData?.Name,
                FetchedAt = job.StateData?.Name == ProcessingState.StateName
                    ? JobHelper.DeserializeNullableDateTime(stateData.FirstOrDefault(x => x.Key == "StartedAt").Value)
                    : null
            };
        }

        public JobDetailsDto JobDetails(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            var session = _storage.Repository.OpenSession();
            var id = _storage.Repository.GetId(typeof(RavenJob), jobId);
            var job = session.Load<RavenJob>(id);

            if (job == null) return null;

            return new JobDetailsDto
            {
                CreatedAt = job.CreatedAt,
                ExpireAt = session.GetExpiry<RavenJob>(job),
                Job = DeserializeJob(job.InvocationData),
                History = job.History,
                Properties = job.Parameters
            };
        }

        private Job DeserializeJob(InvocationData invocationData)
        {
            try
            {
                return invocationData.Deserialize();
            }
            catch (JobLoadException)
            {
                return null;
            }
        }

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            var session = _storage.Repository.OpenSession();

            var queueGroups = session.Query<JobQueue>()
                                   .ToList()
                                   .GroupBy(x => x.Queue)
                                   .Select(g => new QueueWithTopEnqueuedJobsDto
                                   {
                                       Name = g.Key,
                                       Length = g.Count(x => !x.FetchedAt.HasValue),
                                       Fetched = g.Count(x => x.FetchedAt.HasValue),
                                       FirstJobs = GetJobsById<EnqueuedJobDto>(
                                           g.Take(5).Select(x => x.JobId),
                                           CreateEnqueuedJobDto)
                                   })
                                   .ToList();

            return queueGroups;
        }

        public IList<ServerDto> Servers()
        {
            var session = _storage.Repository.OpenSession();

            return session.Query<RavenServer>()
                         .ToList()
                         .Select(server => new ServerDto
                         {
                             Name = server.Id.Split('/')[1],
                             Heartbeat = server.LastHeartbeat,
                             Queues = server.Data.Queues.ToList(),
                             StartedAt = server.Data.StartedAt ?? DateTime.MinValue,
                             WorkersCount = server.Data.WorkerCount
                         })
                         .ToList();
        }

        private JobList<T> GetJobs<T>(
            int from,
            int count,
            string stateName,
            Func<RavenJob, Job, Dictionary<string, string>, T> selector)
        {
            var session = _storage.Repository.OpenSession();

            var jobs = session.Query<RavenJob>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Where(x => x.StateData.Name == stateName)
                            .OrderByDescending(x => x.CreatedAt)
                            .Skip(from)
                            .Take(count)
                            .ToList();

            return new JobList<T>(jobs.Select(job =>
            {
                var stateData = job.StateData?.Data != null
                    ? new Dictionary<string, string>(job.StateData.Data, StringComparer.OrdinalIgnoreCase)
                    : null;
                var dto = selector(job, DeserializeJob(job.InvocationData), stateData);
                return new KeyValuePair<string, T>(job.Id.Split('/')[1], dto);
            }));
        }

        private JobList<T> GetJobsById<T>(
            IEnumerable<string> jobIds,
            Func<RavenJob, Job, Dictionary<string, string>, T> selector)
        {
            var session = _storage.Repository.OpenSession();

            var jobs = session.Load<RavenJob>(
                jobIds.Select(id => _storage.Repository.GetId(typeof(RavenJob), id)))
                .Where(kvp => kvp.Value != null)
                .Select(kvp => kvp.Value)
                .ToList();

            return new JobList<T>(jobs.Select(job =>
            {
                var stateData = job.StateData?.Data != null
                    ? new Dictionary<string, string>(job.StateData.Data, StringComparer.OrdinalIgnoreCase)
                    : null;
                var dto = selector(job, DeserializeJob(job.InvocationData), stateData);
                return new KeyValuePair<string, T>(job.Id.Split('/')[1], dto);
            }));
        }



        //public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
        //{
        //    return GetJobs(from, count, ProcessingState.StateName, (job, deserializedJob, stateData) =>
        //        new ProcessingJobDto
        //        {
        //            Job = deserializedJob,
        //            ServerId = stateData.FirstOrDefault(x => x.Key == "ServerId").Value ?? stateData.FirstOrDefault(x => x.Key == "ServerName").Value,
        //            StartedAt = JobHelper.DeserializeNullableDateTime(stateData.FirstOrDefault(x => x.Key == "StartedAt").Value)
        //        });
        //}

        //public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        //{
        //    return GetJobs(from, count, ScheduledState.StateName, (job, deserializedJob, stateData) =>
        //        new ScheduledJobDto
        //        {
        //            Job = deserializedJob,
        //            EnqueueAt = JobHelper.DeserializeDateTime(stateData.FirstOrDefault(x => x.Key == "EnqueueAt").Value),
        //            ScheduledAt = JobHelper.DeserializeNullableDateTime(stateData.FirstOrDefault(x => x.Key == "ScheduledAt").Value)
        //        });
        //}

        //public JobList<SucceededJobDto> SucceededJobs(int from, int count)
        //{
        //    return GetJobs(from, count, SucceededState.StateName, (job, deserializedJob, stateData) =>
        //    {
        //        var performanceDuration = stateData.FirstOrDefault(x => x.Key == "PerformanceDuration").Value;
        //        var latency = stateData.FirstOrDefault(x => x.Key == "Latency").Value;

        //        long? totalDuration = null;
        //        if (!string.IsNullOrEmpty(performanceDuration) && !string.IsNullOrEmpty(latency))
        //        {
        //            if (long.TryParse(performanceDuration, out var duration) &&
        //                long.TryParse(latency, out var lat))
        //            {
        //                totalDuration = duration + lat;
        //            }
        //        }

        //        return new SucceededJobDto
        //        {
        //            Job = deserializedJob,
        //            Result = stateData.FirstOrDefault(x => x.Key == "Result").Value,
        //            TotalDuration = totalDuration,
        //            SucceededAt = JobHelper.DeserializeNullableDateTime(stateData.FirstOrDefault(x => x.Key == "SucceededAt").Value),
        //            InSucceededState = true
        //        };
        //    });
        //}

        //public JobList<FailedJobDto> FailedJobs(int from, int count)
        //{
        //    return GetJobs(from, count, FailedState.StateName, (job, deserializedJob, stateData) =>
        //        new FailedJobDto
        //        {
        //            Job = deserializedJob,
        //            Reason = job.StateData?.Reason,
        //            ExceptionDetails = stateData.FirstOrDefault(x => x.Key == "ExceptionDetails").Value,
        //            ExceptionMessage = stateData.FirstOrDefault(x => x.Key == "ExceptionMessage").Value,
        //            ExceptionType = stateData.FirstOrDefault(x => x.Key == "ExceptionType").Value,
        //            FailedAt = JobHelper.DeserializeNullableDateTime(stateData.FirstOrDefault(x => x.Key == "FailedAt").Value)
        //        });
        //}

        public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
        {
            return GetJobs<ProcessingJobDto>(from, count, ProcessingState.StateName, (jsonJob, job, stateData) => new ProcessingJobDto
            {
                Job = job,
                ServerId = stateData.ContainsKey("ServerId") ? stateData["ServerId"] : stateData["ServerName"],
                StartedAt = new DateTime?(JobHelper.DeserializeDateTime(stateData["StartedAt"]))
            });
        }

        public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {
            return GetJobs<ScheduledJobDto>(from, count, ScheduledState.StateName, (jsonJob, job, stateData) => new ScheduledJobDto
            {
                Job = job,
                EnqueueAt = JobHelper.DeserializeDateTime(stateData["EnqueueAt"]),
                ScheduledAt = new DateTime?(JobHelper.DeserializeDateTime(stateData["ScheduledAt"]))
            });
        }

        public JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            return GetJobs<SucceededJobDto>(from, count, SucceededState.StateName, (jsonJob, job, stateData) => new SucceededJobDto
            {
                Job = job,
                InSucceededState = true,
                Result = stateData.ContainsKey("Result") ? (object)stateData["Result"] : (object)(string)null,
                TotalDuration = !stateData.ContainsKey("PerformanceDuration") || !stateData.ContainsKey("Latency")
                    ? new long?()
                    : new long?(long.Parse(stateData["PerformanceDuration"]) + long.Parse(stateData["Latency"])),
                SucceededAt = JobHelper.DeserializeNullableDateTime(stateData["SucceededAt"])
            });
        }

        public JobList<FailedJobDto> FailedJobs(int from, int count)
        {
            return GetJobs<FailedJobDto>(from, count, FailedState.StateName, (jsonJob, job, stateData) => new FailedJobDto
            {
                Job = job,
                Reason = jsonJob.StateData.Reason,
                ExceptionDetails = stateData["ExceptionDetails"],
                ExceptionMessage = stateData["ExceptionMessage"],
                ExceptionType = stateData["ExceptionType"],
                FailedAt = JobHelper.DeserializeNullableDateTime(stateData["FailedAt"])
            });
        }

        private IPersistentJobQueueMonitoringApi GetQueueApi(string queueName)
        {
            return _storage.QueueProviders.GetProvider(queueName).GetJobQueueMonitoringApi();
        }
    }
}