using Hangfire.Logging;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.Storage;
using Hangfire.Storage;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Hangfire.Raven.DistributedLocks
{
    public class RavenDistributedLock : IDisposable
    {
        private static readonly ILog Logger = LogProvider.For<RavenDistributedLock>();
        private static readonly ThreadLocal<Dictionary<string, int>> AcquiredLocks = new ThreadLocal<Dictionary<string, int>>(() => new Dictionary<string, int>());
        private static readonly TimeSpan KeepAliveInterval = TimeSpan.FromMinutes(1.0);
        private RavenStorage _storage;
        private string _resource;
        private readonly RavenStorageOptions _options;
        private DistributedLock _distributedLock;
        private Timer _heartbeatTimer;
        private bool _completed;
        private readonly object _lockObject = new object();

        private string EventWaitHandleName => GetType().FullName + "." + _resource;

        public RavenDistributedLock(
          RavenStorage storage,
          string resource,
          TimeSpan timeout,
          RavenStorageOptions options)
        {
            storage.ThrowIfNull(nameof(storage));
            if (string.IsNullOrEmpty(resource))
                throw new ArgumentNullException(nameof(resource));
            if (timeout.TotalSeconds > (double)int.MaxValue)
                throw new ArgumentException(string.Format("The timeout specified is too large. Please supply a timeout equal to or less than {0} seconds", int.MaxValue), nameof(timeout));
            options.ThrowIfNull(nameof(options));
            _storage = storage;
            _resource = resource;
            _options = options;
            if (!AcquiredLocks.Value.ContainsKey(_resource) || AcquiredLocks.Value[_resource] == 0)
            {
                Acquire(timeout);
                AcquiredLocks.Value[_resource] = 1;
                StartHeartBeat();
            }
            else
                AcquiredLocks.Value[_resource]++;
        }

        public void Dispose()
        {
            if (_completed)
                return;
            _completed = true;
            if (!AcquiredLocks.Value.ContainsKey(_resource))
                return;
            AcquiredLocks.Value[_resource]--;
            if (AcquiredLocks.Value[_resource] > 0)
                return;
            lock (_lockObject)
            {
                AcquiredLocks.Value.Remove(_resource);
                if (_heartbeatTimer != null)
                {
                    _heartbeatTimer.Dispose();
                    _heartbeatTimer = null;
                }
                Release();
            }
        }

        private void Acquire(TimeSpan timeout)
        {
            try
            {
                DateTime dateTime = DateTime.Now.Add(timeout);
                int millisecondsTimeout = timeout.TotalMilliseconds > 10000.0 ? 2000 : (int)(timeout.TotalMilliseconds / 5.0);
                while (dateTime >= DateTime.Now)
                {
                    _distributedLock = new DistributedLock()
                    {
                        ClientId = _storage.Options.ClientId,
                        Resource = _resource
                    };
                    using (IDocumentSession session = _storage.Repository.OpenSession())
                    {
                        session.Advanced.UseOptimisticConcurrency = false;
                        session.Store((object)_distributedLock);
                        session.SetExpiry(_distributedLock, _options.DistributedLockLifetime);
                        try
                        {
                            session.SaveChanges();
                            return;
                        }
                        catch (ConcurrencyException)
                        {
                            _distributedLock = null;
                            try
                            {
                                new EventWaitHandle(false, EventResetMode.AutoReset, EventWaitHandleName).WaitOne(millisecondsTimeout);
                            }
                            catch (PlatformNotSupportedException)
                            {
                                Thread.Sleep(millisecondsTimeout);
                            }
                        }
                    }
                }
                throw new DistributedLockTimeoutException(_resource);
            }
            catch (DistributedLockTimeoutException ex)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new RavenDistributedLockException("Could not place a lock on the resource '" + _resource + "': Check inner exception for details.", ex);
            }
        }

        private void Release()
        {
            try
            {
                if (_distributedLock != null)
                {
                    using (IDocumentSession documentSession = _storage.Repository.OpenSession())
                    {
                        documentSession.Delete(_distributedLock.Id);
                        documentSession.SaveChanges();
                        _distributedLock = null;
                    }
                }
                if (!EventWaitHandle.TryOpenExisting(EventWaitHandleName, out EventWaitHandle result))
                {
                    return;
                }

                result.Set();
            }
            catch (PlatformNotSupportedException ex)
            {
            }
            catch (Exception ex)
            {
                _distributedLock = null;
                throw new RavenDistributedLockException("Could not release a lock on the resource '" + _resource + "': Check inner exception for details.", ex);
            }
        }

        private void StartHeartBeat()
        {
            Logger.InfoFormat(".Starting heartbeat for resource: {0}", _resource);
            _heartbeatTimer = new Timer(state =>
            {
                lock (_lockObject)
                {
                    try
                    {
                        Logger.InfoFormat("..Heartbeat for resource {0}", _resource);
                        using var session = _storage.Repository.OpenSession();
                        IDocumentSessionExtensions.SetExpiry<string>(session, _distributedLock.Id, _options.DistributedLockLifetime);
                        session.SaveChanges();
                    }
                    catch (Exception ex)
                    {
                        Logger.ErrorFormat("...Unable to update heartbeat on the resource '{0}'. {1}", _resource, ex);
                        Release();
                    }
                }
            }, null, KeepAliveInterval, KeepAliveInterval);
        }
    }
}
