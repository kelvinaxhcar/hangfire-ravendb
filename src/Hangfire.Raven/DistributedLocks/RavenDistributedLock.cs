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
        private static readonly ThreadLocal<Dictionary<string, int>> AcquiredLocks = new ThreadLocal<Dictionary<string, int>>((Func<Dictionary<string, int>>)(() => new Dictionary<string, int>()));
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
                throw new ArgumentException(string.Format("The timeout specified is too large. Please supply a timeout equal to or less than {0} seconds", (object)int.MaxValue), nameof(timeout));
            options.ThrowIfNull(nameof(options));
            _storage = storage;
            _resource = resource;
            _options = options;
            if (!RavenDistributedLock.AcquiredLocks.Value.ContainsKey(_resource) || RavenDistributedLock.AcquiredLocks.Value[_resource] == 0)
            {
                Acquire(timeout);
                RavenDistributedLock.AcquiredLocks.Value[_resource] = 1;
                StartHeartBeat();
            }
            else
                RavenDistributedLock.AcquiredLocks.Value[_resource]++;
        }

        public void Dispose()
        {
            if (_completed)
                return;
            _completed = true;
            if (!RavenDistributedLock.AcquiredLocks.Value.ContainsKey(_resource))
                return;
            RavenDistributedLock.AcquiredLocks.Value[_resource]--;
            if (RavenDistributedLock.AcquiredLocks.Value[_resource] > 0)
                return;
            lock (_lockObject)
            {
                RavenDistributedLock.AcquiredLocks.Value.Remove(_resource);
                if (_heartbeatTimer != null)
                {
                    _heartbeatTimer.Dispose();
                    _heartbeatTimer = (Timer)null;
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
                        session.SetExpiry<DistributedLock>(_distributedLock, _options.DistributedLockLifetime);
                        try
                        {
                            session.SaveChanges();
                            return;
                        }
                        catch (ConcurrencyException ex1)
                        {
                            _distributedLock = (DistributedLock)null;
                            try
                            {
                                new EventWaitHandle(false, EventResetMode.AutoReset, EventWaitHandleName).WaitOne(millisecondsTimeout);
                            }
                            catch (PlatformNotSupportedException ex2)
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
                        _distributedLock = (DistributedLock)null;
                    }
                }
                EventWaitHandle result;
                if (!EventWaitHandle.TryOpenExisting(EventWaitHandleName, out result))
                    return;
                result.Set();
            }
            catch (PlatformNotSupportedException ex)
            {
            }
            catch (Exception ex)
            {
                _distributedLock = (DistributedLock)null;
                throw new RavenDistributedLockException("Could not release a lock on the resource '" + _resource + "': Check inner exception for details.", ex);
            }
        }

        private void StartHeartBeat()
        {
            RavenDistributedLock.Logger.InfoFormat(".Starting heartbeat for resource: {0}", (object)_resource);
            _heartbeatTimer = new Timer((TimerCallback)(state =>
            {
                lock (_lockObject)
                {
                    try
                    {
                        RavenDistributedLock.Logger.InfoFormat("..Heartbeat for resource {0}", (object)_resource);
                        using (IDocumentSession session = _storage.Repository.OpenSession())
                        {
                            IDocumentSessionExtensions.SetExpiry<string>(session, _distributedLock.Id, _options.DistributedLockLifetime);
                            session.SaveChanges();
                        }
                    }
                    catch (Exception ex)
                    {
                        RavenDistributedLock.Logger.ErrorFormat("...Unable to update heartbeat on the resource '{0}'. {1}", (object)_resource, (object)ex);
                        Release();
                    }
                }
            }), (object)null, RavenDistributedLock.KeepAliveInterval, RavenDistributedLock.KeepAliveInterval);
        }
    }
}
