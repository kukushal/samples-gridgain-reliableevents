using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Cache.Event;
using Apache.Ignite.Core.Cache.Query;
using Apache.Ignite.Core.Cache.Query.Continuous;
using Apache.Ignite.Core.Log;
using Apache.Ignite.Core.Resource;
using Apache.Ignite.Core.Services;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Samples.GridGain.ReliableEvents
{
    [Serializable]
    class EventHandlerService : IService
    {
        public static string InCache = "IN_CACHE";
        public static string OutCache = "OUT_CACHE";
        public static int IngressRate = 1_000;

        [InstanceResource]
        private readonly IIgnite ignite;

        private ICache<string, int> stateCache;
        private ICache<int, string> outCache;
        private IContinuousQueryHandle<ICacheEntry<int, string>> qryHandle;

        public static CacheConfiguration CacheConfiguration(string cacheName)
        {
            return new CacheConfiguration(cacheName, new QueryEntity(typeof(int), typeof(string)))
            {
                CacheMode = CacheMode.Replicated
            };
        }

        public void Cancel(IServiceContext context)
        {
            if (qryHandle != null)
                qryHandle.Dispose();

            ignite.Logger.Log(LogLevel.Info, $">>> {nameof(EventHandlerService)} cancelled");
        }

        public void Execute(IServiceContext context)
        {
            ignite.Logger.Log(LogLevel.Info, $">>> {nameof(EventHandlerService)} simulated initialization delay...");
            Thread.Sleep(IngressRate * 2);

            var inCache = WaitForInputCache();
            outCache = ignite.GetOrCreateCache<int, string>(CacheConfiguration(OutCache));
            stateCache = ignite.GetOrCreateCache<string, int>("STATE_CACHE");

            var qry = new ContinuousQuery<int, string>(new LocalListener(outCache, stateCache));

            const string OffsetField = "_KEY";

            var offset = stateCache.ContainsKey(LocalListener.OffsetKey)
                ? stateCache.Get(LocalListener.OffsetKey)
                : int.MaxValue;

            var initQry = new SqlQuery(typeof(string), $"FROM {InCache}.STRING WHERE {OffsetField} > ?")
            {
                Arguments = new[] { (object)offset }
            };

            qryHandle = inCache.QueryContinuous(qry, initQry);

            foreach (var entry in qryHandle.GetInitialQueryCursor())
                LocalListener.Action(entry, outCache, stateCache);

            ignite.Logger.Log(LogLevel.Info, $">>> {nameof(EventHandlerService)} is executing");
        }

        public void Init(IServiceContext context)
        {
            ignite.Logger.Log(LogLevel.Info, $">>> {nameof(EventHandlerService)} initialized");
        }

        private ICache<int, string> WaitForInputCache()
        {
            while (!ignite.GetCacheNames().Contains(InCache))
            {
                ignite.Logger.Log(LogLevel.Info, $">>> Waiting for {InCache}...");
                Thread.Sleep(1_000);
            }

            return ignite.GetCache<int, string>(InCache);
        }

        private class LocalListener : ICacheEntryEventListener<int, string>
        {
            public static string OffsetKey = "Offset";

            private readonly ICache<int, string> outCache;
            private readonly ICache<string, int> stateCache;

            public LocalListener(ICache<int, string> outCache, ICache<string, int> stateCache)
            {
                this.outCache = outCache;
                this.stateCache = stateCache;
            }

            public void OnEvent(IEnumerable<ICacheEntryEvent<int, string>> evts)
            {
                foreach (var evt in evts)
                {
                    if (evt.EventType != CacheEntryEventType.Removed)
                        Action(evt, outCache, stateCache);
                }
            }

            public static void Action(
                ICacheEntry<int, string> entry,
                ICache<int, string> outCache,
                ICache<string, int> stateCache)
            {
                outCache.Put(entry.Key, entry.Value);

                stateCache.Put(OffsetKey, entry.Key);

                outCache.Ignite.Logger.Log(LogLevel.Info, $">>> Handled key {entry.Key}, value {entry.Value}");
            }
        }
    }
}
