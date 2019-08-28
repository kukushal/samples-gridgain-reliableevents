using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Common;
using System;
using System.Threading;

namespace Samples.GridGain.ReliableEvents
{
    class Program
    {
        static void Main(string[] args)
        {
            Ignition.ClientMode = true;

            using (var ignite = Ignition.StartFromApplicationConfiguration())
            {
                ignite.GetServices().DeployClusterSingleton(nameof(EventHandlerService), new EventHandlerService());

                var inCache = ignite.GetOrCreateCache<int, string>(
                    EventHandlerService.CacheConfiguration(EventHandlerService.InCache));

                Console.WriteLine(">>> Press CTRL+C to exit...");

                for (var i = 0;; i++)
                {
                    Thread.Sleep(EventHandlerService.IngressRate);
                    while (true)
                    {
                        try
                        {
                            inCache.Put(i, $"s{i}");
                            break;
                        }
                        catch (CacheException cacheEx)
                        {
                            if (cacheEx.InnerException is ClientDisconnectedException disEx)
                                disEx.ClientReconnectTask.Wait();
                            else
                                throw;
                        }
                    }
                }
            }
        }
    }
}
