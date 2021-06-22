using System;
using System.Diagnostics;
using System.Reflection;
using Microsoft.Extensions.Configuration;
using DbUp;
using DbUp.CassandraCql;

namespace CassandraSampleApplication
{
    class Program
    {
        static int Main()
        {
            var config = GetConfig();
            var connectionString = config.GetConnectionString("SampleCassandra");

            var executingAssembly = Assembly.GetExecutingAssembly();
            var upgrader =
                DeployChanges.To
                    .CassandraDatabase(connectionString)
                    .WithScriptsEmbeddedInAssembly( executingAssembly,
                        s => s.StartsWith($"{executingAssembly.GetName().Name}.{"Scripts"}"))
                    .WithoutTransaction()
                    .LogToConsole()
                    .Build();

            var result = upgrader.PerformUpgrade();

            if (!result.Successful)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(result.Error);
                Console.ResetColor();

                WaitIfDebug();
                return -1;
            }

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Success!");
            Console.ResetColor();
            WaitIfDebug();
            return 0;
        }
       
        private static IConfiguration GetConfig()
        {
            IConfiguration config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", true, true)
                .Build();
            return config;
        }

        [Conditional("DEBUG")]
        public static void WaitIfDebug()
        {
            Console.ReadLine();
        }
    }
}
