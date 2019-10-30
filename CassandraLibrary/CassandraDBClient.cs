using Cassandra;
using System;
using System.Collections.Generic;
using System.Text;

namespace CassandraLibrary
{
   public class CassandraDBClient
    {
        public Cluster Cluster { get; private set; }
        public ISession Session { get; private set; }

        public ISession Connect()
        {
            Cluster = Cluster.Builder().AddContactPoint("127.0.0.1").Build();
            // Connect to the nodes using a keyspace
            Session = Cluster.Connect();
            return Session;
        }
        
        public void DisConnect()
        {
            Console.WriteLine("Disposing Connection!!!");
            Cluster.Dispose();
        }
    }
}
