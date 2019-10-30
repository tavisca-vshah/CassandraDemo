using Cassandra;
using Cassandra.Mapping;
using System;
using System.Collections.Generic;
using System.ComponentModel;

namespace CassandraLibrary
{
    public class CassandraDBDemo 
    {
       
        public  void MigrateUserDB(ISession session)
        {
            CreateKeyspace(session);
            CreateTable(session);
        }

        public void SeedDataIntoTable(ISession session)
        {
            //session = cluster.Connect("uprofile");
            IMapper mapper = new Mapper(session);

            // Inserting Data into user table
            mapper.Insert<User>(new User(1, "LyubovK", "Dubai"));
            mapper.Insert<User>(new User(2, "JiriK", "Toronto"));
            mapper.Insert<User>(new User(3, "IvanH", "Mumbai"));
            mapper.Insert<User>(new User(4, "LiliyaB", "Seattle"));
            mapper.Insert<User>(new User(5, "JindrichH", "Buenos Aires"));
            Console.WriteLine("Inserted data into user table");
           
        }

        public void DeleteUser(ISession session, int userId)
        {
            session.Execute($"DELETE FROM user WHERE user_id = {userId}; ");
            Console.WriteLine("Deleted Successfully");
        }

        public void Update(ISession session,int userId)
        {
            session.Execute($"UPDATE user SET user_name='vshah' WHERE user_id = {userId}; ");
            Console.WriteLine("Updated Successfully");
        }

        public  void CleanUp(ISession session)
        {
            session.Execute("DROP table user");
            session.Execute("DROP KEYSPACE uprofile");
        }

        public  void ShowUserById(ISession session,int userId)
        {
            IMapper mapper = new Mapper(session);
            Console.WriteLine($"Getting by id {userId}");
            Console.WriteLine("-------------------------------");
            User user = mapper.FirstOrDefault<User>("Select * from user where user_id = ?", userId);
            if (user == null)
            {
                Console.WriteLine("No user Available");
            }
            else
            {
                Console.WriteLine(user);
            }
        }

        public  void ShowUsers(ISession session)
        {
            IMapper mapper = new Mapper(session);
            Console.WriteLine("ALL User");
            Console.WriteLine("-------------------------------");
            foreach (User user in mapper.Fetch<User>("Select * from user"))
            {
                Console.WriteLine(user);
            }
        }

        private static void CreateTable(ISession session)
        {
            session.Execute("CREATE TABLE IF NOT EXISTS uprofile.user (user_id int PRIMARY KEY, user_name text, user_bcity text)");
            Console.WriteLine(String.Format("created table user"));
        }

        private static void CreateKeyspace(ISession session)
        {
            session.Execute("DROP KEYSPACE IF EXISTS uprofile");
            session.Execute("CREATE KEYSPACE uprofile WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 };");
            Console.WriteLine(String.Format("created keyspace uprofile"));
        }


        

    }
}
