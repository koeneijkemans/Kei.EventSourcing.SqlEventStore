using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;

namespace Kei.EventSourcing.SqlEventStore
{
    public class SqlEventStore : EventStore
    {
        private readonly string _connectionString;
        private readonly string _tableName;

        public SqlEventStore(string connectionString, EventPublisher eventPublisher, string tableName = "Events")
            : base(eventPublisher)
        {
            _connectionString = connectionString;
            _tableName = tableName;

            EnsureTableCreated();
        }

        public override List<Event> Get(Guid aggregateId)
        {
            List<Event> events = new List<Event>();

            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                string query = $"SELECT * FROM {_tableName} WHERE AggregateId = @aggregateId ORDER BY [Order] ASC";
                var param = new SqlParameter("aggregateId", SqlDbType.UniqueIdentifier);
                param.Value = aggregateId;

                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    command.Parameters.Add(param);

                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            SqlEvent sqlEvent = new SqlEvent();
                            sqlEvent.Id = new Guid(reader["Id"].ToString());
                            sqlEvent.AggregateId = new Guid(reader["AggregateId"].ToString());
                            sqlEvent.Order = int.Parse(reader["Order"].ToString());
                            sqlEvent.EventType = reader["Type"].ToString();
                            sqlEvent.Data = reader["Data"].ToString();

                            Event @event = ToEvent(sqlEvent);
                            events.Add(@event);
                        }
                    }
                }
            }

            return events;
        }

        protected override void SaveInStore(Event @event)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                string data = JsonConvert.SerializeObject(@event);
                string query = $"INSERT INTO events ([Id], [AggregateId], [Order], [Type], [Data]) VALUES (NEWID(), '{@event.AggregateRootId}', '{@event.Order}', '{@event.GetType().Name}', '{data}');";

                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }

        private void EnsureTableCreated()
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                string query = $"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='events' AND xtype='U') CREATE TABLE {_tableName} ([Id] UNIQUEIDENTIFIER PRIMARY KEY, [AggregateId] UNIQUEIDENTIFIER, [Order] INT, [Type] VARCHAR(100), [Data] VARCHAR(max))";

                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
