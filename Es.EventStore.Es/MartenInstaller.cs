using Es.EventStore.MartenDb;
using Es.Framework;
using Marten;
using Marten.Services;
using Microsoft.Extensions.DependencyInjection;

namespace Es.EventStore.Es
{
    public static class MartenInstaller
    {
        public static void AddMarten(this IServiceCollection services, string cnnString)
        {
            services.AddSingleton(CreateDocumentStore(cnnString));

            services.AddScoped<IEventStore, MartenEventStore>();
        }

        private static IDocumentStore CreateDocumentStore(string cn)
        {
            return DocumentStore.For(_ =>
            {
                _.Connection(cn);
                _.DatabaseSchemaName = "MartenEventStore";
                _.Serializer(CustomizeJsonSerializer());
                _.NameDataLength = 120;
                _.Schema.For<EventStoreItem>().Duplicate(t => t.Id, pgType: "uuid", configure: idx => idx.IsUnique = true);
            });
        }

        private static JsonNetSerializer CustomizeJsonSerializer()
        {
            var serializer = new JsonNetSerializer();

            serializer.Customize(_ =>
            {
                _.ContractResolver = new ProtectedSettersContractResolver();
            });

            return serializer;
        }
    }
}
