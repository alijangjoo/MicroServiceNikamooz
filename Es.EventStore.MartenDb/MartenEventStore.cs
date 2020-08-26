using Es.Framework;
using Marten;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Es.EventStore.MartenDb
{
    public class MartenEventStore : IEventStore,IDisposable
    {
        public MartenEventStore(IDocumentStore documentStore)
        {
            
            documentSession = documentStore.DirtyTrackedSession();

        }

        private readonly IDocumentSession documentSession;
        private readonly JsonSerializerSettings _jsonSerializerSettings = new JsonSerializerSettings()
        {
            TypeNameHandling = TypeNameHandling.All,
            NullValueHandling = NullValueHandling.Ignore
        };
        public async Task<IReadOnlyCollection<EventStoreItem>> GetAll(DateTime? afterDateTime)
        {
            var events = await documentSession.Query<EventStoreItem>()
                 //.Where(item => item.CreatedAt > afterDateTime)
                 .ToListAsync()
                 .ConfigureAwait(false);            
            return events;
        }

        public async Task<IReadOnlyCollection<IDomainEvent>> LoadAsync(Guid aggregateRootId, string aggregateName)
        {
           var events = await documentSession.Query<EventStoreItem>()
                .Where(item => item.Aggregate == aggregateName && Guid.Parse(item.AggregateId) == aggregateRootId)
                .ToListAsync()
                .ConfigureAwait(false);

            var domainEvents = events.Select(TransformEvent).Where(x => x != null).ToList().AsReadOnly();
            return domainEvents;

        }
        private IDomainEvent TransformEvent(EventStoreItem eventSelected)
        {
            var o = JsonConvert.DeserializeObject(eventSelected.Data, _jsonSerializerSettings);
            var evt = o as IDomainEvent;

            return evt;
        }
        public async Task SaveAsync(Guid aggregateId, string aggregateName, int originatingVersion, IReadOnlyCollection<IDomainEvent> events)
        {
            if (events.Count < 1)
            {
                return;
            }

            var createdAt = DateTime.Now;

            var listOfEvents = events.Select(ev => new
            {
                Id = Guid.NewGuid(),
                Version = ++originatingVersion,
                Name = ev.GetType().Name,
                AggregateId = aggregateId.ToString(),
                Data = JsonConvert.SerializeObject(ev, Formatting.Indented, _jsonSerializerSettings),
                Aggregate = aggregateName,
                CreatedAt = createdAt,

            });
            foreach (var item in listOfEvents)
            {
                
                documentSession.Insert(item);
                await documentSession.SaveChangesAsync();
            }



        }
        
        #region Dispose
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                documentSession.Dispose();
            }

        }
        #endregion
    }
}
