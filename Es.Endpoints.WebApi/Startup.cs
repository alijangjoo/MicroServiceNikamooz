using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Es.EventStore.Es;
using Es.EventStore.MartenDb;
using Es.EventStore.SqlServer;
using Es.Framework;
using EventStore.ClientAPI;
//using Marten;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Session07.Es.CustomerManagement.ApplicationServices.Customers;
using Session07.Es.CustomerManagement.Domain.Customers.Repositories;
using Session07.Es.CustomerManagement.Infrastructure.Data.Commands.Customers;

namespace Es.Endpoints.WebApi
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {

            //var esConnection = EventStoreConnection.Create( Configuration["eventStore:connectionString"], ConnectionSettings.Create().KeepReconnecting(),"MyApplication");
            //services.AddSingleton(esConnection);

            services.AddControllers();
            services.AddMarten(Configuration.GetConnectionString("PostgreDb"));
            services.AddScoped<CustomerService>();
            services.AddScoped<ICustomerRepository, CustomerRepository>();
            //services.AddScoped<IEventStore, SqlEventStore>();
            //services.AddScoped<IEventStore, EsEventStore>();
            services.AddScoped<IEventStore, MartenEventStore>();
            services.AddSwaggerGen(c =>
            {
                c.SwaggerDoc("v1", new OpenApiInfo { Title = "My API", Version = "v1" });
            });
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseHttpsRedirection();

            app.UseRouting();

            app.UseAuthorization();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });
            app.UseSwagger();

            app.UseSwaggerUI(c =>
            {
                c.SwaggerEndpoint("/swagger/v1/swagger.json", "My API V1");
            });
        }
    }
}
