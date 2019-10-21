using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Newtonsoft.Json;
using Respawn;

namespace NETCoreOptimistic
{
    class Program
    {
        static void Main(string[] args)
        {
            var existingReservations = Enumerable.Range(0, 25).Select(x => (Guid?) Guid.NewGuid()).ToList();
            var reservationsToCreate = Enumerable.Range(0, 25).Select(x => Guid.NewGuid()).ToList();

            var optionsBuilder = new DbContextOptionsBuilder<StockLinesDbContext>();

            optionsBuilder.UseSqlServer("Data Source=localhost;Initial Catalog=DBName;Integrated Security=True;");

            using (var context = new StockLinesDbContext(optionsBuilder.Options))
            {
                var temp = context.StockLines.FirstOrDefault(p => p.Product == "coke");

                new Checkpoint { TablesToInclude = new List<string> { "StockLines", "Reservations" }.ToArray() }.Reset(context.Database.GetDbConnection().ConnectionString).Wait();

                var stockLine = new StockLine
                {
                    Id = Guid.NewGuid(),
                    Product = "coke",
                    Quantity = 300,
                    IsReserved = false,
                    ReservationId = null
                };

                var existingPerson = context.StockLines.SingleOrDefault(p => p.Product == "coke");
                if (existingPerson == null)
                {
                    context.StockLines.Add(stockLine);
                    context.SaveChanges();
                }

                var reservs = existingReservations.Select(x => new StockLine
                {
                    Id = x.Value,
                    Product = "coke",
                    Quantity = 1,
                    IsReserved = true,
                    ReservationId = x
                });
                context.StockLines.AddRange(reservs);
                context.SaveChanges();
            }

            var tasks = GenerateTasks(reservationsToCreate, existingReservations);

            Task.WhenAll(tasks).Wait();

            using (var context = new StockLinesDbContext(optionsBuilder.Options))
            {
                var stockLines = context.StockLines.Where(x => existingReservations.Contains(x.ReservationId)).AsNoTracking().ToList();
                var availableStockLine = context.StockLines.SingleOrDefault(x => x.IsReserved == false && x.ReservationId == null);
                var createdReservations = context.StockLines.Where(x => reservationsToCreate.Contains(x.ReservationId.Value)).AsNoTracking().ToList();

                if (stockLines.All(x => x.Quantity == 4))
                {
                    Console.WriteLine("ALL existing Reservations updated correctly");
                }

                if (createdReservations.Count == 25 && createdReservations.All(x => x.Quantity == 1))
                {
                    Console.WriteLine("ALL reservations created correctly");
                }

                if (availableStockLine != null && availableStockLine.Quantity == 200)
                {
                    Console.WriteLine("Available stockline quantity is consistent. ");
                }

                Console.ReadKey();
            }
           
        }

        private static IEnumerable<Task> GenerateTasks(List<Guid> reservationsToCreate, List<Guid?> existingReservations)
        {
            var result = new List<Task>();
            for (int i = 0; i < 25; ++i)
            {
                var i2 = i;
                var modifyCreatedReservationsTask1 = ExecuteHandler(existingReservations[i2]);
                var modifyCreatedReservationsTask2 = ExecuteHandler(existingReservations[i2]);
                var modifyCreatedReservationsTask3 = ExecuteHandler(existingReservations[i2]);
                var createNewReservationsTask = ExecuteHandler(reservationsToCreate[i2]);

                result.Add(modifyCreatedReservationsTask1);
               
                result.Add(createNewReservationsTask);
                result.Add(modifyCreatedReservationsTask2);
                result.Add(modifyCreatedReservationsTask3);
            }

            return result;
        }

        private static Task ExecuteHandler(Guid? existingReservation)
        {
            var handler = new Handler();
            return handler.Handle(new StockReserved
            {
                Amount = 1,
                ReservationId = existingReservation.Value
            });
        }
    }

    public class StockLinesDbContext : DbContext
    {
        public DbSet<StockLine> StockLines { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlServer("Data Source=localhost;Initial Catalog=DBName;Integrated Security=True;");
        }

        public StockLinesDbContext(DbContextOptions<StockLinesDbContext> options) : base(options)
        {

        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            modelBuilder.Entity<StockLine>()
                .Property(p => p.RowVersion).IsConcurrencyToken();
        }

        public override int SaveChanges(bool acceptAllChangesOnSuccess)
        {
            ChangeTracker.DetectChanges();

            var addedOrModified = ChangeTracker.Entries<ITrackable>().Where(x => x.State == EntityState.Added || x.State == EntityState.Modified);

            foreach (var item in addedOrModified)
            {
                item.Entity.LastUpdatedUtc = DateTime.UtcNow;
            }

            return base.SaveChanges(acceptAllChangesOnSuccess);
        }
    }

    public interface ITrackable
    {
        DateTime LastUpdatedUtc { get; set; }
    }

    public class InventoryContextFactory : IDesignTimeDbContextFactory<StockLinesDbContext>
    {
        public StockLinesDbContext CreateDbContext(string[] args)
        {
            var optionsBuilder = new DbContextOptionsBuilder<StockLinesDbContext>();

            optionsBuilder.UseSqlServer("Data Source=localhost;Initial Catalog=DBName;Integrated Security=True;");

            return new StockLinesDbContext(optionsBuilder.Options);
        }
    }

    public class StockLine : ITrackable
    {
        public Guid Id { get; set; }
        public string Product { get; set; }
        public int Quantity { get; set; }
        public Guid? ReservationId { get; set; }
        public bool IsReserved { get; set; }

        [Timestamp] public byte[] RowVersion { get; set; }
        public DateTime LastUpdatedUtc { get; set; }
    }

    public class StockReserved
    {
        public Guid ReservationId { get; set; }
        public int Amount { get; set; }
    }

    public class Handler
    {
        private object _entity;

        public async Task Handle(StockReserved @event)
        {
            var optionsBuilder = new DbContextOptionsBuilder<StockLinesDbContext>();

            optionsBuilder.UseSqlServer("Data Source=localhost;Initial Catalog=DBName;Integrated Security=True;");

            using (var context = new StockLinesDbContext(optionsBuilder.Options))
            {
                var available = context.StockLines.Single(p => !p.IsReserved);
                var existingReservation = context.StockLines.SingleOrDefault(p => p.Id == @event.ReservationId);

                if (existingReservation != null)
                {
                    existingReservation.Quantity += @event.Amount;
                }
                else
                {
                    var res = new StockLine
                    {
                        Id = Guid.NewGuid(),
                        Quantity = @event.Amount,
                        IsReserved = true,
                        ReservationId = @event.ReservationId,
                        Product = "coke"
                    };
                    context.StockLines.Add(res);
                }

                available.Quantity -= @event.Amount;

                var saved = false;
                while (!saved)
                {
                    try
                    {
                        await context.SaveChangesAsync();
                        Console.WriteLine("--------------------------------------------------------------------------------------------");
                        Console.WriteLine(JsonConvert.SerializeObject(available));
                        Console.WriteLine(JsonConvert.SerializeObject(existingReservation));
                        Console.WriteLine("--                                                                                        --");
                        Console.WriteLine(JsonConvert.SerializeObject(_entity));
                        Console.WriteLine("--------------------------------------------------------------------------------------------");
                        saved = true;
                    }
                    catch (DbUpdateConcurrencyException ex)
                    {
                        foreach (var entry in ex.Entries)
                        {
                           
                            if (entry.Entity is StockLine && !((StockLine)entry.Entity).IsReserved)
                            {
                                var proposedValues = entry.CurrentValues;
                                var databaseValues = (StockLine)entry.GetDatabaseValues().ToObject();

                                ((StockLine)entry.Entity).Quantity = databaseValues.Quantity - @event.Amount;

                                // Refresh original values to bypass next concurrency check
                                entry.OriginalValues.SetValues(databaseValues);
                            }
                            if (entry.Entity is StockLine && ((StockLine)entry.Entity).IsReserved)
                            {
                                var proposedValues = entry.CurrentValues;
                                var databaseValues = (StockLine)entry.GetDatabaseValues().ToObject();


                                ((StockLine)entry.Entity).Quantity = databaseValues.Quantity + @event.Amount;

                                // Refresh original values to bypass next concurrency check
                                entry.OriginalValues.SetValues(databaseValues);
                                _entity = entry.Entity;
                            }
                        }
                    }
                }
            }
        }
    }
}
