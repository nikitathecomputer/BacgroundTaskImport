using Microsoft.EntityFrameworkCore;
using System.Reflection;

using task.Models;

namespace task.Data;

public class DellinDictionaryDbContext : DbContext
{
    public DbSet<Office> Offices { get; set; }

    public DellinDictionaryDbContext(DbContextOptions<DellinDictionaryDbContext> options)
        : base(options) { }

    protected override void OnModelCreating(ModelBuilder builder)
    {
        builder.ApplyConfigurationsFromAssembly(Assembly.GetExecutingAssembly());
    }
}
