using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

using task.Models;

namespace task.Data;

public class OfficeConfiguration : IEntityTypeConfiguration<Office>
{
    public void Configure(EntityTypeBuilder<Office> builder)
    {
        builder.ToTable("Offices");
        builder.HasKey(o => o.Id);
        builder.Property(o => o.Id).ValueGeneratedOnAdd();

        builder.Property(o => o.Code);
        builder.Property(o => o.CityCode);
        builder.Property(o => o.Uuid);
        builder.Property(o => o.Type).HasConversion<string>();
        builder.Property(o => o.CountryCode);
        
        builder.OwnsOne(o => o.Coordinates, coords =>
        {
            coords.Property(c => c.Latitude).HasColumnName("Latitude");
            coords.Property(c => c.Longitude).HasColumnName("Longitude");
        });

        builder.Property(o => o.AddressRegion);
        builder.Property(o => o.AddressCity);
        builder.Property(o => o.AddressStreet);
        builder.Property(o => o.AddressHouseNumber);
        builder.Property(o => o.AddressApartment);
        builder.Property(o => o.WorkTime);

        builder.OwnsOne(o => o.Phones, phone =>
        {
            phone.ToJson();
            phone.Property(p => p.PhoneNumber);
            phone.Property(p => p.Additional);
        });

        builder.HasIndex(o => o.Code);
        builder.HasIndex(o => o.CityCode);
        builder.HasIndex(o => o.Uuid);
    }
}
