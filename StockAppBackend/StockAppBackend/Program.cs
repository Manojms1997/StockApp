using Microsoft.EntityFrameworkCore;
using StockAppBackend.Data;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddCors(options =>
{
	options.AddPolicy(name: "_myAllowSpecificOrigins",
					  policy =>
					  {
						  policy.AllowAnyOrigin()
							  .AllowAnyHeader()
							  .AllowAnyMethod();
					  });
});
builder.Services.AddControllers();
builder.Services.AddDbContext<DataContext>(options =>
{
	options.UseSqlServer(builder.Configuration.GetConnectionString("DefaultConnection"));
});
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
	app.UseSwagger();
	app.UseSwaggerUI();
}

//app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
