﻿using Microsoft.EntityFrameworkCore;

namespace StockAppBackend.Data
{
	public class DataContext : DbContext
	{
		public DataContext(DbContextOptions options) : base(options) { }

	}
}
