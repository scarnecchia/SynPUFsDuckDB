# Script for converting SAS datasets to Parquet format with Snappy compression using DuckDB
# Use with caution, this took about an hour on my machine (a MBP M3 w/ 16 GB of RAM)

tables <-
  c("death",
    "demographic",
    "diagnosis",
    "dispensing",
    "encounter",
    "enrollment",
    "procedure")

# Create the DuckDB connection. Make sure to use dbdir to avoid out of memory errors on large databases.

con <- DBI::dbConnect(duckdb::duckdb(dbdir = "~/dev/SynPUFsDuckDB/data/duckdb.duckdb"))

# Loop through each table and convert them to Parquet format with Snappy compression
purrr::walk(tables, function(x) {

  # Because of the size of the diagnosis table, we have to read each file into the database one   by one (note, your mileage may vary)
  data <- haven::read_sas(paste0("data/", x, "_1", ".sas7bdat"))

  DBI::dbExecute(con, "drop table if exists data")
  duckdb::dbWriteTable(con, "data", data)

  # Append the remaining files to the table
  num <- 2:20

  # for (y in num) {
  #   data <- haven::read_sas(paste0("data/", x, "_", y, ".sas7bdat"))
  #   duckdb::dbAppendTable(con, "data", data)
  #
  # }

  walk(num, function(y) {
    data <- haven::read_sas(paste0("data/", x, "_", y, ".sas7bdat"))
    duckdb::dbAppendTable(con, "data", data)
  })


  # Write the table to Parquet format with Snappy compression
  DBI::dbExecute(
    con,
    sprintf(
      'copy data to \'~/dev/SynPUFsDuckDB/data/%s-snappy.parquet\' (format \'parquet\');', x))

})

# Disconnect from the database and shut it down
DBI::dbExecute(con, "drop table if exists data")
DBI::dbDisconnect(con, shutdown = TRUE)
