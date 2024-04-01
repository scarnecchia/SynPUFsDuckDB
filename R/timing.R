# This requiers a synpuf dataset to be downloaded and converted to CSV. This was before I converted the whole of the data to parquet, but is useful for bench marking

# Load data for later
data <- data.table::fread("~/dev/SynPUFsDuckDB/data/procedure_1.csv")

# Create a list for holding values
read_time <- list()

# Read in the data using various methods, starting with readr's read_csv
read_time[["read_csv"]] <- system.time({
  readr::read_csv("~/dev/SynPUFsDuckDB/data/procedure_1.csv")
})

# Use haven to read the original sas file in
read_time[["read_sas"]] <- system.time({
  haven::read_sas("~/dev/SynPUFsDuckDB/data/procedure_1.sas7bdat")
})

# Use data.table's fread
read_time[["datatable_fread"]] <- system.time({
data.table::fread("~/dev/SynPUFsDuckDB/data/procedure_1.csv")
})

# Create a database connection and use DuckDB's read_csv_auto function
con <- DBI::dbConnect(duckdb::duckdb())
DBI::dbExecute(con, "drop table if exists procedure_1")

read_time[["DuckDB"]] <- system.time({
  DBI::dbExecute(con, 'create table procedure_1 as select * from read_csv_auto("~/dev/SynPUFsDuckDB/data/procedure_1.csv")')
})

DBI::dbExecute(con, "drop table if exists procedure_1")
  DBI::dbDisconnect(con, shutdown=TRUE)

gc()
print(read_time)

# Loading the data in memory, perform a simple operation (group by and count), and time
summarize <- list()

# Use data.table
summarize[["datatable"]]  <- system.time({
data.table::setorder(data[, .(Year = lubridate::year(ADate), PX, PX_codetype)][, .(n=.N), by = .(Year, PX, PX_codetype)], Year)
})

# Use dplyr
summarize[["dplyr"]]  <- system.time({
  data |>
    dplyr::mutate(Year = lubridate::year(ADate)) |>
    dplyr::group_by(Year, PX, PX_codetype) |>
    dplyr::summarise(n = dplyr::n()) |>
    dplyr::arrange(Year)
})

# Use dplyr and DuckDB
con <- DBI::dbConnect(duckdb::duckdb())

summarize[["duckDB_dplyr"]] <- system.time({
  dplyr::tbl(con, "procedure_1") |>
    dplyr::mutate(Year = lubridate::year(ADate)) |>
    dplyr::group_by(Year, PX, PX_codetype) |>
    dplyr::summarise(n = dplyr::n()) |>
    dplyr::arrange(Year)
})

summarize[["duckDB"]] <- system.time({
  DBI::dbSendQuery(con, "SELECT PX, PX_codetype, year(ADate) as year, count (*) as count from read_csv_auto('~/dev/SynPUFsDuckDB/data/procedure_1.csv') GROUP BY PX, PX_codetype, year(ADate);") |> DBI::dbFetch()
})

print(summarize)

DBI::dbExecute(con, 'copy procedure_1 to \'~/dev/SynPUFsDuckDB/data/procedure_1-snappy.parquet\' (format \'parquet\');')

