# Create a connection to DuckDB
con <- DBI::dbConnect(duckdb::duckdb(dbdir = "~/dev/SynPUFsDuckDB/data/duckdb.duckdb"))

# Describe the varibles in the diagnosis table
DBI::dbSendQuery(con, "describe select * from '~/dev/SynPUFsDuckDB/data/diagnosis-snappy.parquet';") |> DBI::dbFetch()

# Note that PatID, EncounterID, and ProviderID should be integers and not doubles. Handling these properly (BIGINT). I assume coercing to the proper type should be straight forward but haven't had time to explore yet.

# Count the rows in the diagnosis table
DBI::dbSendQuery(con, "select count (*) from '~/dev/SynPUFsDuckDB/data/diagnosis-snappy.parquet';") |> DBI::dbFetch() |> format(big.mark = ",") |> print()

# Summarize the diagnosis table by Dx Code, CodeType, and derived year
DBI::dbSendQuery(con, "SELECT DX, DX_codetype, year(ADate) as year, count (*) as count from '~/dev/SynPUFsDuckDB/data/diagnosis-snappy.parquet' GROUP BY DX, DX_codetype, year(ADate);") |> DBI::dbFetch()


# Describe the varibles in the demographic table
DBI::dbSendQuery(con, "describe select * from '~/dev/SynPUFsDuckDB/data/demographic-snappy.parquet';") |> DBI::dbFetch()

# Count the rows in the demographic table
DBI::dbSendQuery(con, "describe select * from '~/dev/SynPUFsDuckDB/data/enrollment-snappy.parquet';") |> DBI::dbFetch()


system.time({
  # Count the number of patients with medical and drug coverage and join with Sex, Race, and     Ethnicity variables from the demographic table
DBI::dbSendQuery(con, "SELECT a.Sex, a.hispanic, a.race, count(*) as count FROM '~/dev/SynPUFsDuckDB/data/demographic-snappy.parquet' as a JOIN '~/dev/SynPUFsDuckDB/data/enrollment-snappy.parquet' as b ON (a.patid = b.patid) WHERE b.MedCov = 'Y' and b.DrugCov = 'Y' GROUP BY a.Sex, a.hispanic, a.race;") |> DBI::dbFetch()
})
