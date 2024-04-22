# Script for converting SAS datasets to Parquet format with Snappy compression using DuckDB

ptm <- proc.time()

con <- DBI::dbConnect(duckdb::duckdb(dbdir = "duckdb.duckdb"))

# Loop through each table and convert them to Parquet format with Snappy compression
purrr::walk(c("death",
              "demographic",
              "diagnosis",
              "dispensing",
              "encounter",
              "enrollment",
              "procedure",
              "facility",
              "provider"), function(t) {

  purrr::walk(1:20, function(i) {
    ptm <- proc.time()
    print(paste("Processing", t, "table, part", i))

    data <- haven::read_sas(paste0("data/", t, "_", i, ".sas7bdat")) |>
      dplyr::mutate(dplyr::across(dplyr::ends_with("id"), ~ bit64::as.integer64(.x)))

    DBI::dbExecute(con, paste0("drop table if exists data"))
    duckdb::dbWriteTable(con, "data", data)

    DBI::dbExecute(con,
                   paste0("copy data to 'parquet/" ,t , "-", i, "-snappy.parquet' (format 'parquet');"))
    print(proc.time() - ptm)
})
})

# The ID variables need to be recreated to ensure they're unique. This is a bit tricky because we need to purrr over each dataset to interatively assign a new ID variable. We'll start with the canonical source for each ID var, and then join the tables by original variable.

construct_query <- function(len, table, id_var, part) {
  ptm <- proc.time()

  duckdb::dbSendQuery(con, paste0("CREATE OR REPLACE TABLE tmp AS SELECT * FROM 'parquet/", table, "-", part, "-snappy.parquet';"))
  duckdb::dbSendQuery(con, paste0("ALTER TABLE tmp RENAME ", id_var, " TO Orig", id_var, ";"))
  duckdb::dbSendQuery(con, paste0("CREATE OR REPLACE SEQUENCE id_sequence START ", len, ";"))
  duckdb::dbSendQuery(con, paste0("ALTER TABLE tmp ADD COLUMN ", id_var, " UBIGINT DEFAULT nextval('id_sequence');"))
  duckdb::dbSendQuery(con, paste0("COPY tmp TO 'parquet_tmp/", table, "-", part, "-snappy.parquet' (format 'parquet');"))
  duckdb::dbSendQuery(con, paste0("SUMMARIZE SELECT ", id_var, " FROM 'parquet_tmp/", table, "-", part, "-snappy.parquet';")) |>
    duckdb::dbFetch() |> print()
  duckdb::dbSendQuery(con, paste0("DROP TABLE IF EXISTS tmp;"))

  print(proc.time() - ptm)
}

purrr::walk(c("death", "diagnosis", "dispensing", "enrollment", "procedure"), function(t) {
  purrr::walk(1:20, function(i) {
    fs::file_copy(paste0("parquet/", t, "-", i, "-snappy.parquet"), paste0("parquet_tmp/", t, "-", i, "-snappy.parquet"), overwrite = TRUE)
  })
})

# SetID variables in their source tables
purrr::walk2(c("demographic", "facility", "provider", "encounter"), c("PatID", "FacilityID", "ProviderID", "EncounterID"), function(t, v) {
  ptm <- proc.time()
  prev_tablen <- 0

  purrr::walk(1:20, function(i) {
    cat(paste0("Table: ", t, ", Variable: ", v, ", Part: ", i))

    if (i == 1) {
      .tablen = prev_tablen + 1
    } else {
      .partition <- i - 1
      .tablen <- DBI::dbSendQuery(con, paste0("SELECT count(*) FROM 'parquet/", t, "-", .partition, "-snappy.parquet';")) |>
        DBI::dbFetch() |>
        dplyr::pull()
    .tablen = prev_tablen + .tablen
    }

    prev_tablen <<- .tablen
    print(.tablen)
    construct_query(.tablen, t, v, i)
  })
  print(proc.time() - ptm)
})

replace_orig <- function(table, id_var, part, id_source) {
  ptm <- proc.time()

  print(paste0("Table: ", table, ", ", "Part: ", part))

  duckdb::dbSendQuery(con, paste0("CREATE OR REPLACE TABLE ", table, " AS SELECT * FROM 'parquet_tmp/", table, "-", part, "-snappy.parquet';"))

  print(paste0("Rename variable ", id_var, " to Orig", id_var))
  duckdb::dbSendQuery(con, paste0("ALTER TABLE ", table, " RENAME ", id_var, " TO Orig", id_var, ";"))

  print("Create new table")
  duckdb::dbSendQuery(
    con,
    paste0(
      "CREATE OR REPLACE TABLE tmp AS SELECT a.",
      id_var,
      ", b.* FROM 'parquet_tmp/",
      id_source,
      "-",
      part,
      "-snappy.parquet' AS a JOIN ",
      table,
      " AS b ON a.Orig",
      id_var,
      " = b.Orig",
      id_var,
      ";"
    )
  )

  print(paste0("Drop Orig", id_var, " from table tmp"))
  duckdb::dbSendQuery(con, paste0("ALTER TABLE tmp DROP Orig", id_var, ";"))

  print(paste0("Drop table ", table))
  duckdb::dbSendQuery(con, paste0("DROP TABLE IF EXISTS ", table, ";"))

  print(paste0("Copy tmp to 'parquet_tmp/", table, "-", part, "-snappy.parquet'"))
  duckdb::dbSendQuery(con, paste0("COPY tmp TO 'parquet_tmp/", table, "-", part, "-snappy.parquet' (format 'parquet');"))

  print("Drop table tmp")
  duckdb::dbSendQuery(con, paste0("DROP TABLE IF EXISTS tmp;"))
  print(proc.time() - ptm)
}

clean_source <- function(id_source, id_var) {

  purrr::walk(1:20, function(part) {
  print(paste0("Create table ", id_source))
  duckdb::dbSendQuery(con, paste0("CREATE OR REPLACE TABLE ", id_source, " AS SELECT * FROM 'parquet_tmp/", id_source, "-", part, "-snappy.parquet';"))

  print(paste0("Drop Orig", id_var, " from table ", id_source))
  duckdb::dbSendQuery(con, paste0("ALTER TABLE ", id_source, " DROP Orig", id_var, ";"))

  print(paste0("Copy ", id_source, " to 'parquet_tmp/", id_source, "-", part, "-snappy.parquet'"))
  duckdb::dbSendQuery(con, paste0("COPY ", id_source, " TO 'parquet_tmp/", id_source, "-", part, "-snappy.parquet' (format 'parquet');"))

  print(paste0("Drop table ", id_source))
  duckdb::dbSendQuery(con, paste0("DROP TABLE IF EXISTS ", id_source, ";"))
})
}

# PatID
purrr::walk(c(
  "death",
  "diagnosis",
  "dispensing",
  "encounter",
  "enrollment",
  "procedure"
), function(t) {
  purrr::walk(1:20, function(i) {
    replace_orig(t, "PatID", i, "demographic")
  })
})

clean_source("demographic", "PatID")

# ProviderID
purrr::walk(c("diagnosis", "dispensing", "procedure"), function(t) {
  purrr::walk(1:20, function(i) {
    replace_orig(t, "ProviderID", i, "provider")
  })
})

clean_source("provider", "ProviderID")

# FacilityID
purrr::walk(c("Encounter"), function(t) {
  purrr::walk(1:20, function(i) {
    replace_orig(t, "FacilityID", i, "facility")
  })
})

clean_source("facility", "FacilityID")

# EncounterID

purrr::walk(c("Diagnosis", "Procedure"), function(t) {
  purrr::walk(1:20, function(i) {
    replace_orig(t, "EncounterID", i, "Encounter")

  })
})

clean_source("encounter", "EncounterID")

# Bind the tables

purrr::walk(c("death",
              "demographic",
              "diagnosis",
              "dispensing",
              "encounter",
              "enrollment",
              "procedure",
              "facility",
              "provider"), function(t) {

   ptm <- proc.time()

   duckdb::dbSendQuery(con, paste0("CREATE OR REPLACE TABLE tmp AS SELECT * FROM 'parquet_tmp/", t, "-1-snappy.parquet';"))
   duckdb::dbSendQuery(con, paste0("COPY tmp TO 'output/", t, "-snappy.parquet' (format 'parquet');"))
   duckdb::dbSendQuery(con, paste0("DROP TABLE IF EXISTS tmp;"))


   purrr::walk(2:20, function(i) {

     duckdb::dbSendQuery(con, paste0("CREATE OR REPLACE TABLE tmp AS SELECT * FROM 'output/", t, "-snappy.parquet' UNION ALL SELECT * FROM 'parquet_tmp/", t, "-", i, "-snappy.parquet';"))
     duckdb::dbSendQuery(con, paste0("COPY tmp TO 'output/", t, "-snappy.parquet' (format 'parquet');"))
     duckdb::dbSendQuery(con, paste0("DROP TABLE IF EXISTS tmp;"))

   })

   print(paste("Time to assemble", t, "is:"))
   print(proc.time() - ptm)
})

DBI::dbDisconnect(con, shutdown = TRUE)

print(proc.time() - ptm)
