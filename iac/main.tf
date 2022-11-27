terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "0.52.0"
    }
  }
}

variable "snowflake_account" {
  type = string
}
variable "snowflake_region" {
  type = string
}

variable "snowflake_user" {
  type = string
}
variable "snowflake_pass" {
  type      = string
  sensitive = true
}

variable "wf_user" {
  type = string
}
variable "wf_pass" {
  type      = string
  sensitive = true
}

# Use Admin user to create dbs
provider "snowflake" {
  username = var.snowflake_user
  account  = var.snowflake_account
  region   = var.snowflake_region
  password = var.snowflake_pass
}


# 1 - Create Raws Infra
# 1.1 - dedicated loading warehouse to allow for tuning & aggregate cost
resource "snowflake_warehouse" "loader" {
  name           = "LOADER"
  comment        = "used for RAWS loading"
  warehouse_size = "x-small"
}

# 1.2 - dedicated database to reduce read access down the line
resource "snowflake_database" "raws" {
  name                        = "RAWS"
  comment                     = "database for the raw data ingestion"
  data_retention_time_in_days = 90
}

########################
#The below is a essentially a module we can use for all new loader teams
########################

# 1.3 - dedicated domain resource schema for implicit documentation
resource "snowflake_schema" "raws_westfarmers" {
  database = snowflake_database.raws.name
  name     = "WESTFARMERS"
  comment  = "WestFarmers Raws"
}

# 2 - Permissions model for loader users
# 2.1 - dedicated loader role with write access to raws database
#       Could be extended to allow for schema only access & downstream ro access
resource "snowflake_role" "loader_role" {
  name    = "loader"
  comment = "A role granted to users who may need to load data"
}

resource "snowflake_role" "wf_loader_role" {
  name    = "WF_loader"
  comment = "A role granted to users who may need to load data into the westfarmers schema"

}

# 2.2 - grant loader role usage on loader warehouse
resource "snowflake_warehouse_grant" "loader_wh_use" {
  warehouse_name = snowflake_warehouse.loader.name
  privilege      = "USAGE"
  roles          = [snowflake_role.wf_loader_role.name]
}

# 2.3 - grant loader role access on raws db
resource "snowflake_database_grant" "raws_db_use" {
  database_name = snowflake_database.raws.name
  privilege     = "USAGE"
  roles = [
    snowflake_role.loader_role.name,
    snowflake_role.wf_loader_role.name
  ]
  with_grant_option = false
}

# 2.2 - grant loader role access on only raws westfarmers schema
resource "snowflake_schema_grant" "westfarmers_schema_use" {
  database_name = snowflake_database.raws.name
  schema_name   = snowflake_schema.raws_westfarmers.name
  privilege     = "OWNERSHIP"
  roles = [
    snowflake_role.wf_loader_role.name
  ]
  with_grant_option = false
}

# 3 - User and grants to infra
# 3.2 - dedicated user app user, can hand creds to teams to manage
resource "snowflake_user" "wf" {
  name                 = var.wf_user
  password             = var.wf_pass
  default_warehouse    = snowflake_warehouse.loader.name
  default_role         = snowflake_role.wf_loader_role.name
  must_change_password = false
}

# 3.3 - grant our user the loader role, enabling access to dbs & warehouse
resource "snowflake_role_grants" "loader_role_grants" {
  role_name = snowflake_role.loader_role.name

  roles = [
    "SYSADMIN"
  ]
}

resource "snowflake_role_grants" "wf_loader_role_grants" {
  role_name = snowflake_role.wf_loader_role.name

  users = [
    snowflake_user.wf.name
  ]
  roles = [
    snowflake_role.loader_role.name
  ]
}
