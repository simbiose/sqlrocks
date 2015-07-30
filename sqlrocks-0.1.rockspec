package = "sqlrocks"
version = "0.1"

description = {
  summary  = "Sqlrocks, transparent, schemaless library for building and composing SQL statements.",
  homepage = "http://rocks.simbio.se/sqlrocks",
  license  = "MIT"
}

source = {
  url    = "git://github.com/simbiose/sqlrocks.git",
  branch = "v0.1"
}

dependencies = {
  "lua >= 5.1, < 5.3"
}

build = {
  type    = "builtin",
  modules = {
    sqlrocks = 'sqlrocks.lua'
  }
}