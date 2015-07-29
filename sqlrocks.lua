--
-- Sqlrocks, is a transparent, schemaless library for building and composing SQL statements.
--
-- @author    leite (xico@simbio.se)
-- @license   MIT
-- @copyright Simbiose 2015

local string, math, table = require 'string', require 'math', require 'table'

local find, match, gmatch, gsub, sub, lower, upper, format, max, floor, concat, insert,
  type, pairs, ipairs, tostring, select, next, error, rawset, setmetatable, getmetatable =
  string.find, string.match, string.gmatch, string.gsub, string.sub, string.lower,
  string.upper, string.format, math.max, math.floor, table.concat, table.insert, type,
  pairs, ipairs, tostring, select, next, error, rawset, setmetatable, getmetatable

string, math, table = nil, nil, nil

local reserved = {
  abort=true, action=true, add=true, after=true, all=true, alter=true, analyse=true,
  analyze=true, ['and']=true, any=true, array=true, as=true, asc=true,
  asymmetric=true, attach=true, authorization=true, autoincrement=true, before=true,
  begin=true, between=true, both=true, by=true, cascade=true, case=true, cast=true,
  check=true, collate=true, collation=true, column=true, commit=true, conflict=true,
  constraint=true, create=true, cross=true, current_catalog=true, current_date=true,
  current_role=true, current_time=true, current_timestamp=true, current_user=true,
  database=true, default=true, deferrable=true, deferred=true, delete=true,
  desc=true, detach=true, distinct=true, ['do']=true, drop=true, each=true,
  ['else']=true, ['end']=true, escape=true, except=true, exclusive=true, exists=true,
  explain=true, fail=true, ['false']=true, fetch=true, ['for']=true, foreign=true,
  freeze=true, from=true, full=true, glob=true, grant=true, group=true, having=true,
  ['if']=true, ignore=true, ilike=true, immediate=true, ['in']=true, index=true,
  indexed=true, initially=true, inner=true, insert=true, instead=true,
  intersect=true, into=true, is=true, isnull=true, join=true, key=true, lateral=true,
  leading=true, left=true, like=true, limit=true, localtime=true,
  localtimestamp=true, match=true, natural=true, no=true, ['not']=true, notnull=true,
  null=true, of=true, offset=true, on=true, only=true, ['or']=true, order=true,
  outer=true, over=true, overlaps=true, placing=true, plan=true, pragma=true,
  primary=true, query=true, raise=true, references=true, regexp=true, reindex=true,
  release=true, rename=true, replace=true, restrict=true, returning=true, right=true,
  rollback=true, row=true, savepoint=true, ['select']=true, session_user=true,
  set=true, similar=true, some=true, symmetric=true, table=true, temp=true,
  temporary=true, ['then']=true, trailing=true, transaction=true, trigger=true,
  ['true']=true, union=true, unique=true, update=true, to=true, user=true, using=true,
  vacuum=true, values=true, variadic=true, verbose=true, view=true, virtual=true,
  when=true, where=true, window=true, with=true}

local compounds = {
  union='UNION', unionAll='UNION ALL', intersect='INTERSECT',
  intersectAll='INTERSECT ALL', minus='MINUS', minusAll='MINUS ALL',
  except='EXCEPT', exceptAll='EXCEPT ALL'}

local EMPTY, SPACE, DOT, COMA, COMA_SPC, Q_MARK, STAR, PAR_SPC, LT, LE, NOT_EQ,
  EQ, GT, GE, DOLLAR, QUOTE, QQ, ESP_AND, ESP_AS, _INDEX, _HAVING, I, _RETURNING,
  USING, WHERE, ALLSPACE, AND, ANYSPACE, COLS, CROSS, DELSPACEFROMSPACE, DEL,
  FOR_UPDATE, FOR_UPDATE_TBLS, FULL, FUNCTION, GROUPBY, GROUP_BY, HASH, INDEX,
  INNER, INSERT, IS_NOT_NULL, IS_NULL, LEFT, NO_WAIT, NUMBER, OR_ABORT, OR_FAIL,
  OR_IGNORE, OR_REPLACE, OR_ROLLBACK, OR, ORDERBY, ORDER_BY, RETURNING, RIGHT,
  SELECT, _SELECT, STRING, TABLE, TBLS, TEMP, UPDATE, _UPDATE, VALS_P =
  '', ' ', '.', ',', ', ', '?', '*', ') ', '<', '<=', '<>', '=', '>', '>=', '$',
  "'", "''", ' AND ', ' AS ', '__index', '_having', '_i', '_returning', '_using',
  '_where', 'ALL ', 'AND', 'ANY ', 'cols', 'CROSS', 'DELETE FROM ', 'delete',
  'FOR UPDATE ', 'for_update_tbls', 'FULL', 'function', 'GROUP BY ', 'group_by',
  'Hash', 'index', 'INNER', 'INSERT ', 'IS NOT NULL', 'IS NULL', 'LEFT', 'NO WAIT ',
  'number',  'OR ABORT ', 'OR FAIL ', 'OR IGNORE ', 'OR REPLACE ', 'OR ROLLBACK ',
  'OR', 'ORDER BY ', 'order_by', 'RETURNING ', 'RIGHT', 'SELECT ', 'select', 'string',
  'table', 'tbls', 'TEMP ', 'UPDATE ', 'update', 'VALUES ('

local patterns = {
  DQS = '"%s"', CAP1 = '([^%.]+)', DPS = '(%s)', SSPACE = '%s ', SSPACESSSPACE = '%s %s ',
  SSPACESSSPACESS = '%s %s %s%s', SSPACESSUNDERS = '%s %s_%s', SSPACES = '%s %s',
  SEQS = '%s = %s', SEQSS = '%s = %s%s', S_BTW_S_AND_S = '%s BETWEEN %s AND %s',
  S_IN_S = '%s IN (%s)', S_JOIN_S_ON_S = '%s JOIN %s ON %s', S_LIKE_SS = '%s LIKE %s%s',
  S_SET = '%s SET ', S_S = '%s_%s', CAP2 = '%s*([^%s,$]+)%s*,?$?', SS = '%s%s',
  S7 = '%s%s%s%s%s%s%s', EXISTS_S = 'EXISTS (%s)', FROM_S = 'FROM %s ', HVG_S = 'HAVING %s',
  CAP3 = '^([%w%_%-%d]*)%.?([%w%_%-%d]*)%.?([%w%_%-%d]*)%s?[aA]?[sS]?%s?([[%w%_%-%d]*]?)$',
  EXPECT_VAL_GOT = 'expecting %d values, got %d', INTO_S_P = 'INTO %s (', NOT_S = 'NOT %s',
  INTO_SS = 'INTO %s%s ', LIMIT_S = 'LIMIT %s ', OFFSET_S = 'OFFSET %s ',
  USING_S = 'USING %s ', WHERE_S = 'WHERE %s', ESCAPE_Q_S = " ESCAPE '%s'", Q_S_Q = "'%s'",
  EXPECT_HASH = "expecting hash, got a '%s'", UNSP_N_D =
  'Unsupported number of tables in pseudo-view: %d'
}

--
--
--
--
--

local class

do

  local classes = setmetatable({}, {__mode='k'})

  local function deep_copy(t, dest)
    local t, r = t or {}, dest or {}
    for k,v in pairs(t) do
      if type(v) == TABLE and k ~= _INDEX then
        r[k] = deep_copy(v)
      else
        r[k] = v
      end
    end
    return r
  end

  local function is(self, klass)
    local m = getmetatable(self)
    while m do
      if m == klass then return true end
      m = m.super
    end
    return false
  end

  local function include(self, ...)
    if not ... then return end
    for k,v in pairs(...) do
      if self[k] then self[k] = nil end
      self[k] = v
    end
  end

  local function extends(self, ...)
    local meta = {}
    if ... then deep_copy(..., deep_copy(self, meta)) else deep_copy(self, meta) end
    setmetatable(meta, getmetatable(self))
    return meta
  end

  local function new_index(self, key, value)
    rawset(self, key, value)
    for i=1, #classes[self] do
      classes[self][i][key] = value
    end
  end

  class = function (base)
    local c, mt = {}, {}
    if type(base) == TABLE then
      for i,v in pairs(base) do c[i] = v end
      c.super = base
    end

    c.__index     = c
    c.is          = is
    c.extends     = extends
    c.include     = include
    mt.__newindex = new_index
    classes[c]    = {}

    mt.__call = function(self, ...)
      local obj = {}
      setmetatable(obj, c)
      if c.__init then c.__init(obj, ...) end
      return obj
    end

    if c.super then
      insert(classes[c.super], c)
    end

    setmetatable(c, mt)
    return c
  end
end

--
--
--
--
--

local Hash = class()
Hash.__name = 'Hash'

function Hash:__init(bootstrap)
  rawset(self, I, 0)
  rawset(self, 'index', {})

  if TABLE ~= type(bootstrap) then return end
  for k,v in pairs(bootstrap) do self.index[k] = v end
end

function Hash:__newindex(key, val)
  if not self.index[key] then
    insert(self.index, key)
  end

  if nil == val then
    for i=1, #self.index do
      if self.index[i] == key then
        remove(self.index, i)
        self._i = self._i > 0 and (self._i - 1) or self._i
      end
    end
  end

  self.index[key] = val
end

function Hash:__index(key)
  return rawget(getmetatable(self), key) or self.index[key]
end

function Hash:__pairs()
  self._i = 0
  local function iter(self)
    self._i = self._i + 1
    local k = self.index[self._i]
    if k then return k, self.index[k] end
  end
  return iter, self
end

function Hash:add(index, val)
  if NUMBER == type(index) then
    insert(self.index[self.index[index]], val)
  else
    insert(self.index[index], val)
  end
end

function Hash:set(index, val)
  if self.index[index] then
    self.index[self.index[index]] = val return true
  end
  return false
end

function Hash:get(index)
  if self.index[index] then return self.index[self.index[index]] end
  return nil
end

function Hash:__len()
  return #self.index
end

function Hash:__sub(hash)
  if not(TABLE == type(hash) and hash.__name == 'Hash') then
    error(format(patterns.EXPECT_HASH, type(hash)))
  end
  local new_index = {}
  for i=1, #self.index do
    if hash.index[self.index[i]] then
      insert(new_index, self.index[i])
      new_index[self.index[i]] = self.index[self.index[i]]
    end
  end
  return getmetatable(self)(new_index)
end

function Hash:__add(hash)
  if not(TABLE == type(hash) and hash.__name == 'Hash') then
    error(format(patterns.EXPECT_HASH, type(hash)))
  end
  local new_hash = getmetatable(self)(self.index)
  for i=1, #hash.index do
    if not new_hash.index[hash.index[i]] then
      new_hash[hash.index[i]] = hash.index[hash.index[i]]
    end
  end
  return new_hash
end

--
--
--
--
--

local function clone(orig)
  local orig_type, copy = type(orig), {}
  --local copy
  if TABLE == orig_type and orig.is then
    copy = orig:extends()
  elseif TABLE == orig_type then
    --copy = {}
    for orig_key, orig_value in next, orig, nil do
      copy[clone(orig_key)] = clone(orig_value)
    end
  else
    copy = orig
  end
  return copy
end

local function is_array(t)
  if not t or type(t) ~= TABLE then return false end
  local i = 0
  for _ in pairs(t) do
    i = i + 1
    if t[i] == nil then return false end
  end
  return true
end

local function args2array(args)
  if not args then return {} end
  if #args==1 and is_array(args[1]) then
    return args[1]
  elseif STRING == type(args[1]) and find(args[1], COMA, 1, true) ~= nil then
    local results = {}
    gsub(args[1], patterns.CAP2, function(st) insert(results, st) end)
    return results
  elseif TABLE == type(args[1]) and #args==1 then
    return args[1]
  else
    return args
  end
end

local function args2object(args)
  if TABLE == type(args[1]) then return args[1] end
  local obj = {}
  if nil ~= args[1] then obj[args[1]] = args[2] end
  return obj
end

local function check_same_len(length, expected)
  assert(length == expected, format(patterns.EXPECT_VAL_GOT, length, expected))
  --if length ~= expected then
  --  error(format(patterns.EXPECT_VAL_GOT, length, expected))
  --end
end

local function convert(col, new_aliases)
  local col_parts = {}
  for key in gmatch(col, "([^%.]+)") do insert(col_parts, key) end
  if #col_parts == 1 then return col end

  local tbl_ix    = #col_parts - 1
  local tbl_alias = col_parts[tbl_ix]
  if new_aliases[tbl_alias] then
    col_parts[tbl_ix] = new_aliases[tbl_alias]
    return concat(col_parts, DOT)
  end
  return col
end

local function convert_expr(expr, new_aliases)
  if expr.col then expr.col = convert(expr.col, new_aliases) end
  if expr.expressions then
    for i=1, #expr.expressions do
      convert_expr(expr.expressions[i], new_aliases)
    end
  end
end

local function namespaced_on(on, new_aliases)
  local namespace = {}
  for key, value in pairs(on) do
    namespace[convert(key, new_aliases)] = convert(value, new_aliases)
  end
  return namespace
end

-- define basic class-like metatables
local Select, Insert, Update, Delete
local Sql, Val, Statement, Join, Group, Not, Binary, Like, Between, Unary,
  In, Exists = class(), class(), class(), class(), class(), class(), class(),
  class(), class(), class(), class(), class()

Sql.__name, Val.__name, Statement.__name, Join.__name, Group.__name, Not.__name,
  Binary.__name, Like.__name, Between.__name, Unary.__name,
  In.__name, Exists.__name = 'Sql', 'Val', 'Statement', 'Join', 'Group', 'Not',
  'Binary', 'Like', 'Between', 'Unary', 'In', 'Exists'

function get_alias(tbl)
  local sep    = ESP_AS
  local sep_ix = find(tbl, sep, 1, true)
  if not sep_ix then
    sep    = SPACE
    sep_ix = find(tbl, sep, 1, true)
  end
  if sep_ix then return sub(tbl, sep_ix + #sep, -1) end
  return tbl
end

function get_table(tbl)
  local sep_ix = find(tbl, SPACE, 1, true)
  if sep_ix then return sub(tbl, 1, sep_ix - 1) end
  return tbl
end

local function is_expr(e)
  return e.is and (e:is(Group) or e:is(Not) or e:is(Binary) or e:is(Unary) or
    e:is(In) or e:is(Like) or e:is(Between) or e:is(Exists))
end

local function handle_value(val, opts)
  local _type = type(val)
  if TABLE == _type and val.is then
    if val:is(Statement) then return format(patterns.DPS, val:_toString(opts)) end
    if val:is(Sql) then return tostring(val) end
  end

  if opts and opts.parameterized then
    insert(opts.values, val)
    local prefix = EMPTY
    if opts.placeholder and opts.placeholder == '%' then
      prefix = '%s'
      opts.value_ix = opts.value_ix + 1
      return prefix
    else
      prefix = format(patterns.SS, (opts.placeholder or DOLLAR), tostring(opts.value_ix))
      opts.value_ix = opts.value_ix + 1
      return prefix
    end
  end

  if STRING == _type or TABLE == _type then
    return format(
        patterns.Q_S_Q,
        gsub(tostring(TABLE == _type and concat(val, COMA) or val), QUOTE, QQ)
      )
  end

  return val
end

local function handle_column(expr, opts)
  if expr.is then
    if expr:is(Statement) then return format(patterns.DPS, expr:_toString(opts)) end
    if expr:is(Val) then return handle_value(expr.val, opts) end
  end

  local db, tb, fd, al = match(expr, patterns.CAP3)
  if db then
    local tb_dot, db_dot = DOT, DOT
    if fd == EMPTY and tb == EMPTY then
      fd = (reserved[lower(db)] and format(patterns.DQS, db) or db)
      db = EMPTY db_dot = EMPTY tb_dot = EMPTY
    elseif fd == EMPTY and tb ~= EMPTY then
      fd, tb = (reserved[lower(tb)] and format(patterns.DQS, tb) or tb),
        (reserved[lower(db)] and format(patterns.DQS, db) or db) db = EMPTY db_dot = EMPTY
    else
      fd, tb, db = (reserved[lower(fd)] and format(patterns.DQS, fd) or fd),
        (reserved[lower(tb)] and format(patterns.DQS, tb) or tb),
        (reserved[lower(db)] and format(patterns.DQS, db) or db)
    end
    return format(
      patterns.S7, db, db_dot, tb, tb_dot, fd, (al == EMPTY and EMPTY or ESP_AS), al)
  end
  return expr
end

--
--
--
--
--

local function handles(res, data, opts, ...)
  local both, value, appends, addendum = ...
  both, value, appends, addendum =
    (both or false), (value or false), (appends or COMA_SPC), (addendum or SPACE)

  if both then
    for k,v in pairs(data) do
      insert(res, format(
        patterns.SEQSS, handle_column(k, opts), handle_value(v, opts), appends))
    end
    res[#res] = format(patterns.SS, sub(res[#res], 1, ((#appends + 1) * -1)), addendum)
    return res
  end

  if value then
    if data.is and data.is(data, Hash) and TABLE == type(data.get(data, 1)) then
      for i=1, #data.get(data, 1) do
        for k,v in pairs(data) do
          insert(res, format(patterns.SS, handle_value(v[i], opts), appends))
        end
        res[#res] = format('%s), (', sub(res[#res], 1, -3))
      end
      res[#res] = sub(res[#res], 1, -5)
      return res
    end
    for k, v in pairs(data) do
      insert(res, format(patterns.SS, handle_value(v, opts), appends))
    end
  else
    for key in pairs(data) do
      if NUMBER == type(key) then
        for k,v in pairs(data) do
          insert(res, format(patterns.SS, handle_column(v, opts), appends))
        end
      else
        for k in pairs(data) do
          insert(res, format(patterns.SS, handle_column(k, opts), appends))
        end
      end
      break
    end
  end
  res[#res] = format(patterns.SS, sub(res[#res], 1, ((#appends + 1) * -1)), addendum)
  return res
end

--
--
--
--
--

local function _and(...)            return Group('AND', args2array({...}))  end
local function _or(...)             return Group(OR, args2array({...}))   end
local function isNull(col)          return Unary('IS NULL', col)            end
local function isNotNull(col)       return Unary('IS NOT NULL', col)        end
local function innerJoin(self, ...) return self:_addJoins({...}, 'INNER')   end
local function leftJoin(self, ...)  return self:_addJoins({...}, 'LEFT')    end
local function rightJoin(self, ...) return self:_addJoins({...}, RIGHT)   end
local function fullJoin(self, ...)  return self:_addJoins({...}, FULL)    end
local function where(self, ...) return self:_addExpression({...}, '_where') end
local function group(self, ...) return self:_addListArgs({...}, GROUP_BY) end
local function order(self, ...) return self:_addListArgs({...}, ORDER_BY) end
local function orReplace(self)    self._or = OR_REPLACE  return self end
local function orRollback(self)   self._or = OR_ROLLBACK return self end
local function orAbort(self)      self._or = OR_ABORT    return self end
local function orFail(self)       self._or = OR_FAIL     return self end
local function orIgnore(self)     self._or = OR_IGNORE   return self end
local function eq(col, val)       return Binary(EQ, col, val)          end
local function eqAll(col, val)    return Binary(EQ, col, val, 'ALL ')  end
local function eqAny(col, val)    return Binary(EQ, col, val, 'ANY ')  end
local function notEq(col, val)    return Binary(NOT_EQ, col, val)         end
local function notEqAll(col, val) return Binary(NOT_EQ, col, val, 'ALL ') end
local function notEqAny(col, val) return Binary(NOT_EQ, col, val, 'ANY ') end
local function lt(col, val)       return Binary(LT, col, val)          end
local function ltAll(col, val)    return Binary(LT, col, val, 'ALL ')  end
local function ltAny(col, val)    return Binary(LT, col, val, 'ANY ')  end
local function le(col, val)       return Binary(LE, col, val)         end
local function leAll(col, val)    return Binary(LE, col, val, 'ALL ') end
local function leAny(col, val)    return Binary(LE, col, val, 'ANY ') end
local function gt(col, val)       return Binary(GT, col, val)          end
local function gtAll(col, val)    return Binary(GT, col, val, 'ALL ')  end
local function gtAny(col, val)    return Binary(GT, col, val, 'ANY ')  end
local function ge(col, val)       return Binary(GE, col, val)         end
local function geAll(col, val)    return Binary(GE, col, val, 'ALL ') end
local function geAny(col, val)    return Binary(GE, col, val, 'ANY ') end
local function into(self, tbl)     self._into = tbl return self end
local function intoTemp(self, tbl) self._into_tmp = true return self:into(tbl) end
local function forUpdate(self, ...)
  self.for_update = true
  return self:_addListArgs({...}, FOR_UPDATE_TBLS)
end

local function obj2equals(obj, expressions)
  for key in pairs(obj) do insert(expressions, eq(key, obj[key])) end
  return expressions
end

--
--
--
--
--

function Statement:__init(type)
  self.type = type
end

function Statement:clone()
  local ctor
  if     self:is(Select) then ctor = Select
  elseif self:is(Insert) then ctor = Insert
  elseif self:is(Update) then ctor = Update
  elseif self:is(Delete) then ctor = Delete end

  local stmt = ctor:extends(self)

  if stmt._where  then stmt._where  = clone(stmt._where)       end
  if stmt.joins   then stmt.joins   = clone(stmt.joins)        end
  if stmt._values then stmt._values = Hash(stmt._values.index) end
  return stmt
end

function Statement:toParams(opts)
  if self.prev_stmt then
    return self.prev_stmt:toParams(opts)
  end

  opts = opts or {}
  opts.parameterized, opts.values, opts.value_ix =
    (opts.parameterized or true), (opts.values or {}), (opts.value_ix or 1)

  local sql, result = (self .. opts), {}
  local vv, vk      = next(opts.values)

  if TABLE == type(vk) then
    for _, key in pairs(opts.values) do
      insert(result, (key.is and tostring(key) or concat(key, COMA)))
    end
  else
    for i=1, #opts.values do
      insert(result, opts.values[i])
    end
  end

  return {text=sql, values=result}
end

function Statement:__tostring()
  return self:__concat({})
end

function Statement:__concat(opts)
  if self.prev_stmt then return tostring(self.prev_stmt)
  else return self:_toString(opts) end
end

function Statement:_exprToString(opts, expr)
  expr = (expr or self._where)
  expr.parens = false
  if expr.expressions and #expr.expressions == 1 then
    expr.expressions[1].parens = false
  end
  return format(patterns.SSPACE, (expr .. opts))
end

function Statement:_add(arr, name)
  if not self[name] then self[name] = {} end

  if TABLE == type(arr) and not arr.is then
    for i=1, #arr do insert(self[name], arr[i]) end
  else
    insert(self[name], arr)
  end

  return self
end

function Statement:_addListArgs(args, name)
  return self:_add(args2array(args), name)
end

function Statement:_addExpression(args, name)
  if not self[name] then self[name] = _and() end
  if #args==2 and TABLE ~= type(args[1]) and TABLE ~= type(args[2]) or
   (#args==2 and args[1].is and (args[1]:is(Sql) or args[1]:is(Val)) or
   args[2] and args[2].is and (args[2]:is(Sql) or args[2]:is(Val))) then
    insert(self[name].expressions, eq(args[1], args[2]))
  else
    for k in pairs(args) do
      if is_expr(args[k]) then
        insert(self[name].expressions, args[k])
      else
        obj2equals(args[k], self[name].expressions)
      end
    end
  end
  return self
end

function Statement:_addJoins(args, _type)
  if not self.joins then self.joins = {} end
  local tbls, on

  if TABLE == type(args[2]) then
    tbls, on = {args[1]}, args[2]
  else
    tbls = args2array(args)
  end

  for _, tbl in pairs(tbls) do
    tbl = self:expandAlias(tbl)
    local left_tbl = self.last_join or (self.tbls and self.tbls[#self.tbls])
    insert(self.joins, Join(tbl, left_tbl, on, _type))
    self.joins[#self.joins]._joinCriteria = self._joinCriteria
  end

  self.last_join = tbls[#tbls]
  return self
end

function Statement:expandAlias(tbl)
  return self._aliases[tbl] and format(patterns.SSPACES, self._aliases[tbl], tbl) or tbl
end

--
--
--
--
--

Select = class(Statement)
Select.__name = 'Select'

function Select:__init(...)
  self.super.__init(self, _SELECT)
  return self:select(...)
end

function Select:select(...)
  return self:_addListArgs({...}, 'cols')
end

function Select:distinct(...)
  self._distinct = true
  return self:_addListArgs({...}, 'cols')
end

function Select:crossJoin(...)
  return self:_addJoins({...}, 'CROSS')
end

function Select:on(...)
  local on = ...
  local last_join, arr = self.joins[#self.joins]
  if is_expr(on) then
    last_join.on = on
  else
    last_join.on = last_join.on or {}
    local args = args2object({...})
    for k,v in pairs(args) do
      last_join.on[k] = v
    end
  end
  return self
end

function Select:having(...)
  return self:_addExpression({...}, _HAVING)
end

function Select:limit(count)
  self._limit = count
  return self
end

function Select:offset(count)
  self._offset = count
  return self
end

function Select:noWait()
  self.no_wait = true
  return self
end

function Select:from(...)
  local arr = args2array({...})
  for i=1, #arr do
    arr[i] = self:expandAlias(arr[i])
  end
  return self:_add(arr, TBLS)
end

Select.into           = into
Select.intoTable      = into
Select.intoTemp       = intoTemp
Select.intoTempTable  = intoTemp
Select.forUpdate      = forUpdate
Select.forUpdateOf    = forUpdate
Select.join           = innerJoin
Select.innerJoin      = innerJoin
Select.leftJoin       = leftJoin
Select.leftOuterJoin  = leftJoin
Select.rightJoin      = rightJoin
Select.rightOuterJoin = rightJoin
Select.fullJoin       = fullJoin
Select.fullOuterJoin  = fullJoin
Select.where          = where
Select._and           = where
Select.group          = group
Select.groupBy        = group
Select.order          = order
Select.orderBy        = order

for key, value in pairs(compounds) do
  Select[key] = function(self, ...)
    local stmts, stmt = args2array({...})
    if TABLE == type(stmts) and #stmts == 0 and not stmts.is then
      stmt = Select()
      stmt.prev_stmt, stmt._aliases, stmt._views, stmt._joinCriteria =
        self, self._aliases, self._views, self._joinCriteria
      stmts = {stmt}
    end

    self:_add(stmts, '_' .. key)

    if stmt then
      return stmt
    end
    return self
  end
end

function Select:joinView(view, on, _type)
  local alias, view_name = get_alias(view), get_table(view)
  local view             = self._views[view_name]
  local tbl              = format(patterns.SSPACES, get_table(view.tbls[1]), alias)
  local new_aliases      = {[get_alias(view.tbls[1])] = alias}

  self:_addJoins({tbl, (on or {})}, (_type and upper(_type) or 'INNER'))

  if view.joins then
    local j_alias, j_tbl, _tbl  = EMPTY, EMPTY, EMPTY
    for i=1, #view.joins do
      j_alias, j_tbl = get_alias(view.joins[i].tbl), get_table(view.joins[i].tbl)
      new_aliases[j_alias] = format(patterns.S_S, alias, j_alias)
      _tbl = format(patterns.SSPACESSUNDERS, j_tbl, alias, j_alias)
      local join =
        Join(_tbl, view.joins[i].left_tbl, view.joins[i].on, view.joins[i].type)
      join._joinCriteria = self._joinCriteria
      if not join.on then join.on = join:autoGenerateOn(_tbl, join.left_tbl) end
      insert(self.joins, join)
      join.on = namespaced_on(join.on, new_aliases)
    end
  end

  if view._where then
    for i=1, #view._where.expressions do
      local expr = view._where.expressions[i]:extends()
      convert_expr(expr, new_aliases)
      self:where(expr)
    end
  end

  return self
end

function Select:_toString(opts)
  local cols, res = (#self.cols > 0 and self.cols or {STAR}), {SELECT}
  if self._distinct then insert(res, 'DISTINCT ') end
  handles(res, cols, opts)
  if self._into then
    insert(res, format(patterns.INTO_SS, (self._into_tmp and TEMP or EMPTY), self._into))
  end
  if self.tbls then
    insert(res, format(patterns.FROM_S, concat(self.tbls, COMA_SPC)))
  end
  if self.joins then
    for i=1, #self.joins do
      insert(res, format(patterns.SSPACE, (self.joins[i] .. opts)))
    end
  end
  if self._where then
    insert(res, format(patterns.WHERE_S, self:_exprToString(opts)))
  end
  if self.group_by then
    insert(res, GROUPBY)
    handles(res, self.group_by, opts)
  end
  if self._having then
    insert(res, format(patterns.HVG_S, self:_exprToString(opts, self._having)))
  end
  if self.order_by then
    insert(res, ORDERBY)
    handles(res, self.order_by, opts)
  end
  if self._limit then
    insert(res, format(patterns.LIMIT_S, tostring(self._limit)))
  end
  if self._offset then
    insert(res, format(patterns.OFFSET_S, tostring(self._offset)))
  end

  for key, value in pairs(compounds) do
    local stmt = self['_' .. key]
    if stmt and #stmt > 0 then
      insert(res, value)
      insert(res, SPACE)
      for i=1, (#stmt - 1) do
        insert(res, format(patterns.SSPACESSSPACE, stmt[i]:_toString(opts), value))
      end
      insert(res, stmt[#stmt]:_toString(opts))
      insert(res, SPACE)
    end
  end

  if self.for_update then
    insert(res, FOR_UPDATE)
    if self.for_update_tbls then
      insert(res, concat(self.for_update_tbls, COMA_SPC))
      insert(res, SPACE)
    end
    if self.no_wait then insert(res, 'NO WAIT ') end
  end

  return sub(concat(res), 1, -2)
end

--
--
--
--
--

Insert = class(Statement)
Insert.__name = 'Insert'

function Insert:__init(expansions, tbl, ...)
  self._aliases = expansions
  self.super.__init(self, 'insert')
  return self:into(tbl, ...)
end

Insert.orReplace  = orReplace
Insert.orRollback = orRollback
Insert.orAbort    = orAbort
Insert.orFail     = orFail
Insert.orIgnore   = orIgnore

function Insert:into(tbl, ...)
  if tbl then self.tbls = {self:expandAlias(tbl)} end
  local values = {...}
  if #values > 0 then
    if STRING == type(values[1]) or
      (is_array(values[1]) and STRING == type(values[1][1]))
    then
      self._split_keys_vals_mode, self._values = true, Hash(args2array({...}))
    else
      self:values(...)
    end
  end
  return self
end

function Insert:values(...)
  local values, check = args2array({...}), 0
  if self._split_keys_vals_mode and #values > 0 then
    if TABLE == type(values[1]) then
      check_same_len(#self._values, #values[1])
      for i=1, #values[1] do self._values.set(self._values, i, {values[1][i]}) end
      for i=2, #values do
        check_same_len(#self._values, #values[i])
        for k=1, #values[i] do self._values.add(self._values, k, values[i][k]) end
      end
    else
      check_same_len(#self._values, #values)
      for k=1, #values do self._values.set(self._values, k, values[k]) end
    end
    goto returning
  end

  self._values = self._values or Hash()
  if TABLE == type(values[1]) and TABLE == type(values[1][1]) then
    for k,v in pairs(values[1][1]) do self._values[k] = {v} end
    for i=2, #values[1] do
      check_same_len(#self._values, #values[1][i])
      for k,v in pairs(values[1][i]) do self._values.add(self._values, k, v) end
    end
  else
    for k,v in pairs((values[1] or values)) do self._values[k] = v end
  end

  ::returning::
  return self
end

function Insert:select(...)
  self._select = Select(...)
  self._select._aliases, self._select._views, self._select._joinCriteria =
    self._aliases, self._views, self._joinCriteria
  self._select.prev_stmt = self
  return self._select
end

function Insert:returning(...)
  return self._addListArgs({...}, _RETURNING)
end

function Insert:_toString(opts)
  local res = {'INSERT '}
  if self._or then insert(res, self._or) end
  insert(res, format(patterns.INTO_S_P, concat(self.tbls, COMA_SPC)))
  handles(res, self._values, opts, false, false, nil, EMPTY)
  insert(res, PAR_SPC)
  if self._select then
    insert(res, self._select:_toString(opts))
    insert(res, SPACE)
  else
    insert(res, VALS_P)
    handles(res, self._values, opts, false, true, nil, EMPTY)
    insert(res, PAR_SPC)
  end
  if self._returning then
    insert(res, RETURNING)
    handles(res, self._returning, opts)
  end

  return sub(concat(res), 1, -2)
end

--
--
--
--
--

Update = class(Statement)
Update.__name = 'Update'

function Update:__init(expansions, tbl, ...)
  self._aliases = expansions
  self.super.__init(self, _UPDATE)
  self.tbls = {self:expandAlias(tbl)}
  if ... then self:set(...) end
  return self
end

Update.orReplace  = orReplace
Update.orRollback = orRollback
Update.orAbort    = orAbort
Update.orFail     = orFail
Update.orIgnore   = orIgnore
Update.where      = where
Update._and       = where

function Update:set(...)
  local args = args2object({...})
  self._values = self._values or Hash()
  for k,v in pairs(args) do self._values[k] = v end
  return self
end

function Update:values(...)
  return self:set(...)
end

function Update:_toString(opts)
  local res = {UPDATE}
  if self._or then insert(res, self._or) end
  insert(res, format(patterns.S_SET, self.tbls[1]))
  handles(res, self._values, opts, true)
  if self._where then insert(res, format(patterns.WHERE_S, self:_exprToString(opts))) end

  return sub(concat(res), 1, -2)
end

--
--
--
--
--

Delete = class(Statement)
Delete.__name = 'Delete'

function Delete:__init(expansions, ...)
  self._aliases = expansions
  self.super.__init(self, 'delete')
  if ... then self.tbls = {self:expandAlias(...)} end
  return self
end

function Delete:from(...)
  local arr = args2array({...})
  for i=1, #arr do arr[i] = self:expandAlias(arr[i]) end
  return self:_add(arr, TBLS)
end

Delete.where = where
Delete._and  = where

function Delete:using(...)
  local args = args2array({...})
  for i=1, #args do args[i] = self:expandAlias(args[i]) end
  return self:_add(args, USING)
end

function Delete:_toString(opts)
  local res = {'DELETE FROM ', self.tbls[1], SPACE}
  if self._using then insert(res, format(patterns.USING_S, concat(self._using, COMA_SPC))) end
  if self._where then insert(res, format(patterns.WHERE_S, self:_exprToString(opts))) end
  return sub(concat(res), 1, -2)
end

--
--
--
--
--

function Join:__init(tbl, left_tbl, on, type)
  self.tbl, self.left_tbl, self.on, self.type = tbl, left_tbl, on, type
end

function Join:autoGenerateOn(tbl, left_tbl)
  return self._joinCriteria(
      get_table(left_tbl), get_alias(left_tbl), get_table(tbl), get_alias(tbl)
    )
end

function Join:__concat(opts)
  local on, result = self.on, {}
  if not on or next(on) == nil then
    if self._joinCriteria then
      on = self:autoGenerateOn(self.tbl, self.left_tbl)
    else
      error(format('No join criteria supplied for "%s" join', get_alias(self.tbl)))
    end
  end
  if is_expr(on) then
    on = on .. opts
  else
    for key, val in pairs(on) do
      insert(result, format(patterns.SEQS, handle_column(key, opts), handle_column(val, opts)))
      insert(result, ESP_AND)
    end
    result[#result] = nil
    on = concat(result)
  end
  return format(patterns.S_JOIN_S_ON_S, self.type, self.tbl, on)
end

function Join:__tostring() return self:__concat({}) end

--
--
--
--
--

function Group:__init(op, expressions)
  self.op, self.expressions = op, {}
  for k in pairs(expressions) do
    if is_expr(expressions[k]) then
      insert(self.expressions, expressions[k])
    else
      obj2equals(expressions[k], self.expressions)
    end
  end
end

function Group:clone()
  return Group(self.op, self.expressions:extends())
end

function Group:__concat(opts)
  local res = {}
  for k in pairs(self.expressions) do
    insert(res, format("%s %s ", (self.expressions[k] .. opts), self.op))
  end
  res[#res] = sub(res[#res], 1, (#self.op + 3) * -1)
  if #self.expressions > 1 and (self.parens or self.parens == nil) then
    return format("(%s)", concat(res))
  end
  return concat(res)
end

function Group:__tostring() return self:__concat({}) end

--
--
--
--
--

function Not:__init(expr)
  self.expressions = is_expr(expr) and {expr} or {_and({expr})}
end

function Not:clone()
  return Not(self.expressions[1]:extends())
end

function Not:__concat(opts)
  return format(patterns.NOT_S, self.expressions[1] .. opts)
end

function Not:__tostring() return self:__concat({}) end

--
--
--
--
--

function Binary:__init(op, col, val, ...)
  self.op, self.col, self.val, self.quantifier =
    op, col, val, (... or EMPTY)
end

function Binary:clone()
  return Binary(self.op, self.col, self.val)
end

function Binary:__concat(opts)
  return format(patterns.SSPACESSSPACESS, handle_column(self.col, opts), self.op,
    self.quantifier, handle_value(self.val, opts))
end

function Binary:__tostring() return self:__concat({}) end

--
--
--
--
--

function Like:__init(col, val, ...)
  self.col, self.val, self.escape_char = col, val, ...
end

function Like:clone()
  return Like(self.col, self.val, self.escape_char)
end

function Like:__concat(opts)
  return format(
      patterns.S_LIKE_SS, handle_column(self.col, opts), handle_value(self.val, opts),
      (self.escape_char and format(patterns.ESCAPE_Q_S, self.escape_char) or EMPTY)
    )
end

function Like:__tostring() return self:__concat({}) end

--
--
--
--
--

function Between:__init(col, val1, val2)
  self.col, self.val1, self.val2 = col, val1, val2
end

function Between:clone()
  return Between(self.col, self.val1, self.val2)
end

function Between:__concat(opts)
  return format(patterns.S_BTW_S_AND_S, handle_column(self.col, opts),
    handle_value(self.val1, opts), handle_value(self.val2, opts))
end

function Between:__tostring() return self:__concat({}) end

--
--
--
--
--

function Unary:__init(op, col)
  self.op, self.col = op, col
end

function Unary:clone()
  return Unary(self.op, self.col)
end

function Unary:__concat(opts)
  return format(patterns.SSPACES, handle_column(self.col, opts), self.op)
end

function Unary:__tostring() return self:__concat({}) end

--
--
--
--
--

function In:__init(col, ...)
  self.col, self.list = col, ...
  self.list = TABLE ~= type(self.list) and {...} or self.list
end

function In:clone()
  return In(self.col, clone(self.list))
end

function In:__concat(opts)
  local res = EMPTY
  if is_array(self.list) then
    res = sub(concat(handles({}, self.list, opts, false, true)), 1, -2)
  elseif self.list.is and self.list:is(Statement) then
    res = self.list:_toString(opts)
  end

  return format(patterns.S_IN_S, handle_column(self.col, opts), res)
end

function In:__tostring() return self:__concat({}) end

--
--
--
--
--

function Exists:__init(subquery)
  self.subquery = subquery
end

function Exists:clone()
  return Exists(clone(self.subquery))
end

function Exists:__concat(opts)
  return format(patterns.EXISTS_S, self.subquery:_toString(opts))
end

function Exists:__tostring() return self:__concat({}) end

--
--
--
--
--

function Val:__init(_val) self.val = _val end

function Val:__tostring() return self.val end

--
--
--
--
--

function Sql:__init(str)   self.str = str end

function Sql:__tostring()  return self.str end

function Sql:__concat(arg) return self.str .. arg end

--
--
--
--
--

local SQLRocks = {}
SQLRocks.__index = SQLRocks

function SQLRocks:__call(_expansions, _criteria, _views)
  local expansions, criteria, views, this =
    (TABLE    == type(_expansions) and _expansions or {}),
    (FUNCTION == type(_criteria)   and _criteria   or nil),
    (TABLE    == type(_views)      and _views      or {}),
    {
      val       = Val,       sql    = Sql,    _and     = _and,     _or    = _or,
      _not      = Not,       like   = Like,   between  = Between,  isNull = isNull,
      isNotNull = isNotNull, exists = Exists, eq       = eq,       eqAll  = eqAll,
      eqAny     = eqAny,     notEq  = notEq,  notEqAll = notEqAll, ltAny  = ltAny,
      notEqAny  = notEqAny,  lt     = lt,     ltAll    = ltAll,    le     = le,
      leAll     = leAll,     leAny  = leAny,  gt       = gt,       gtAll  = gtAll,
      gtAny     = gtAny,     ge     = ge,     geAll    = geAll,    geAny  = geAny,
      _in       = In
    }

  this.addView = function(name, sel)
    if #sel.tbls ~= 1 then
      error(format(patterns.UNSP_N_D, #sel.tbls))
    end
    views[name] = sel
  end
  this.getView         = function(name) return views[name] end
  this.aliasExpansions = function(_expansions) expansions = _expansions end
  this.joinCriteria    = function(_criteria) criteria = _criteria end

  this.select, this.insert, this.update, this.delete, this.Join =
    function(...)
      local sel = Select(...)
      sel._aliases, sel._views, sel._joinCriteria = expansions, views, criteria
      return sel
    end,
    function(...)
      local ins = Insert(expansions, ...)
      ins._views, ins._joinCriteria = views, criteria
      return ins
    end,
    function(...) return Update(expansions, ...) end,
    function(...) return Delete(expansions, ...) end,
    function(...) local joi = Join(...) joi._joinCriteria = criteria return joi end

  this.insertInto, this.deleteFrom = this.insert, this.delete

  return this
end

return setmetatable({
    Statement = Statement, Join   = Join,   Select = Select,
    Insert    = Insert,    Update = Update, Delete = Delete,
    Sql       = Sql,       Val    = Val,    Group  = Group,
    Not       = Not,       Binary = Binary, Like   = Like,
    Between   = Between,   Unary  = Unary,  In     = In,     Exists = Exists
  }, SQLRocks)
