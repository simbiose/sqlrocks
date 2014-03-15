
local class, string, math, table =
  require '30log', require 'string', require 'math', require 'table'

--local ppx = require('utils')
local pp=function(...) end
local ppx=pp
--local ppx = print

local find, match, gsub, sub, lower, upper, format, max, floor, concat, type, pairs,
  ipairs, tostring, select, next, error, setmetatable, getmetatable =
  string.find, string.match, string.gsub, string.sub, string.lower, string.upper,
  string.format, math.max, math.floor, table.concat, type, pairs, ipairs, tostring,
  select, next, error, setmetatable, getmetatable

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
  rollback=true, row=true, savepoint=true, select=true, session_user=true, set=true,
  similar=true, some=true, symmetric=true, table=true, temp=true, temporary=true,
  ['then']=true, trailing=true, transaction=true, trigger=true, ['true']=true,
  union=true, unique=true, update=true, to=true, user=true, using=true, vacuum=true,
  values=true, variadic=true, verbose=true, view=true, virtual=true, when=true,
  where=true, window=true, with=true}

local compounds = {
  union='UNION', unionAll='UNION ALL', intersect='INTERSECT',
  intersectAll='INTERSECT ALL', minus='MINUS', minusAll='MINUS ALL',
  except='EXCEPT', exceptAll='EXCEPT ALL'}

local function clone(val)
  if type(val) ~= 'table' then return val end
  local mt, res = getmetatable(val), {}
  for k,v in pairs(val) do
    if type(v) == 'table' then
      v = clone(v)
    end
    res[k] = v
  end
  setmetatable(res, mt)
  return res
end

local function extend(original, ...)
  local args = {...}
  for i=1, #args do
    for k,v in pairs(args[i]) do
      if 'table' == type(v) and v.key and v.val then
        original[v.key] = v.val
      else
        original[k] = v
      end
    end
  end
  return original
end

local function concatenate(...)
  local result = {}
  for i=1, select('#', ...) do
    local arg = select(i, ...)
    if type(arg) == 'table' then
      for _, v in ipairs(arg) do
        result[#result + 1] = v
      end
    else
      result[#result + 1] = arg
    end
  end
  return result
end

local function trim(s)
  return match(s,'^()%s*$') and '' or match(s,'^%s*(.*%S)')
end

local function split(delimiter, text, ...)
  local list, pos, no_patterns = {}, 1, (... or false)
  if find('', delimiter, 1) then return list end
  while true do
    local first, last = find(text, delimiter, pos, no_patterns)
    if first then
      list[#list + 1] = sub(text, pos, first-1)
      pos = last+1
    else
      list[#list + 1] = sub(text, pos)
      break
    end
  end
  return list
end

local function is_array(array)
  if not array or 'table' ~= type(array) then
    return false
  end
  for k in pairs(array) do
    if type(k) ~= 'number' or k < 0 or floor(k) ~= k then
      return false
    end
  end
  return true
end

local function args2array(args)
  if not args then return {} end
  if #args==1 and is_array(args[1]) then
    return args[1]
  elseif 'string' == type(args[1]) and find(args[1], ',', 1, true) ~= nil then
    local results = split(',', args[1], true)
    for i=1, #results do results[i] = trim(results[i]) end
    return results
  elseif 'table' == type(args[1]) then
    return args[1]
  else
    ppx(' returning ')
    return args
  end
end

local function args2object(args)
  if 'table' == type(args[1]) then return args[1] end
  local obj = {}
  if nil ~= args[1] then obj[args[1]] = args[2] end
  return obj
end

local function convert(col, new_aliases)
  local col_parts = split('.', col)
  if #col_parts == 1 then return col end

  local tbl_ix    = #col_parts - 2
  local tbl_alias = col_parts[tbl_ix]
  if new_aliases[tbl_alias] then
    col_parts[tbl_ix] = new_aliases[tbl_alias]
    return concat(col_parts, '.')
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
  local sep    = ' AS '
  local sep_ix = find(tbl, sep, 1, true)
  if not sep_ix then
    sep    = ' '
    sep_ix = find(tbl, sep, 1, true)
  end
  if sep_ix then return sub(tbl, sep_ix + #sep, -1) end
  return tbl
end

function get_table(tbl)
  local sep_ix = find(tbl, ' ', 1, true)
  if sep_ix then return sub(tbl, 1, sep_ix - 1) end
  return tbl
end

local function is_expr(e)
  return e.is and (e:is(Group) or e:is(Not) or e:is(Binary) or e:is(Unary) or 
    e:is(In) or e:is(Like) or e:is(Between) or e:is(Exists))
end

-- TODO: refactor
local function quote_reserved_column(expr)
  local prefix, suffix, mark = '', '', find(expr, '.', 1, true)
  if mark > -1 then
    prefix, expr = sub(expr, 1, mark), sub(expr, mark + 1, -1)
  end
  mark = find(expr, ' ', 1 , true)
  if mark > -1 then
    suffix, expr = sub(expr, mark, -1), sub(expr, 1, mark - 1)
  end
  if reserved[lower(expr)] then
    expr = format('"%s"', expr)
  end
  return prefix .. expr .. suffix
end

local function handle_value(val, opts)
  if 'table' == type(val) and val.is then
    if val:is(Statement) then return '(' .. val:_toString(opts) .. ')' end
    if val:is(Sql) then return tostring(val) end
  end

  if opts and opts.parameterized then
    opts.values[#opts.values + 1] = val
    local prefix = (opts.placeholder or '$')
    return prefix .. (opts.value_ix + 1)
  end

  if 'string' == type(val) then
    return ("'" .. gsub(tostring(val), "'", "''") .. "'")
  end

  return val
end

local function handle_column(expr, opts)
  ppx(' handle column ', expr, opts)
  if expr.is then
    if expr.is(Statement) then return '('.. expr:_toString(opts) ..')' end
    if expr.is(val) then return handle_value(expr.val, opts) end
  end

  if match(expr, "[%w%_%.%d]+( AS [%w%d%_]+)?") then
    return quote_reserved_column(expr)
  else
    return expr
  end
end

--
--
--
--
-- res, data, opts, multiples=false, value=false, appends=' '

local function handles(res, data, opts, ...)
  pp(' handles ', res, data, opts)
  local both, value, appends, addendum = ...
  both, value, appends, addendum =
    (both or false), (value or false), (appends or ', '), (addendum or ' ')

  if both then
    for k in pairs(data) do
      if 'table' == type(data[k]) and data[k].key then
        res[#res + 1] = format(
            '%s = %s%s',
            handle_column(data[k].key, opts),
            handle_value(data[k].val, opts),
            appends
          )
      else
        res[#res + 1] = format(
          '%s = %s%s', handle_column(k, opts), handle_value(data[k], opts), appends
        )
      end
    end
    res[#res] = sub(res[#res], 1, ((#appends + 1) * -1)) .. addendum
    return res
  end

  if value then
    local _, total = next(data)
    if 'table' == type(total) and 'table' == type(total.val) then
      for w=1, #total.val do
        for k=1, #data do
          res[#res + 1] =
            format('%s%s', handle_value(data[k].val[w], opts), appends)
        end
        res[#res] = format('%s), (', sub(res[#res], 1, -3))
      end
      res[#res] = sub(res[#res], 1, -5)
      return res
    else
      ppx(data)
      for k in pairs(data) do
        res[#res + 1] =
          format('%s%s', handle_value((data[k].val or data[k]), opts), appends)
      end
    end
  else
    for k in pairs(data) do
      if 'number' == type(k) then
        res[#res + 1] = 
          format('%s%s', handle_column((data[k].key or data[k]), opts), appends)
      else
        res[#res + 1] = 
          format('%s%s', handle_column((data[k].key or k), opts), appends)
      end
    end
  end
  res[#res] = sub(res[#res], 1, ((#appends + 1) * -1)) .. addendum
  ppx(res)
  return res
end

--
--
--
--
--

local function _and(...)            return Group('AND', args2array({...}))  end
local function _or(...)             return Group('OR', args2array({...}))   end
local function isNull(col)          return Unary('IS NULL', col)            end
local function isNotNull(col)       return Unary('IS NOT NULL', col)        end
local function innerJoin(self, ...) return self:_addJoins({...}, 'INNER')   end
local function leftJoin(self, ...)  return self:_addJoins({...}, 'LEFT')    end
local function rightJoin(self, ...) return self:_addJoins({...}, 'RIGHT')   end
local function fullJoin(self, ...)  return self:_addJoins({...}, 'FULL')    end
local function where(self, ...) return self:_addExpression({...}, '_where') end
local function group(self, ...) return self:_addListArgs({...}, 'group_by') end
local function order(self, ...) return self:_addListArgs({...}, 'order_by') end
local function orReplace(self)    self._or = 'OR REPLACE '  return self end
local function orRollback(self)   self._or = 'OR ROLLBACK ' return self end
local function orAbort(self)      self._or = 'OR ABORT '    return self end
local function orFail(self)       self._or = 'OR FAIL '     return self end
local function orIgnore(self)     self._or = 'OR IGNORE '   return self end
local function eq(col, val)       return Binary('=', col, val)          end
local function eqAll(col, val)    return Binary('=', col, val, 'ALL ')  end
local function eqAny(col, val)    return Binary('=', col, val, 'ANY ')  end
local function notEq(col, val)    return Binary('<>', col, val)         end
local function notEqAll(col, val) return Binary('<>', col, val, 'ALL ') end
local function notEqAny(col, val) return Binary('<>', col, val, 'ANY ') end
local function lt(col, val)       return Binary('<', col, val)          end
local function ltAll(col, val)    return Binary('<', col, val, 'ALL ')  end
local function ltAny(col, val)    return Binary('<', col, val, 'ANY ')  end
local function le(col, val)       return Binary('<=', col, val)         end
local function leAll(col, val)    return Binary('<=', col, val, 'ALL ') end
local function leAny(col, val)    return Binary('<=', col, val, 'ANY ') end
local function gt(col, val)       return Binary('>', col, val)          end
local function gtAll(col, val)    return Binary('>', col, val, 'ALL ')  end
local function gtAny(col, val)    return Binary('>', col, val, 'ANY ')  end
local function ge(col, val)       return Binary('>=', col, val)         end
local function geAll(col, val)    return Binary('>=', col, val, 'ALL ') end
local function geAny(col, val)    return Binary('>=', col, val, 'ANY ') end
local function into(self, tbl)     self._into = tbl return self end
local function intoTemp(self, tbl) self._into_tmp = true return self:into(tbl) end
local function forUpdate(...)
  self.for_update = true
  return self:_addListArgs({...}, 'for_update_tbls')
end

local function obj2equals(obj, expressions)
  ppx(obj, 'obj2equals')
  for key in pairs(obj) do
    expressions[#expressions + 1] = eq(key, obj[key])
  end
  return expressions
end

--
--
--
--
--

function Statement:__init(type)
  pp(' Statement ', type)
  self.type = type
end

function Statement:clone()
  local ctor
  if     self:is(Select) then ctor = Select
  elseif self:is(Insert) then ctor = Insert
  elseif self:is(Update) then ctor = Update
  elseif self:is(Delete) then ctor = Delete end

  local stmt = ctor:extends(self)
  if stmt._where  then stmt._where  = stmt._where:clone() end
  if stmt.joins   then stmt.joins   = stmt.joins:slice()  end
  if stmt._values then stmt._values = clone(stmt._values) end
  return stmt
end

function Statement:toParams(opts)
  if(self.prev_stmt) then
    return self.prev_stmt:toParams(opts)
  end

  opts = opts or {}
  extend(opts, {parameterized=true, values={}, value_ix=1})
  local sql = self .. opts
  
  opts.values = opts.values.map(objToString)
  return {text=sql, values=opts.values}
end

function Statement:__tostring()
  if self.prev_stmt then return self.prev_stmt
  else return trim(self:_toString({})) end
end

function Statement:_exprToString(opts, expr)
  expr = (expr or self._where)
  expr.parens = false
  if expr.expressions and #expr.expressions == 1 then
    expr.expressions[1].parens = false
  end
  return (expr .. opts) .. ' '
end

function Statement:_add(arr, name)
  if not self[name] then self[name] = {} end
  ppx(arr, name)
  self[name] = concatenate(self[name], arr)
  return self
end

function Statement:_addToObj(obj, name)
  if not self[name] then self[name] = {} end

  extend(self[name], obj)
  return self
end

function Statement:_addListArgs(args, name)
  pp(' _addListArgs ', args, name)
  return self:_add(args2array(args), name)
end

function Statement:_addExpression(args, name)
  --ppx(' _addExpression ', args, name)
  if not self[name] then self[name] = _and() end
  if #args==2 and 'table' ~= type(args[1]) and 'table' ~= type(args[2]) or 
   (#args==2 and args[1].is and (args[1]:is(sql) or args[1]:is(val)) and
   args[2].is and (args[2]:is(sql) or args[2]:is(val))) then
    self[name].expressions[#self[name].expressions + 1] = eq(args[1], args[2])
  else
    for k in pairs(args) do
      if is_expr(args[k]) then
        self[name].expressions[#self[name].expressions + 1] = args[k]
      else
        obj2equals(args[k], self[name].expressions)
      end
    end
  end
  return self
end

function Statement:_addJoins(args, type)
  if not self.joins then self.joins = {} end
  local tbls, on

  if 'table' == type(args) then
    tbls, on = {args[1]}, args[2]
  else
    tbls = args2array(args)
  end

  for tbl in pairs(tbls) do
    tbl = self:expandAlias(tbl)
    local left_tbl = self.last_join or (self.tbls and self.tbls[#self.tbls - 1])
    self.joins[#self.joins + 1] = Join(tbl, left_tbl, on, type)
    self.joins[#self.joins]._joinCriteria = self._joinCriteria
  end

  self.last_join = tbls[#tbls - 1]
  return self
end

function Statement:expandAlias(tbl)
  return self._aliases[tbl] and (self._aliases[tbl] ..' '.. tbl) or tbl
end

--
--
--
--
--

Select = Statement:extends()
Select.__name = 'Select'
function Select:__init(...)
  ppx(' select ', ...)
  self.super.__init(self, 'select')
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

function Select:on(on, ...)
  local last_join = self.joins[#self.joins - 1]
  if is_expr(on) then
    last_join.on = on
  else
    if not last_join.on then
      last_join.on = {}
    end
    extend(last_join.on, args2object(...))
  end
  return self
end

function Select:having(...)
  return self:_addExpression({...}, '_having')
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
  return self:_add(arr, 'tbls')
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
    local stmt
    local stmts = args2array({...})
    if 'table' ~= type(stmts) and #stmts == 0 then
      stmt = Select()
      stmt.prev_stmt = self
      stmts = {stmt}
    end

    self:_add(stmts, '_' .. key)

    if stmt then return stmt end
    return self
  end
end

function Select:joinView(view, on, _type)
  local alias, view_name = get_alias(view), get_table(view)
  local view             = self._views[view_name]
  local tbl              = format('%s %s', get_table(view.tbls[1]), alias)
  local new_aliases      = {[get_alias(view.tbls[1])] = alias}

  self:_addJoins({tbl, (on or {})}, (_type and upper(_type) or 'INNER'))

  if view.joins then
    local j_alias, j_tbl, _tbl  = '', '', ''
    for i=1, #view.joins do
      j_alias, j_tbl = get_alias(view.joins[i].tbl), get_table(view.joins[i].tbl)
      new_aliases[j_alias] = format('%s_%s', alias, j_alias)
      _tbl = format('%s %s_%s', j_tbl, alias, j_alias)
      local join =
        Join(_tbl, view.joins[i].left_tbl, views.joins[i].on, views.joins[i].type)
      self.joins[#self.joins + 1] = join
      if join.on then join.on = join.autoGenerateOn(_tbl, join.left_tbl) end
      join.on = namespace_on(join.on, new_aliases)
    end
  end

  if view._where then
    for i=1, #view._where.expressions do
      local expr = view._where.expressions[i]:clone()
      convert_expr(expr, new_aliases)
      self:where(expr)
    end
  end
end

function Select:_toString(tbl)
  ppx(self.cols)
  local cols, res = (#self.cols > 0 and self.cols or {'*'}), {'SELECT '}
  if self._distinct then res[#res + 1] = 'DISTINCT ' end
  handles(res, cols, opts)
  if self._into then
    res[#res + 1] = 'INTO '
    if self._into_temp then res[#res + 1] = 'TEMP ' end
    res[#res + 1] = self._into .. ' '
  end
  if self.tbls then
    res[#res + 1] = format('FROM %s ', concat(self.tbls, ', '))
  end
  if self.joins then
    for i=1, #self.joins do
      res[#res + 1] = format('%s ', (self.joins[i] .. opts))
    end
  end
  if self._where then
    res[#res + 1] = format('WHERE %s ', self:_exprToString(opts))
  end
  if self.group_by then
    res[#res + 1] = 'GROUP BY ' handles(res, self.group_by, opts)
  end
  if self._having then
    res[#res + 1] = format('HAVING %s ', self:_exprToString(opts, self._having))
  end
  if self._order_by then
    res[#res + 1] = 'ORDER BY ' handles(res, self.order_by, opts)
  end
  if self._limit then
    res[#res + 1] = format('LIMIT %s ', tostring(self._limit))
  end
  if self._offset then
    res[#res + 1] = format('OFFSET %s ', tostring(self._offset))
  end

  for key, value in pairs(compounds) do
    local stmt = self['_' .. key]
    if stmt then
      res[#res + 1] = value .. ' '
      for i=1, #stmt do
        res[#res + 1] = (stmt[i] .. opts) .. 
          (i == #stmt and ' ' or ' ' .. value .. ' ')
      end
    end
  end

  if self.for_update then
    res[#res + 1] = 'FOR UPDATE '
    if self.for_update_tbls then
      res[#res + 1] = concat(self.for_update_tbls, ', ') .. ' '
    end
    if self.no_wait then
      res[#res + 1] = 'NO WAIT '
    end
  end

  return concat(res)
end

--
--
--
--
--

Insert = Statement:extends()
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
    ppx(is_array(values[1]), values[1][1])
    if 'string' == type(values[1]) or 
      (is_array(values[1]) and 'string' == type(values[1][1]))
    then

      self._split_keys_vals_mode, self._values = true, {}
      local arr = args2array({...})
      ppx(' into! ', arr)
      for k=1, #arr do
        self._values[#self._values + 1] = {key=arr[k]}
      end
    else
      ppx(' goto values ! ')
      self:values(...)
    end
  end
end
function Insert:values(...)
  local values = args2array({...})
  if self._split_keys_vals_mode then
    --
    if #values > 0 then
      ppx(values, #values)
      if 'table' == type(values[1]) then
        local y, x = 1, 1
        while true do
          if not values[x] then break end
          if not self._values[y].val then
            self._values[y].val = {values[x][y]}
          else
            self._values[y].val[#self._values[y].val + 1] = values[x][y]
          end
          y = y + 1
          if y > #self._values then x, y = x + 1, 1 end
        end
        ppx(self._values)
      else
        for k=1, #values do
          self._values[k].val = values[k]
        end
      end
    else
      ppx('WTF!!')
    end
  else
    local x = 1
    if not self._values then self._values = {} end
    if 'table' == type(values[1]) and 'table' == type(values[1][1]) then
      for k=1, #values[1] do
        for k, v in pairs(values[1][k]) do
          if self._values[x] and self._values[x].val then
            self._values[x].val[#self._values[x].val + 1] = v
          else
            self._values[x] = {key=k, val={v}}
          end
          x = x + 1
        end
        x = 1
      end
    else
      for k, v in pairs((values[1] or values)) do
        self._values[#self._values + 1] = {key=k, val=v}
      end
    end
  end
  return self
end

function Insert:select(...)
  self._select = Select(...)  
  self._select._aliases  = self._aliases
  self._select.prev_stmt = self
  return self._select
end
function Insert:returning(...)
  return self._addListArgs({...}, '_returning')
end
function Insert:_toString(opts)
  local res = {'INSERT '}
  if self._or then
    res[#res + 1] = self._or
  end
  res[#res + 1] = format('INTO %s (', concat(self.tbls, ', '))
  handles(res, self._values, opts, false, false, nil, '')
  res[#res + 1] = ') '
  if self._select then
    res[#res + 1] = self._select:_toString(opts)
    res[#res + 1] = ' '
  else
    res[#res + 1] = 'VALUES ('
    handles(res, self._values, opts, false, true, nil, '')
    res[#res + 1] = ') '
  end
  if self._returning then
    res[#res + 1] = 'RETURNING '
    handles(res, self._returning, opts)
  end

  return concat(res)
end

--
--
--
--
--

Update = Statement:extends()
Update.__name = 'Update'
function Update:__init(expansions, tbl, ...)
  self._aliases = expansions
  self.super.__init(self, 'update')
  self.tbls = {self:expandAlias(tbl)}
  if ... then
    self:set(...)
  end
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
  return self:_addToObj(args2object({...}), '_values')
end

function Update:values(...)
  return self:set(...)
end

function Update:_toString(opts)
  local res = {'UPDATE '}
  if self._or then
    res[#res + 1] = self._or
  end
  res[#res + 1] = format('%s SET ', self.tbls[1])
  ppx(self._values)
  handles(res, self._values, opts, true)
  if self._where then
    res[#res + 1] = format('WHERE %s', self:_exprToString(opts))
  end

  return concat(res)
end

--
--
--
--
--

Delete = Statement:extends()
Delete.__name = 'Delete'
function Delete:__init(expansions, ...)
  self._aliases = expansions
  self.super.__init(self, 'delete')
  if ... then
    self.tbls = {self:expandAlias(...)}
  end
  return self
end

function Delete:from(...)
  local arr = args2array({...})
  for i=1, #arr do
    arr[i] = self:expandAlias(arr[i])
  end
  return self:_add(arr, 'tbls')
end

Delete.where = where
Delete._and  = where

function Delete:using(...)
  local args = args2array({...})
  for i=1, #args do
    args[i] = self:expandAlias(args[i])
  end
  return self:_add(args, '_using')
end

function Delete:_toString(opts)
  local res = {'DELETE FROM ', self.tbls[1], ' '}
  if self._using then
    res[#res + 1] = format('USING %s ', concat(self._using, ', '))
  end
  if self._where then
    res[#res + 1] = format('WHERE %s', self:_exprToString(opts))
  end

  return concat(res)
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
  local on = self.on
  if not on then
    if self._joinCriteria then
      on = self:autoGenerateOn(self.tbl, self.left_tbl)
    else
      error(format('No join criteria supplied for "%s" join', get_alias(self.tbl)))
    end
  end
  if is_expr(on) then
    on = on .. opts
  else
    handles(on, on, opts, true, false, ' AND ')
    on = concat(on)
  end
  return format('%s JOIN %s ON %s', self.type, self.tbl, on)
end

function Join:__tostring() return self:__concat({}) end

--
--
--
--
--

function Group:__init(op, expressions)
  self.op, self.expressions = op, {}
  ppx(' group ', op, expressions, #expressions)
  for k in pairs(expressions) do
    if is_expr(expressions[k]) then
      self.expressions[#self.expressions + 1] = expressions[k]
    else
      obj2equals(expressions[k], self.expressions)
    end
  end
end

function Group:clone()
  return Group(self.op, clone(self.expressions))
end

function Group:__concat(opts)
  local res = {}
  pp(' group __concat ', res, self.expressions, opts, #self.expressions)
  for k in pairs(self.expressions) do
    res[#res + 1] = format("%s %s ", (self.expressions[k] .. opts), self.op)
  end
  res[#res] = sub(res[#res], 1, (#self.op + 3) * -1)
  if #self.expressions > 1 and self.parens then
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
  self.expressions = (is_expr(expr) and {_and(expr)} or {expr})
end

function Not:clone()
  return Not(clone(self.expressions[1]))
end

function Not:__concat(opts)
  return format('NOT %s', self.expressions[1] .. opts)
end

function Not:__tostring() return self:__concat({}) end

--
--
--
--
--

function Binary:__init(op, col, val, ...)
  ppx(' _binary ', op, col, val)
  self.op, self.col, self.val, self.quantifier =
    op, col, val, (... or '')
end

function Binary:clone()
  return Binary(self.op, self.col, self.val)
end

function Binary:__concat(opts)
  ppx(self.col)
  return format('%s %s %s%s', handle_column(self.col, opts), self.op,
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
      '%s LIKE %s%s',
      handle_column(self.col, opts),
      handle_value(self.val, opts),
      (self.escape_char and format(" ESCAPE '%s'", self.escape_char) or '')
    )
end

function Like:__tostring() return self:__concat({}) end

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
  return format('%s %s', handle_column(self.col, opts), self.op)
end

function Unary:__tostring() return self:__concat({}) end

--
--
--
--
--

function In:__init(col, list)
  self.col, self.list = col, list
end

function In:clone()
  return In(self.col, clone(self.list))
end

function In:__concat(opts)
  local res = ''
  if is_array(self.list) then
    res = concat(handles({}, self.list, opts, false, true))
  elseif self.list.is and self.list:is(Statement) then
    res = self.list:_toString(opts)
  end

  return format('%s IN (%s)', handle_column(self.col, opts), res)
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
  return format('EXISTS (%s)', self.subquery._toString(opts))
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
    ((_expansions and 'table' == type(_expansions)) and _expansions or {}),
    ((_criteria and 'function' == type(_criteria)) and _criteria or nil),
    ((_views and 'table' == type(_views)) and _views or {}), 
    {
      val       = Val,       sql    = Sql,    _and     = _and,     _or    = _or,
      _not      = _not,      like   = Like,   between  = Between,  isNull = isNull,
      isNotNull = isNotNull, exists = Exists, eq       = eq,       eqAll  = eqAll,
      eqAny     = eqAny,     notEq  = notEq,  notEqAll = notEqAll, ltAny  = ltAny,
      notEqAny  = notEqAn,   lt     = lt,     ltAll    = ltAll,    le     = le,
      leAll     = leAll,     leAny  = leAny,  gt       = gt,       gtAll  = gtAll,
      gtAny     = gtAny,     ge     = ge,     geAll    = geAll,    geAny  = geAny
    }

  this.addView = function(name, sel)
    if #sel.tbls ~= 1 then
      error(format('Unsupported number of tables in pseudo-view: %d', #sel.tbls))
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
    function(...) local ins = Insert(expansions, ...) return ins end,
    function(...) local upd = Update(expansions, ...) return upd end,
    function(...) local del = Delete(expansions, ...) return del end,
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