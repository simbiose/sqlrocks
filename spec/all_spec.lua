local ins, say, json, string, table =
  require '..sqlrocks'(), require 'say', require 'dkjson', require 'string', require 'table'

local _select, type, tostring, format, concat, remove =
  select, type, tostring, string.format, table.concat, table.remove

local sql, val, select, insertInto, insert, update, delete, _and, _or, like, _not,
  _in, isNull, isNotNull, equal, eq, lt, le, gt, ge, between, exists, eqAny,
  notEqAny, union, res, sel, _ins =
  ins.sql, ins.val, ins.select, ins.insertInto, ins.insert, ins.update, ins.delete,
  ins._and, ins._or, ins.like, ins._not, ins._in, ins.isNull, ins.isNotNull,
  ins.equal, ins.eq, ins.lt, ins.le, ins.gt, ins.ge, ins.between, ins.exists,
  ins.eqAny, ins.notEqAny, ins.union

local alias_expansions, table_to_alias =
  {usr='user', psn='person', addr='address'}, {user='usr', person='psn', address='addr'}

ins.aliasExpansions(alias_expansions)
ins.joinCriteria(function(l_tbl, l_alias, r_tbl, r_alias)
  return {[l_alias ..'.'.. table_to_alias[r_tbl] ..'_fk'] = r_alias ..'.pk'}
end)

local function extended_same(state, arguments)
  local compare_to = arguments[1]
  remove(arguments, 1)
  local size_of, ok = #arguments, 1

  if size_of == 1 then --or 'string' ~= type(compare_to) then print(' here ! ')
    return state.mod == (compare_to == arguments[1])
  else
    if 'table' == type(compare_to) then
      for i=1, size_of do
        ok = json.encode(compare_to) == json.encode(arguments[i]) and i or ok
      end
      return state.mod == (json.encode(compare_to) == json.encode(arguments[ok]))
    end

    for i=1, size_of do
      ok = compare_to == arguments[i] and i or ok
    end
    return state.mod == (compare_to == arguments[ok])
  end
end

say:set("assertion.esame.positive", "Expected %s to be equal to\n%s")
say:set("assertion.esame.negative", "Expected %s to be equal to\n%s")
assert:register("assertion", "esame", extended_same, "assertion.esame.positive", "assertion.esame.negative")

local function check_params(state, arguments) --instance, text, values)
  local result = arguments[1]:toParams()
  if not extended_same({mod=true}, {result.text,
    ('table' == type(arguments[2]) and unpack(arguments[2]) or arguments[2])})
  then
    if 'table' == type(arguments[2]) then
      local xis = arguments[2][#arguments[2]] arguments[2][#arguments[2]] = nil
      io.write(format("Expected sql text %s to be equal to \n%s or %s",
        result.text, concat(arguments[2], ', '), xis))
    else
      print(format("Expected sql text %s to be equal to \n%s", result.text, arguments[2]))
    end
    return state.mod == false
  end
  for i=1, #arguments[3] do
    if arguments[3][i] ~= result.values[i] then
      io.write(format("Expected values %s to be equal to\n%s",
        json.encode(result.values), json.encode(arguments[3])))
      return state.mod == false
    end
  end
  return state.mod == true
end

assert:register("assertion", "params", check_params)

describe('deep Statement.clone()', function()
  it('should deep clone WHERE expressions', function()
    sel = select():from('user'):where({first_name='Fred'})
    sel:clone():where({last_name='Flintstone'})
    assert.are.same(tostring(sel), "SELECT * FROM user WHERE first_name = 'Fred'")
  end)

  it('should deep clone .order()', function()
    sel = select():from('user'):order('name')
    sel:clone():order('last_name')
    assert.are.same(tostring(sel), 'SELECT * FROM user ORDER BY name')
  end)

  it('should deep clone .join()', function()
    sel = select():from('user'):join('addr')
    sel:clone():join('psn')
    assert.are.same(
      tostring(sel), 'SELECT * FROM user INNER JOIN address addr ON "user".addr_fk = addr.pk'
    )
  end)

  it('should clone values', function()
    _ins = insert('user', {first_name='Fred'})
    _ins:clone():values({last_name='Flintstone'})
    assert.are.same(tostring(_ins), "INSERT INTO user (first_name) VALUES ('Fred')")
  end)
end)

describe('sql select', function()
  it('should handle an array', function()
    assert.are.same(tostring(select({'one', 'order'}):from('user')),
      'SELECT one, "order" FROM user')
  end)

  it('should handle multiple args', function()
    assert.are.same(tostring(select('one', 'order'):from('user')),
      'SELECT one, "order" FROM user')
  end)

  it('should default to *', function()
    assert.are.same(tostring(select():from('user')),
      'SELECT * FROM user')
  end)

  it('should handle a comma-delimited str', function()
    assert.are.same(tostring(select('one, order'):from('user')),
      'SELECT one, "order" FROM user')
  end)

  it('should handle being called multiple times', function()
    assert.are.same(tostring(select('one, order'):select({'two', 'desc'})
      :select('three', 'four'):from('user')),
      'SELECT one, "order", two, "desc", three, four FROM user')
  end)

  it('should support DISTINCT', function()
    assert.are.same(tostring(select('one, order')
      :distinct('two, desc'):from('user')),
      'SELECT DISTINCT one, "order", two, "desc" FROM user')
  end)

  it('should support FOR UPDATE', function()
    assert.are.same(tostring(select():from('user'):forUpdate('user')),
      'SELECT * FROM user FOR UPDATE user')
  end)

  it('should support FOR UPDATE ... NO WAIT', function()
    assert.are.same(tostring(select():from('user'):forUpdateOf('user'):noWait()),
      'SELECT * FROM user FOR UPDATE user NO WAIT')
  end)

  it('select from table', function()
    res = select():from('users')
    assert.are.same(tostring(res), "SELECT * FROM users")
  end)

  it('select fields from table', function()
    res = select('name, nick, gender, age'):from('users')
    assert.are.same(tostring(res), "SELECT name, nick, gender, age FROM users")
  end)

  it('select fields (array) from table', function()
    res = select({'name', 'nick', 'gender', 'age'}):from('users')
    assert.are.same(tostring(res), "SELECT name, nick, gender, age FROM users")
  end)

  it('select from by name', function()
    res = select():from('users'):where({name='xico'})
    assert.are.same(tostring(res), "SELECT * FROM users WHERE name = 'xico'")
  end)

  it('select from users by last_name and first_name', function()
    res = select():from('users')
      :where('last_name', 'Flintstone'):_and('first_name', 'Fred')
    assert.are.same(
      tostring(res),
      "SELECT * FROM users WHERE last_name = 'Flintstone' AND first_name = 'Fred'"
    )
  end)

  it('select from users by last_name or first_name', function()
    res = select():from('users')
      :where({last_name='Flintstone', first_name='Fred'})
    assert.are.esame(
      tostring(res),
      "SELECT * FROM users WHERE first_name = 'Fred' AND last_name = 'Flintstone'",
      "SELECT * FROM users WHERE last_name = 'Flintstone' AND first_name = 'Fred'"
    )
  end)
end)

describe('.from()', function()
  it('should handle an array', function()
    assert.are.same(tostring(select():from({'one', 'two', 'usr'})),
      'SELECT * FROM one, two, user usr')
  end)

  it('should handle multiple args', function()
    assert.are.same(tostring(select():from('one', 'two', 'usr')),
      'SELECT * FROM one, two, user usr')
  end)

  it('should handle a comma-delimited string', function()
    assert.are.same(tostring(select():from('one, two, usr')),
      'SELECT * FROM one, two, user usr')
  end)

  it('should handle being called multiple times', function()
    assert.are.same(tostring(select():from('one', 'usr')
      :from({'two', 'psn'}):from('three, addr')),
      'SELECT * FROM one, user usr, two, person psn, three, address addr')
  end)
end)

describe('select().into() should insert into a new table', function()
  it('.into()', function()
    assert.are.same(tostring(select():into('new_user'):from('user')),
      'SELECT * INTO new_user FROM user')
  end)

  it('.intoTable()', function()
    assert.are.same(tostring(select():intoTable('new_user'):from('user')),
      'SELECT * INTO new_user FROM user')
  end)

  it('intoTemp()', function()
    assert.are.same(tostring(select():intoTemp('new_user'):from('user')),
      'SELECT * INTO TEMP new_user FROM user')
  end)

  it('intoTempTable()', function()
    assert.are.same(tostring(select():intoTempTable('new_user'):from('user')),
      'SELECT * INTO TEMP new_user FROM user')
  end)
end)


describe('sql update', function()
  it('should generate an update statement', function()
    res = update('users', {name='Chico', sex='male'})
    assert.are.esame(
      tostring(res), "UPDATE users SET name = 'Chico', sex = 'male'",
      "UPDATE users SET sex = 'male', name = 'Chico'"
    )
  end)

  it('should update by id', function()
    res = update('users', {name='Camila', age=26}):where({id=1})
    assert.are.esame(
      tostring(res), "UPDATE users SET name = 'Camila', age = 26 WHERE id = 1",
      "UPDATE users SET age = 26, name = 'Camila' WHERE id = 1"
    )
  end)

  it('...', function()
    res = update('users'):set('name', 'Eduardo')
    assert.are.same(tostring(res), "UPDATE users SET name = 'Eduardo'")
  end)

  it('... ---', function()
    res = update('users'):set('company', 'simbio.se'):where({id=9999})
    assert.are.same(
      tostring(res), "UPDATE users SET company = 'simbio.se' WHERE id = 9999")
  end)

  it('.. --', function()
    res = update('users'):orReplace():set({type='music', category='rock'})
    assert.are.esame(
      tostring(res),
      "UPDATE OR REPLACE users SET type = 'music', category = 'rock'",
      "UPDATE OR REPLACE users SET category = 'rock', type = 'music'"
    )
  end)
end)

describe('sql insert', function()
  it('should generate insert statement', function()
    res = insert('users', {first_name='Fred', last_name='Flintstone'})
    assert.are.esame(
      tostring(res),
      "INSERT INTO users (first_name, last_name) VALUES ('Fred', 'Flintstone')",
      "INSERT INTO users (last_name, first_name) VALUES ('Flintstone', 'Fred')"
    )
  end)

  it('should generate insert statement #2', function()
    res = insert('users', {'id', 'name'}):values({33, 'Fred'})
    assert.are.same(
      tostring(res), "INSERT INTO users (id, name) VALUES (33, 'Fred')")
  end)

  it('should generate insert statement #3', function()
    res = insert('users', {'id', 'name'}):values(33, 'Fred')
    assert.are.same(
      tostring(res), "INSERT INTO users (id, name) VALUES (33, 'Fred')")
  end)

  it('should generate insert statement #4', function()
    res = insert('users', 'id', 'name'):values(33, 'Fred')
    assert.are.same(
      tostring(res), "INSERT INTO users (id, name) VALUES (33, 'Fred')")
  end)

  it('should generate insert statement #3', function()
    res = insert('users', {'id', 'name'})
      :values({{33, 'Fred'}, {22, 'John'}, {27, 'Amy'}})
    assert.are.same(
      tostring(res),
      "INSERT INTO users (id, name) VALUES (33, 'Fred'), (22, 'John'), (27, 'Amy')"
    )
  end)
end)

describe('sql delete', function()
  it('should generate a DELETE statement', function()
    res = delete('users')
    assert.are.same(tostring(res), 'DELETE FROM users')
  end)

  it('should support .from()', function()
    res = delete():from('users')
    assert.are.same(tostring(res), 'DELETE FROM users')
  end)

  it('should generate a DELETE statement with a WHERE clause', function()
    res = delete('users'):where('kind', 'music')
    assert.are.same(tostring(res), "DELETE FROM users WHERE kind = 'music'")
  end)

  it('should generate a DELETE with using', function()
    assert.are.same(
      tostring(delete('user'):using('addr'):where('user.addr_fk', sql('addr.pk'))),
      'DELETE FROM user USING address addr WHERE "user".addr_fk = addr.pk'
    )
  end)
end)

describe('parameterized sql', function()

  it('should generate for insert statements #bla', function()
    local result = insert('user', {first_name='Fred', last_name='Flintstone'}):toParams()

    assert.are.esame(result.values, {'Fred', 'Flintstone'}, {'Flintstone', 'Fred'})
    assert.are.esame(
      result.text, 'INSERT INTO user (first_name, last_name) VALUES ($1, $2)',
      'INSERT INTO user (last_name, first_name) VALUES ($1, $2)'
    )
  end)

  it('should generate for UPDATEs', function()
    local result = update('user', {first_name='Fred', last_name='Flintstone'}):toParams()

    assert.are.esame(result.values, {'Fred', 'Flintstone'}, {'Flintstone', 'Fred'})
    assert.are.esame(
      result.text, 'UPDATE user SET first_name = $1, last_name = $2',
      'UPDATE user SET last_name = $1, first_name = $2'
    )
  end)

  it('should generate for WHERE clauses', function()
    local result = select():from('user'):where({removed=0, name='Fred Flintstone'}):toParams()

    assert.are.esame(result.values, {0, 'Fred Flintstone'}, {'Fred Flintstone', 0})
    assert.are.esame(
      result.text, 'SELECT * FROM user WHERE removed = $1 AND name = $2',
      'SELECT * FROM user WHERE name = $1 AND removed = $2'
    )
  end)

  it('should not escape single quotes in the values returned by toParams()', function ()
    assert.params(update('user', {name="Muad'Dib"}), 'UPDATE user SET name = $1', {"Muad'Dib"})
  end)

  it('should call .toString() on arrays in parameterized sql', function ()
    assert.params(
      update('user', {name={"Paul", "Muad'Dib"}}), 'UPDATE user SET name = $1', {"Paul,Muad'Dib"}
    )
  end)

  it('should call .toString() on arrays in non-parameterized sql', function()
    assert.are.same(
      tostring(update('user', {name={"Paul", "Muad'Dib"}})),
      "UPDATE user SET name = 'Paul,Muad''Dib'"
    )
  end)

  it('should generate specified style params', function()
    local result =
      insert('user', {first_name='Fred', last_name='Flintstone'}):toParams({placeholder='?'})

    assert.are.esame(result.values, {'Fred', 'Flintstone'}, {'Flintstone', 'Fred'})
    assert.are.esame(
      result.text, 'INSERT INTO user (first_name, last_name) VALUES (?1, ?2)',
      'INSERT INTO user (last_name, first_name) VALUES (?1, ?2)'
    )
  end)

  it('should properly parameterize subqueries params', function()
    assert.params(
      select(select('last_name'):from('user'):where({first_name='Fred'})),
      'SELECT (SELECT last_name FROM user WHERE first_name = $1)', {'Fred'}
    )
  end)

  it('should properly parameterize subqueries in updates', function()
    local addr_id_for_usr = select('id'):from('address')
        :where('usr_id', sql('user.id')):_and('active', true)
    assert.params(update('user'):set('addr_id', addr_id_for_usr),
      'UPDATE user SET addr_id = (SELECT id FROM address WHERE usr_id = user.id AND active = $1)',
      {true})
  end)
end)

describe('value handling', function()
  it('should escape single quotes when toString() is used', function()
    res = update('user', {name="Muad'Dib"})
    assert.are.same(tostring(res), "UPDATE user SET name = 'Muad''Dib'")
  end)

  it('should escape multiple single quotes in the same string', function()
    res = update('address', {city="Liu'e, Hawai'i"})
    assert.are.same(tostring(res), "UPDATE address SET city = 'Liu''e, Hawai''i'")
  end)

  it('should support sql.val() to pass in values where columns are expected', function()
    res = select():from('user'):where(val('Fred'), sql('first_name'))
    assert.are.same(tostring(res), "SELECT * FROM user WHERE 'Fred' = first_name")
  end)
end)

it('should expand abbreviations in FROM and JOINs', function()
  assert.are.same(
    tostring(select():from('usr'):join('psn', {['usr.psn_fk']='psn.pk'})),
    'SELECT * FROM user usr INNER JOIN person psn ON usr.psn_fk = psn.pk'
  )
end)

it('should support aliases', function()
  assert.are.same(
    tostring(select():from('user usr2'):join('address addr2')),
    'SELECT * FROM user usr2 INNER JOIN address addr2 ON usr2.addr_fk = addr2.pk'
  )
end)

it('should auto-generate join criteria using supplied joinCriteria() function', function()
  assert.are.same(
    tostring(select():from('usr'):join('psn')),
    'SELECT * FROM user usr INNER JOIN person psn ON usr.psn_fk = psn.pk'
  )
end)

it('should auto-generate join criteria to multiple tables', function()
  assert.are.same(
    tostring(select():from('usr'):join('psn'):join('addr')),
    'SELECT * FROM user usr ' ..
    'INNER JOIN person psn ON usr.psn_fk = psn.pk ' ..
    'INNER JOIN address addr ON psn.addr_fk = addr.pk'
  )
end)

it('should auto-generate join criteria from a single table to multiple tables', function()
  assert.are.same(
    tostring(select():from('usr'):join('psn', 'addr')),
    'SELECT * FROM user usr ' ..
    'INNER JOIN person psn ON usr.psn_fk = psn.pk ' ..
    'INNER JOIN address addr ON usr.addr_fk = addr.pk'
  )
end)

it('should handle unions', function()
  assert.are.same(
    tostring(select():from('usr'):where({name='Roy'})
    :union(select():from('usr'):where({name='Moss'}))
    :union(select():from('usr'):where({name='The elders of the internet'}))),
    "SELECT * FROM user usr WHERE name = 'Roy'" ..
    " UNION SELECT * FROM user usr WHERE name = 'Moss'" ..
    " UNION SELECT * FROM user usr WHERE name = 'The elders of the internet'"
  )
end)

it('should handle chained unions', function()
  assert.are.same(
    tostring(select():from('usr'):where({name='Roy'})
    :union():select():from('usr'):where({name='blurns'})),
    "SELECT * FROM user usr WHERE name = 'Roy'" ..
    " UNION SELECT * FROM user usr WHERE name = 'blurns'"
  )
end)

it('should handle chained unions with params', function ()
  local result =
    select():from('usr'):where({name='Roy'}):union():select():from('usr'):where({name='Moss'}):toParams()

  assert.are.esame(result.values, {'Roy', 'Moss'}, {'Moss', 'Roy'})
  assert.are.esame(
    result.text, "SELECT * FROM user usr WHERE name = $1 UNION SELECT * FROM user usr WHERE name = $2"
  )
end)

it('should handle unions with params', function()
  assert.params(
    select():from('usr'):where({name='Roy'})
    :union(select():from('usr'):where({name='Moss'}))
    :union(select():from('usr'):where({name='The elders of the internet'})),
    'SELECT * FROM user usr WHERE name = $1' ..
    ' UNION SELECT * FROM user usr WHERE name = $2' ..
    ' UNION SELECT * FROM user usr WHERE name = $3',
    {'Roy', 'Moss', 'The elders of the internet'}
  )
end)

describe('insert():into() should insert into a preexisting table', function()
  it('insert():into():select()', function()
    assert.are.same(tostring(insert():into('new_user', 'id', 'addr_id')
      :select('id', 'addr_id'):from('user')),
      'INSERT INTO new_user (id, addr_id) SELECT id, addr_id FROM user')
  end)

  it('insert():select()', function()
    assert.are.same(tostring(insert('new_user', 'id', 'addr_id')
      :select('id', 'addr_id'):from('user')),
      'INSERT INTO new_user (id, addr_id) SELECT id, addr_id FROM user')
  end)
end)

describe('GROUP BY clause', function()
  it('should support single group by', function()
    assert.are.same(tostring(select():from('user'):groupBy('last_name')),
      'SELECT * FROM user GROUP BY last_name')
  end)

  it('should support multiple groupBy() args w/ reserved words quoted', function()
    assert.are.same(tostring(select():from('user'):groupBy('last_name', 'order')),
      'SELECT * FROM user GROUP BY last_name, "order"')
  end)

  it('should support :groupBy():groupBy()', function()
    assert.are.same(tostring(select():from('user')
      :groupBy('last_name'):groupBy('order')),
      'SELECT * FROM user GROUP BY last_name, "order"')
  end)

  it('should support an array', function()
    assert.are.same(tostring(select():from('user'):groupBy({'last_name', 'order'})),
      'SELECT * FROM user GROUP BY last_name, "order"')
  end)
end)

describe(':order() / :orderBy()', function()
  it('should support .orderBy(arg1, arg2)', function()
    assert.are.same(tostring(select():from('user'):orderBy('last_name', 'order')),
      'SELECT * FROM user ORDER BY last_name, "order"')
  end)

  it('should support an array', function()
    assert.are.same(tostring(select():from('user'):orderBy({'last_name', 'order'})),
      'SELECT * FROM user ORDER BY last_name, "order"')
  end)

  it('should support being called multiple times', function()
    assert.are.same(tostring(select():from('user')
      :orderBy('last_name'):orderBy('order')),
      'SELECT * FROM user ORDER BY last_name, "order"')
  end)
end)

describe('joins', function()
  it(':join() should accept a comma-delimited string', function()
    assert.are.same(tostring(select():from('usr'):join('psn, addr')),
      'SELECT * FROM user usr ' ..
      'INNER JOIN person psn ON usr.psn_fk = psn.pk ' ..
      'INNER JOIN address addr ON usr.addr_fk = addr.pk')
  end)

  it(':leftJoin() should generate a left join', function()
    assert.are.same(tostring(select():from('usr'):leftJoin('addr')),
      'SELECT * FROM user usr ' ..
      'LEFT JOIN address addr ON usr.addr_fk = addr.pk')
  end)

  it(':leftOuterJoin() should generate a left join', function()
    assert.are.same(tostring(select():from('usr'):leftOuterJoin('addr')),
      'SELECT * FROM user usr ' ..
      'LEFT JOIN address addr ON usr.addr_fk = addr.pk')
  end)

  it(':rightJoin() should generate a right join', function()
    assert.are.same(tostring(select():from('usr'):rightJoin('addr')),
      'SELECT * FROM user usr ' ..
      'RIGHT JOIN address addr ON usr.addr_fk = addr.pk')
  end)

  it(':rightOuterJoin() should generate a right join', function()
    assert.are.same(tostring(select():from('usr'):rightOuterJoin('addr')),
      'SELECT * FROM user usr ' ..
      'RIGHT JOIN address addr ON usr.addr_fk = addr.pk')
  end)

  it(':fullJoin() should generate a full join', function()
    assert.are.same(tostring(select():from('usr'):fullJoin('addr')),
      'SELECT * FROM user usr ' ..
      'FULL JOIN address addr ON usr.addr_fk = addr.pk')
  end)

  it(':fullOuterJoin() should generate a full join', function()
    assert.are.same(tostring(select():from('usr'):fullOuterJoin('addr')),
      'SELECT * FROM user usr ' ..
      'FULL JOIN address addr ON usr.addr_fk = addr.pk')
  end)

  it(':crossJoin() should generate a cross join', function()
    assert.are.same(tostring(select():from('usr'):crossJoin('addr')),
      'SELECT * FROM user usr ' ..
      'CROSS JOIN address addr ON usr.addr_fk = addr.pk')
  end)

  it(':join() should accept an expression for the on argument', function()
    assert.are.same(tostring(select():from('usr')
      :join('addr', eq('usr.addr_id', sql('addr.id')))),
      'SELECT * FROM user usr INNER JOIN address addr ON usr.addr_id = addr.id')
  end)
end)

describe(':on()', function()
  it('should accept an object literal', function()
    assert.are.same(tostring(select():from('usr'):join('addr')
      :on({['usr.addr_id']='addr.id'})),
      'SELECT * FROM user usr ' ..
      'INNER JOIN address addr ON usr.addr_id = addr.id')
  end)

  it('should accept a (key, value) pair', function()
    assert.are.same(tostring(select():from('usr'):join('addr')
      :on('usr.addr_id', 'addr.id')),
      'SELECT * FROM user usr ' ..
      'INNER JOIN address addr ON usr.addr_id = addr.id')
  end)

  it('can be called multiple times', function()
    assert.are.esame(tostring(select():from('usr', 'psn'):join('addr')
      :on({['usr.addr_id']='addr.id'}):on('psn.addr_id', 'addr.id')),
      'SELECT * FROM user usr, person psn ' ..
      'INNER JOIN address addr ON usr.addr_id = addr.id AND psn.addr_id = addr.id',
      'SELECT * FROM user usr, person psn ' ..
      'INNER JOIN address addr ON psn.addr_id = addr.id AND usr.addr_id = addr.id')
  end)

  it('can be called multiple times w/ an object literal', function()
    assert.are.esame(tostring(select():from('usr', 'psn'):join('addr')
      :on({['usr.addr_id']='addr.id'}):on({['psn.addr_id']='addr.id'})),
      'SELECT * FROM user usr, person psn ' ..
      'INNER JOIN address addr ON usr.addr_id = addr.id AND psn.addr_id = addr.id',
      'SELECT * FROM user usr, person psn ' ..
      'INNER JOIN address addr ON psn.addr_id = addr.id AND usr.addr_id = addr.id')
  end)

  it('should accept an expression', function()
    assert.are.same(
      tostring(select():from('usr'):join('addr'):on(eq('usr.addr_id', sql('addr.id')))),
      'SELECT * FROM user usr INNER JOIN address addr ON usr.addr_id = addr.id')
  end)
end)

describe('WHERE clauses', function()
  it('should AND multiple where() criteria by default', function()
    assert.are.esame(
      tostring(select():from('user'):where({first_name='Fred', last_name='Flintstone'})),
      "SELECT * FROM user WHERE first_name = 'Fred' AND last_name = 'Flintstone'",
      "SELECT * FROM user WHERE last_name = 'Flintstone' AND first_name = 'Fred'"
    )
  end)

  it('should AND multiple where()s by default', function()
    assert.are.esame(
      tostring(
        select():from('user'):where({first_name='Fred'}):where({last_name='Flintstone'})
      ),
      "SELECT * FROM user WHERE first_name = 'Fred' AND last_name = 'Flintstone'",
      "SELECT * FROM user WHERE last_name = 'Flintstone' AND first_name = 'Fred'"
    )
  end)

  it('should handle explicit :_and() with (key, value) args', function()
    assert.are.esame(
      tostring(
        select():from('user'):where('first_name', 'Fred'):_and('last_name', 'Flintstone')
      ),
      "SELECT * FROM user WHERE first_name = 'Fred' AND last_name = 'Flintstone'",
      "SELECT * FROM user WHERE last_name = 'Flintstone' AND first_name = 'Fred'"
    )
  end)

  it('should handle nested _and(_or())', function()
    assert.are.esame(
      tostring(
        select():from('user'):where(
          _and({last_name='Flintstone'}, _or({first_name='Fred'}, {first_name='Wilma'}))
        )
      ),
      "SELECT * FROM user WHERE last_name = 'Flintstone' AND (first_name = 'Fred' OR first_name = 'Wilma')",
      "SELECT * FROM user WHERE (first_name = 'Fred' OR first_name = 'Wilma') AND last_name = 'Flintstone'"
    )
  end)

  it('and() should be implicit', function()
    assert.are.esame(
      tostring(
        select():from('user'):where({last_name='Flintstone'},_or({first_name='Fred'}, {first_name='Wilma'}))
      ),
      "SELECT * FROM user WHERE last_name = 'Flintstone' AND (first_name = 'Fred' OR first_name = 'Wilma')",
      "SELECT * FROM user WHERE (first_name = 'Fred' OR first_name = 'Wilma') AND last_name = 'Flintstone'"
    )
  end)

  it('should handle like()', function()
    assert.are.same(
      tostring(select():from('user'):where(like('last_name', 'Flint%'))),
      "SELECT * FROM user WHERE last_name LIKE 'Flint%'"
    )
  end)

  it('should accept a 3rd escape param to like()', function()
    assert.are.same(
      tostring(select():from('user'):where(like('percent', '100\\%', '\\'))),
      "SELECT * FROM user WHERE percent LIKE '100\\%' ESCAPE '\\'"
    )
  end)

  it('should handle _not()', function()
    assert.are.same(
      tostring(select():from('user'):where(_not({first_name='Fred'}))),
      "SELECT * FROM user WHERE NOT first_name = 'Fred'"
    )
  end)

  it('should handle _in()', function()
    assert.are.same(
      tostring(select():from('user'):where(_in('first_name', {'Fred', 'Wilma'}))),
      "SELECT * FROM user WHERE first_name IN ('Fred', 'Wilma')"
    )
  end)

  it('should handle :_in() with multiple args', function()
    assert.are.same(
      tostring(select():from('user'):where(_in('name', 'Jimmy', 'Owen'))),
      "SELECT * FROM user WHERE name IN ('Jimmy', 'Owen')"
    )
  end)

  it('should handle :_in() with a subquery', function()
    assert.are.same(
      tostring(select():from('user'):where(_in('addr_id', select('id'):from('address')))),
      'SELECT * FROM user WHERE addr_id IN (SELECT id FROM address)'
    )
  end)

  it('should handle exists() with a subquery', function()
    assert.are.same(
      tostring(
        select():from('user'):where(exists(select():from('address')
          :where({['user.addr_id']=sql('address.id')})))
      ),
      'SELECT * FROM user WHERE EXISTS (SELECT * FROM address WHERE "user".addr_id = address.id)'
    )
  end)

  it('should handle exists() with a subquery, parameterized', function()
    assert.params(
      select():from('user'):where('active', true):where(
        exists(select():from('address'):where({['user.addr_id']=37}))
      ),
      'SELECT * FROM user WHERE active = $1 AND EXISTS (SELECT * FROM address WHERE "user".addr_id = $2)',
      {true, 37}
    )
  end)

  it('should handle isNull()', function()
    assert.are.same(
      tostring(select():from('user'):where(isNull('first_name'))),
      'SELECT * FROM user WHERE first_name IS NULL'
    )
  end)

  it('should handle isNotNull()', function()
    assert.are.same(
      tostring(select():from('user'):where(isNotNull('first_name'))),
      'SELECT * FROM user WHERE first_name IS NOT NULL'
    )
  end)

  it('should handle explicit equal()', function()
    assert.are.same(
      tostring(select():from('user'):where(eq('first_name', 'Fred'))),
      "SELECT * FROM user WHERE first_name = 'Fred'"
    )
  end)

  it('should handle lt()', function()
    assert.are.same(
      tostring(select():from('user'):where(lt('order', 5))),
      'SELECT * FROM user WHERE "order" < 5'
    )
  end)

  it('should handle le()', function()
    assert.are.same(
      tostring(select():from('user'):where(le('order', 5))),
      'SELECT * FROM user WHERE "order" <= 5'
    )
  end)

  it('should handle gt()', function()
    assert.are.same(
      tostring(select():from('user'):where(gt('order', 5))),
      'SELECT * FROM user WHERE "order" > 5'
    )
  end)

  it('should handle ge()', function()
    assert.are.same(
      tostring(select():from('user'):where(ge('order', 5))),
      'SELECT * FROM user WHERE "order" >= 5'
    )
  end)

  it('should handle between()', function()
    assert.are.same(
      tostring(select():from('user'):where(between('name', 'Frank', 'Fred'))),
      "SELECT * FROM user WHERE name BETWEEN 'Frank' AND 'Fred'"
    )
  end)
end)

describe(':limit()', function()
  it('should add a LIMIT clause', function()
    assert.are.same(tostring(select():from('user'):limit(10)), 'SELECT * FROM user LIMIT 10')
  end)
end)

describe(':offset()', function()
  it('should add an OFFSET clause', function()
    assert.are.same(tostring(select():from('user'):offset(10)), 'SELECT * FROM user OFFSET 10')
  end)

  it('should place OFFSET after LIMIT if both are supplied', function()
    assert.are.same(tostring(select():from('user'):offset(5):limit(10)),
      'SELECT * FROM user LIMIT 10 OFFSET 5')
  end)
end)

describe('should quote reserved words in column names', function()
  it('in ORDER BY', function()
    assert.are.same(tostring(select():from('usr'):orderBy('order')),
      'SELECT * FROM user usr ORDER BY "order"')
  end)

  it('in SELECT', function()
    assert.are.same(tostring(select('desc'):from('usr')), 'SELECT "desc" FROM user usr')
  end)

  it('in JOINs', function()
    assert.are.same(tostring(select():from('usr'):join('psn', {['usr.order']='psn.order'})),
      'SELECT * FROM user usr INNER JOIN person psn ON usr."order" = psn."order"')
  end)

  it('in INSERT', function()
    assert.are.same(tostring(insert('user'):values({order=1})), 'INSERT INTO user ("order") VALUES (1)')
  end)

  it('in alternative insert() API', function()
    assert.are.same(tostring(insert('user', 'order'):values(1)), 'INSERT INTO user ("order") VALUES (1)')
  end)

  it('with a db and table prefix and a suffix', function()
    assert.are.same(tostring(select('db.usr.desc AS usr_desc'):from('usr')),
      'SELECT db.usr."desc" AS usr_desc FROM user usr')
  end)

  it('should quote sqlite reserved words', function()
    assert.are.same(tostring(select('action'):from('user')), 'SELECT "action" FROM user')
  end)

  it('should not quote reserved words in SELECT expressions', function()
    assert.are.same(
      tostring(select("CASE WHEN name = 'Fred' THEN 1 ELSE 0 AS security_level"):from('user')),
      "SELECT CASE WHEN name = 'Fred' THEN 1 ELSE 0 AS security_level FROM user")
  end)
end)

describe('subqueries in <, >, etc', function()
  it('should support a subquery in >', function()
    local ctn = select('count(*)'):from('address'):where({['user.addr_id']=sql('address.id')})
    assert.are.same(tostring(select():from('user'):where(gt(ctn, 5))),
      'SELECT * FROM user WHERE (SELECT count(*) FROM address WHERE "user".addr_id = address.id) > 5')
  end)

  it('should support a subquery in <=', function()
    local ctn = select('count(*)'):from('address'):where({['user.addr_id']=sql('address.id')})
    assert.are.same(tostring(select():from('user'):where(le(ctn, 5))),
      'SELECT * FROM user WHERE (SELECT count(*) FROM address WHERE "user".addr_id = address.id) <= 5')
  end)

  it('should support = ANY (subquery) quantifier', function()
    assert.are.same(
      tostring(select():from('user'):where(eqAny('user.id', select('user_id'):from('address')))),
      'SELECT * FROM user WHERE "user".id = ANY (SELECT user_id FROM address)'
    )
  end)

  it('should support <> ANY (subquery) quantifier', function()
    assert.are.same(
      tostring(select():from('user'):where(notEqAny('user.id', select('user_id'):from('address')))),
      'SELECT * FROM user WHERE "user".id <> ANY (SELECT user_id FROM address)'
    )
  end)
end)

describe('pseudo-views', function()
  it('should namespace joined tables', function()
    ins.addView('activeUsers', select():from('usr'):join('psn'))
    assert.are.same(
      tostring(select():from('accounts'):joinView('activeUsers a_usr')),
      'SELECT * FROM accounts ' ..
      'INNER JOIN user a_usr ON accounts.usr_fk = a_usr.pk ' ..
      'INNER JOIN person a_usr_psn ON a_usr.psn_fk = a_usr_psn.pk'
    )
  end)

  it('should properly quote reserved words in join tables and allow custom ON criteria', function()
    ins.addView('activeUsers', select():from('usr'):join('psn', {['usr.psn_desc']='psn.desc'}))
    assert.are.same(
      tostring(select():from('accounts'):joinView('activeUsers a_usr')),
      'SELECT * FROM accounts ' ..
      'INNER JOIN user a_usr ON accounts.usr_fk = a_usr.pk ' ..
      'INNER JOIN person a_usr_psn ON a_usr.psn_desc = a_usr_psn."desc"'
    )
  end)

  it('should add namespaced WHERE criteria', function()
    ins.addView('activeUsers', select():from('usr'):join('psn')
      :where({['usr.active']=true, ['psn.active']=true}))
    assert.are.esame(
      tostring(select():from('accounts'):joinView('activeUsers a_usr')),

      'SELECT * FROM accounts ' ..
      'INNER JOIN user a_usr ON accounts.usr_fk = a_usr.pk ' ..
      'INNER JOIN person a_usr_psn ON a_usr.psn_fk = a_usr_psn.pk ' ..
      'WHERE a_usr.active = true AND a_usr_psn.active = true',

      'SELECT * FROM accounts INNER JOIN user a_usr ON accounts.usr_fk = a_usr.pk ' ..
      'INNER JOIN person a_usr_psn ON a_usr.psn_fk = a_usr_psn.pk WHERE a_usr_psn.active = true ' ..
      'AND a_usr.active = true'
    )
  end)

  it('should re-alias when re-using a view w/ a diff alias', function()
    ins.addView('activeUsers', select():from('usr'):where({['usr.active']=true}))
    assert.are.same(
      tostring(select():from('accounts'):joinView('activeUsers a_usr'):joinView('activeUsers a_usr2')),
      'SELECT * FROM accounts ' ..
      'INNER JOIN user a_usr ON accounts.usr_fk = a_usr.pk ' ..
      'INNER JOIN user a_usr2 ON a_usr.usr_fk = a_usr2.pk ' ..
      'WHERE a_usr.active = true AND a_usr2.active = true'
    )
  end)

  it('should be able to select from a pseudo-view', function()
    ins.addView('activeUsers', select():from('usr'):where({['usr.active']=true}))
    assert.are.same(
      tostring(ins.getView('activeUsers')), 'SELECT * FROM user usr WHERE usr.active = true'
    )
  end)
end)

describe('the AS keyword', function()
  it('should not generate invalid SQL', function()
    assert.are.same(
      tostring(select():from('user AS usr'):join('addr')),
      'SELECT * FROM user AS usr INNER JOIN address addr ON usr.addr_fk = addr.pk')
  end)
end)
