local ins, say, json, string, table =
  require '..sqlrocks'(), require 'say', require 'dkjson', require 'string', require 'table'

local _select, type, tostring, format, concat, remove =
  select, type, tostring, string.format, table.concat, table.remove

local sql, val, select, insertInto, insert, update, delete, _and, _or, like, _not, 
  _in, isNull, isNotNull, equal, eq, lt, le, gt, ge, between, exists, eqAny,
  notEqAny, union, res = 
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

local function extended_same(state, arguments) --compare_to, ...)
--print(#arguments)
  local compare_to = arguments[1] remove(arguments, 1)
  local size_of    = #arguments
  if size_of == 1 or 'string' ~= type(compare_to) then
    return state.mod == (compare_to == arguments[1])
  else
    local ok = 1
    for i=1, size_of do
      ok = (compare_to == arguments[i] and i or ok)
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

--say:set("assertion.params.positive", "Expected %s to be equal to\n%s")
assert:register("assertion", "params", check_params)

describe('basic', function()
  describe('sql select', function()
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

  describe('sql update', function()
    it('should generate an update statement', function()
      res = update('users', {name='Chico', sex='male'})
      assert.are.same(tostring(res), "UPDATE users SET name = 'Chico', sex = 'male'")
    end)

    it('should update by id', function()
      res = update('users', {name='Camila', age=26}):where({id=1})
      assert.are.same(
        tostring(res), "UPDATE users SET name = 'Camila', age = 26 WHERE id = 1")
    end)

    it('', function()
      res = update('users'):set('name', 'Eduardo')
      assert.are.same(tostring(res), "UPDATE users SET name = 'Eduardo'")
    end)

    it('', function()
      res = update('users'):set('company', 'simbio.se'):where({id=9999})
      assert.are.same(
        tostring(res), "UPDATE users SET company = 'simbio.se' WHERE id = 9999")
    end)

    it('', function()
      res = update('users'):orReplace():set({type='music', category='rock'})
      assert.are.same(
        tostring(res),
        "UPDATE OR REPLACE users SET type = 'music', category = 'rock'")
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
        "DELETE FROM user USING address addr WHERE user.addr_fk = addr.pk");
    end)
  end)

  describe('parameterized sql', function()

    it('should generate for insert statements', function()
      assert.params(insert('user', {first_name='Fred', last_name='Flintstone'}),
        'INSERT INTO user (first_name, last_name) VALUES ($1, $2)',
        {'Fred', 'Flintstone'})
    end)

    it('should generate for UPDATEs', function()
      assert.params(update('user', {first_name='Fred', last_name='Flintstone'}),
        'UPDATE user SET first_name = $1, last_name = $2', {'Fred', 'Flintstone'})
    end)

    it('should generate for WHERE clauses', function()
      assert.params(select():from('user'):where({removed=0, name='Fred Flintstone'}),
        'SELECT * FROM user WHERE removed = $1 AND name = $2',
        {0, 'Fred Flintstone'})
    end)

    it('should not escape single quotes in the values returned by toParams()',
      function()
      assert.params(update('user', {name="Muad'Dib"}),
        'UPDATE user SET name = $1', {"Muad'Dib"})
    end)

    it('should call .toString() on arrays in parameterized sql', function()
      assert.params(update('user', {name={"Paul", "Muad'Dib"}}),
        'UPDATE user SET name = $1', {"Paul,Muad'Dib"})
    end)

    it('should call .toString() on arrays in non-parameterized sql', function()
      assert.are.same(
        tostring(update('user', {name={"Paul", "Muad'Dib"}})),
        "UPDATE user SET name = 'Paul,Muad''Dib'"
      )
    end)

    it('should generate specified style params', function()
      local result = insert('user', {first_name='Fred', last_name='Flintstone'})
        :toParams({placeholder='?'})
      assert.equal(
          result.text, 'INSERT INTO user (first_name, last_name) VALUES (?1, ?2)'
        )
      assert.are.same(result.values, {'Fred', 'Flintstone'})
    end)

    it('should properly parameterize subqueries params', function()
      assert.params(
        select(select('last_name'):from('user'):where({first_name='Fred'})),
        'SELECT (SELECT last_name FROM user WHERE first_name = $1)', {'Fred'});
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

  it('should handle chained unions with params', function() 
    assert.params(
      select():from('usr'):where({name='Roy'})
      :union():select():from('usr'):where({name='Moss'}),
      "SELECT * FROM user usr WHERE name = $1" ..
      " UNION SELECT * FROM user usr WHERE name = $2", {'Roy', 'Moss'}
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

end)