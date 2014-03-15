local sql = require '..sqlrocks'

local ins = sql()

local res
local _or, _and, like, select, insert, delete, update =
  ins._or, ins._and, ins.like, ins.select, ins.insert, ins.delete, ins.update


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
      res = tostring(res)
      assert.are.True(
        (res == "SELECT * FROM users WHERE first_name = 'Fred' AND last_name = 'Flintstone'" or
        res == "SELECT * FROM users WHERE last_name = 'Flintstone' OR first_name = 'Fred'")
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
      res = tostring(res)
      assert.are.True(
        (res == "INSERT INTO users (first_name, last_name) VALUES ('Fred', 'Flintstone')" or
        res == "INSERT INTO users (last_name, first_name) VALUES ('Flintstone', 'Fred')")
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

    --it('should generate a DELETE with using', function()
    --  check(del('user').using('addr').where('user.addr_fk', sql('addr.pk')),
    --    "DELETE FROM user USING address addr WHERE user.addr_fk = addr.pk");
    --end)
  end)
end)