local mysql = require('mysql')
local conv = require('conversion')

local database = mysql:new()
if not database then
  print('database is null')
else
  print('database is not null')
end

local option = {}
option.max_packet_size = 1024*1024
option.user = 'liqikun'
option.password = '123q'
option.pool = 'pool1'
option.host = '192.168.3.9'
option.port = 3306
option.database = 'resty'
--option.ssl_verify = true
local connection, err = database:connect(option)
if not connection then
  print(err)
else
  print('connect to mysql server successful')
end

-- local resultset, err = connection:execute('select * from test; select * from test')
-- if not resultset then
--   print(err)
-- else
--   print('execute sql statement successful')
-- end

-- for k,v in pairs(resultset.table_set) do
--   for kk,vv in pairs(v) do
--     for kkk,vvv in pairs(vv) do
--       print('field=' .. kkk .. ' value=' .. vvv)
--     end
--   end
-- end

local pstmt, err = connection:prepareStatement('select * from test where id = ? or name = ?  ')
if not pstmt then
  print(err)
else
  print('prepare statememt successful')
end

local param = { type = conv.MYSQL_TYPE_LONGLONG, data = 3}
if not pstmt:setParameter(1, param) then
  print('set param error 1')
end
local param = { type = conv.MYSQL_TYPE_VAR_STRING, data = 'c'}
if not pstmt:setParameter(2, param) then
  print('set param error 2')
end

local resultset, err = pstmt:execute()
if not resultset then
  print(err)
else
  print('execute prepare statement successful')
end

for k,v in pairs(resultset.table_set) do
  for kk,vv in pairs(v) do
    for kkk,vvv in pairs(vv) do
      print('field=' .. kkk .. ' value=' .. vvv)
    end
  end
end
