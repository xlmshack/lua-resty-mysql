local mysql = require('mysql')
local conv = require('conversion')
local bit = require("bit")

local strbyte = string.byte
local concat = table.concat
local tohex = bit.tohex
local band = bit.band
local bxor = bit.bxor
local bor = bit.bor

local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end

local function _dump(data, isprint)
    local len = #data
    local bytes = new_tab(len, 0)
    for i = 1, len do
        bytes[i] = tohex(strbyte(data, i), 2)
    end
    if not isprint then print(concat(bytes, " ")) end
    return (concat(bytes, " "))
end

local function _dump_dict(data)
    for k,v in pairs(data) do
        if type(v) == 'string' then
            print('k=' .. k .. ' v=' .. v)
            --print('k=' .. k .. ' v=' .. _dump(v))
        else
            print('k=' .. k .. ' v=' .. tohex(v))
        end
    end
end


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
option.host = '192.168.1.122'
option.port = 3306
option.database = 'resty'
--option.ssl_verify = true
local connection, err = database:connect(option)
if not connection then
  print(err)
else
  print('connect to mysql server successful')
end

-- local resultset, err = connection:execute('select * from user')
-- if not resultset then
--   print(err)
-- else
--   print('execute sql statement successful')
-- end

-- for k,v in pairs(resultset.table_set) do
--   for kk,vv in pairs(v) do
--     -- for kkk,vvv in pairs(vv) do
--     --   print('field=' .. kkk .. ' value=' .. vvv .. ' type=' .. type(vvv))
--     -- end
--     print('iduser=' .. (vv['iduser'] or 'null'))
--     print('username=' .. (vv['username'] or 'null'))
--     print('ctime=' .. (vv['ctime'] or 'null'))
--     print('utime=' .. (vv['utime'] or 'null'))
--     print('rtime=' .. (vv['rtime'] or 'null'))
--     print('--------------------------------------')
--   end
-- end

-- local pstmt, err = connection:prepareStatement('update tab1 set create2 = ? where tab1_id = \'1\'')
--local pstmt, err = connection:prepareStatement('insert into tab1 (tab1_id, tab2_id, create2) values(?, \'123\', ?) ')
local pstmt, err = connection:prepareStatement('select * from tab1 ')
if not pstmt then
  print(err)
else
  print('prepare statememt successful')
end

-- local param = { type = conv.MYSQL_TYPE_DATETIME, data = '2012-12-12 12:12:12.123456', decimals = 6}
-- if not pstmt:setParameter(1, param) then
--   print('set param error 1')
-- end
local resultset, err = pstmt:execute()
if not resultset then
  print(err)
else
  print('execute prepare statement successful')
end
-- local param = { type = conv.MYSQL_TYPE_LONGLONG, data = 5}
-- if not pstmt:setParameter(3, param) then
--   print('set param error 3')
-- end
-- local param = { type = conv.MYSQL_TYPE_DATE, data = '0001-01-01'}
-- if not pstmt:setParameter(4, param) then
--   print('set param error 4')
-- end
-- local resultset, err = pstmt:execute()
-- if not resultset then
--   print(err)
-- else
--   print('execute prepare statement successful')
-- end

for k,v in pairs(resultset.table_set) do
  for kk,vv in pairs(v) do
    for kkk,vvv in pairs(vv) do
      print('field=' .. kkk .. ' value=' .. vvv .. ' type=' .. type(vvv))
    end
    -- print('iduser=' .. (vv['iduser'] or 'null'))
    -- print('username=' .. (vv['username'] or 'null'))
    -- print('ctime=' .. (vv['ctime'] or 'null'))
    -- print('utime=' .. (vv['utime'] or 'null'))
    -- print('rtime=' .. (vv['rtime'] or 'null'))
    -- print('--------------------------------------')
  end
end

-- _dump_dict(resultset)


-- local bin_data, err = conv.ProtocolBinaryConverters[conv.MYSQL_TYPE_TIME][2](
--   '-0d 19:27:30.000 001' , 1)

-- if bin_data then
--   print('bin_data=' .. _dump(bin_data))
-- else
--   print('err=' .. err)
-- end